package artifact

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type Storage interface {
	Put(context.Context, string, []byte) (string, error)
	Get(context.Context, string, string, int64) ([]byte, error)
	Delete(context.Context, string, string) error
}

type StorageConfig struct {
	Kind              string `json:"kind"`
	Endpoint          string `json:"endpoint"`
	Region            string `json:"region"`
	Bucket            string `json:"bucket"`
	PathStyle         bool   `json:"path_style"`
	RequireVersioning bool   `json:"require_versioning"`
}

type S3Storage struct {
	client *s3.Client
	config StorageConfig
}

func NewStorage(ctx context.Context, cfg StorageConfig) (Storage, error) {
	if cfg.Kind != "s3" || cfg.Region == "" || cfg.Bucket == "" || strings.ContainsAny(cfg.Bucket, "/\\?#") {
		return nil, ErrUnsupported
	}
	if cfg.Endpoint != "" && !ValidOrigin(cfg.Endpoint) {
		return nil, ErrInvalid
	}
	client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	loaded, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region), awsconfig.WithHTTPClient(client), awsconfig.WithRetryMaxAttempts(2))
	if err != nil {
		return nil, ErrStorage
	}
	return NewS3Storage(cfg, loaded), nil
}

func NewS3Storage(cfg StorageConfig, loaded aws.Config) *S3Storage {
	return &S3Storage{config: cfg, client: s3.NewFromConfig(loaded, func(options *s3.Options) {
		options.UsePathStyle = cfg.PathStyle
		options.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		options.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		if cfg.Endpoint != "" {
			options.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})}
}

func ValidOrigin(value string) bool {
	u, err := url.Parse(value)
	if err != nil || u.User != nil || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || u.Path != "" {
		return false
	}
	return u.Scheme == "https" || u.Scheme == "http" && (u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost" || u.Hostname() == "::1")
}

func (s *S3Storage) Put(ctx context.Context, key string, data []byte) (string, error) {
	digest, err := hex.DecodeString(Digest(data))
	if err != nil {
		return "", ErrIntegrity
	}
	result, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(key), Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data))), ContentType: aws.String("application/octet-stream"), IfNoneMatch: aws.String("*"), ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(digest))})
	if err != nil {
		if errors.Is(storageError(err), ErrConflict) {
			head, headErr := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(key)})
			if headErr != nil {
				return "", storageError(headErr)
			}
			version := aws.ToString(head.VersionId)
			if s.config.RequireVersioning && (version == "" || version == "null") {
				return "", ErrUnsupported
			}
			return version, ErrConflict
		}
		return "", storageError(err)
	}
	version := aws.ToString(result.VersionId)
	if s.config.RequireVersioning && (version == "" || version == "null") {
		return version, ErrUnsupported
	}
	return version, nil
}

func (s *S3Storage) Get(ctx context.Context, key, version string, limit int64) ([]byte, error) {
	input := &s3.GetObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(key)}
	if version != "" {
		input.VersionId = aws.String(version)
	}
	result, err := s.client.GetObject(ctx, input)
	if err != nil {
		return nil, storageError(err)
	}
	defer result.Body.Close()
	if result.ContentLength != nil && *result.ContentLength > limit {
		return nil, ErrIntegrity
	}
	data, err := io.ReadAll(io.LimitReader(result.Body, limit+1))
	if err != nil {
		return nil, ErrStorage
	}
	if int64(len(data)) > limit {
		return nil, ErrIntegrity
	}
	return data, nil
}

func (s *S3Storage) Delete(ctx context.Context, key, version string) error {
	if version == "" {
		head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(key)})
		if err != nil {
			if errors.Is(storageError(err), ErrMissing) {
				return nil
			}
			return storageError(err)
		}
		version = aws.ToString(head.VersionId)
	}
	input := &s3.DeleteObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(key)}
	if version != "" {
		input.VersionId = aws.String(version)
	}
	_, err := s.client.DeleteObject(ctx, input)
	if err != nil {
		return storageError(err)
	}
	return nil
}

func storageError(err error) error {
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "NoSuchKey", "NoSuchVersion", "NotFound":
			return ErrMissing
		case "PreconditionFailed", "ConditionalRequestConflict":
			return ErrConflict
		case "NotImplemented", "InvalidRequest", "InvalidArgument":
			return ErrUnsupported
		}
	}
	return ErrStorage
}

func VerifyStorage(ctx context.Context, storage Storage) (resultErr error) {
	key := "detent-probe/" + NewID("object")
	data := []byte("detent-artifact-capability-probe-v1")
	version, err := storage.Put(ctx, key, data)
	if err != nil {
		return err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, storage.Delete(cleanupCtx, key, version))
	}()
	if _, err := storage.Put(ctx, key, []byte("must-not-replace")); !errors.Is(err, ErrConflict) {
		return ErrUnsupported
	}
	read, err := storage.Get(ctx, key, version, int64(len(data)))
	if err != nil {
		return err
	}
	if !bytes.Equal(data, read) {
		return ErrIntegrity
	}
	if err := storage.Delete(ctx, key, version); err != nil {
		return err
	}
	if _, err := storage.Get(ctx, key, version, int64(len(data))); !errors.Is(err, ErrMissing) {
		return ErrUnsupported
	}
	return nil
}
