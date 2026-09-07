package artifact

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/smithy-go"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestS3ProviderContracts(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, endpoint, region, host string
		pathStyle, versioned         bool
	}{
		{"AWS", "", "us-east-1", "private-bucket.s3.us-east-1.amazonaws.com", false, true},
		{"Spaces", "https://nyc3.digitaloceanspaces.com", "us-east-1", "private-bucket.nyc3.digitaloceanspaces.com", false, false},
		{"Tigris", "https://t3.storage.dev", "auto", "t3.storage.dev", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			objects := map[string][]byte{}
			transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Host != test.host {
					t.Errorf("host %s want %s", r.URL.Host, test.host)
				}
				if !strings.Contains(r.Header.Get("Authorization"), "/"+test.region+"/s3/aws4_request") {
					t.Errorf("signing region missing")
				}
				rec := httptest.NewRecorder()
				key := r.URL.Path
				if test.pathStyle && !strings.HasPrefix(key, "/private-bucket/") {
					t.Error("path addressing")
				}
				data, exists := objects[key]
				fail := func(code int, name string) {
					rec.WriteHeader(code)
					_, _ = io.WriteString(rec, "<Error><Code>"+name+"</Code></Error>")
				}
				switch r.Method {
				case http.MethodPut:
					if r.Header.Get("If-None-Match") != "*" || r.Header.Get("X-Amz-Checksum-Sha256") == "" {
						t.Error("missing conditional write/checksum")
					}
					if exists {
						fail(412, "PreconditionFailed")
						break
					}
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Fatal(err)
					}
					objects[key] = body
					if test.versioned {
						rec.Header().Set("X-Amz-Version-Id", "v1")
					}
				case http.MethodHead, http.MethodGet:
					if !exists {
						fail(404, "NoSuchKey")
						break
					}
					if test.versioned {
						rec.Header().Set("X-Amz-Version-Id", "v1")
					}
					if r.Method == http.MethodGet {
						if test.versioned && r.URL.Query().Get("versionId") != "v1" {
							t.Error("version not pinned")
						}
						_, _ = rec.Write(data)
					}
				case http.MethodDelete:
					if test.versioned && r.URL.Query().Get("versionId") != "v1" {
						t.Error("version delete missing")
					}
					delete(objects, key)
					rec.WriteHeader(204)
				default:
					t.Errorf("unexpected %s", r.Method)
				}
				return rec.Result(), nil
			})
			cfg := StorageConfig{Kind: "s3", Bucket: "private-bucket", Endpoint: test.endpoint, Region: test.region, PathStyle: test.pathStyle, RequireVersioning: test.versioned}
			storage := NewS3Storage(cfg, aws.Config{Region: test.region, Credentials: credentials.NewStaticCredentialsProvider("example-key", "example-secret", ""), HTTPClient: &http.Client{Transport: transport}})
			if err := VerifyStorage(t.Context(), storage); err != nil {
				t.Fatal(err)
			}
			version, err := storage.Put(t.Context(), "object", []byte("data"))
			if err != nil {
				t.Fatal(err)
			}
			retry, err := storage.Put(t.Context(), "object", []byte("different"))
			if !errors.Is(err, ErrConflict) || retry != version {
				t.Fatal(retry, version, err)
			}
			if _, err := storage.Get(t.Context(), "object", version, 1); !errors.Is(err, ErrIntegrity) {
				t.Fatal(err)
			}
			if err := storage.Delete(t.Context(), "object", ""); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorageCapabilityFailures(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		code string
		want error
	}{{"NoSuchKey", ErrMissing}, {"NoSuchVersion", ErrMissing}, {"PreconditionFailed", ErrConflict}, {"NotImplemented", ErrUnsupported}, {"AccessDenied", ErrStorage}} {
		t.Run(test.code, func(t *testing.T) {
			if got := storageError(&smithy.GenericAPIError{Code: test.code}); !errors.Is(got, test.want) {
				t.Fatal(got)
			}
		})
	}
	for _, cfg := range []StorageConfig{{Kind: "dropbox"}, {Kind: "s3", Bucket: "bad/path", Region: "auto"}, {Kind: "s3", Bucket: "private", Region: "auto", Endpoint: "https://user:secret@example.com"}} {
		if _, err := NewStorage(t.Context(), cfg); err == nil {
			t.Fatal("invalid storage accepted")
		}
	}
	for _, origin := range []string{"http://example.com", "https://example.com/?token=secret", "https://example.com/#fragment", "https://example.com/path", ""} {
		if ValidOrigin(origin) {
			t.Error(origin)
		}
	}
}

type brokenStorage struct {
	Storage
	overwrite, corrupt bool
}

func (s brokenStorage) Put(ctx context.Context, key string, data []byte) (string, error) {
	if s.overwrite {
		return "", nil
	}
	return s.Storage.Put(ctx, key, data)
}
func (s brokenStorage) Get(ctx context.Context, key, version string, limit int64) ([]byte, error) {
	if s.corrupt {
		return bytes.Repeat([]byte("x"), int(limit)), nil
	}
	return s.Storage.Get(ctx, key, version, limit)
}

func TestVerifyStorageRejectsUnsupportedSemantics(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name               string
		overwrite, corrupt bool
		want               error
	}{{"overwrite", true, false, ErrUnsupported}, {"corrupt", false, true, ErrIntegrity}} {
		t.Run(test.name, func(t *testing.T) {
			s := brokenStorage{Storage: &memoryStorage{data: map[string][]byte{}}, overwrite: test.overwrite, corrupt: test.corrupt}
			if err := VerifyStorage(t.Context(), s); !errors.Is(err, test.want) {
				t.Fatal(err)
			}
		})
	}
}
