package hubclient

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"unicode/utf8"

	"github.com/digitaldrywood/detent/internal/artifact"
	"github.com/digitaldrywood/detent/internal/tracker"
)

func (c *NativeClient) Artifacts(ctx context.Context, item tracker.NativeWorkItemID) ([]artifact.Reference, error) {
	var refs []artifact.Reference
	path, err := nativeItemPath(item)
	if err != nil {
		return nil, err
	}
	err = c.client.request(ctx, http.MethodGet, c.base()+path+"/artifacts", nil, &refs)
	return refs, err
}

func (c *NativeClient) ArtifactGrant(ctx context.Context, item tracker.NativeWorkItemID, id string, revision int64) (artifact.Grant, error) {
	var grant artifact.Grant
	path, err := nativeItemPath(item)
	if err != nil || !artifact.ValidID(id, "artifact") {
		return grant, artifact.ErrInvalid
	}
	err = c.client.request(ctx, http.MethodPost, c.base()+path+"/artifacts/"+id+"/access", struct {
		Revision int64 `json:"revision"`
	}{revision}, &grant)
	return grant, err
}

func (c *NativeConnector) FetchArtifacts(ctx context.Context, item tracker.NativeWorkItemID) ([]artifact.Reference, error) {
	return c.client.Artifacts(ctx, item)
}

func (c *NativeClient) ArtifactReader(token string) *NativeClient {
	return &NativeClient{organization: c.organization, project: c.project, client: &Client{baseURL: c.client.baseURL, httpClient: c.client.httpClient, tokenSource: func() string { return token }}}
}

type nativeArtifacts struct {
	mu          sync.Mutex
	client      *artifact.Client
	directory   string
	base        string
	finished    bool
	incomplete  bool
	uploaded    int64
	sequence    int
	reservation artifact.Reservation
	log         artifact.Upload
}

func (e *nativeExecution) PrepareArtifacts(ctx context.Context, directory string) error {
	a := &e.artifacts
	a.mu.Lock()
	defer a.mu.Unlock()
	client := e.scheduler.client
	if client.artifactServiceID == "" {
		return nil
	}
	if client.artifactBytes <= artifact.MaxManifestBytes || client.artifactBytes > artifact.MaxArtifactBytes {
		return artifact.ErrInvalid
	}
	if a.directory != "" {
		return nil
	}
	native := e.claim.source.client
	var bindings []artifact.Binding
	if err := client.request(ctx, http.MethodGet, native.base()+"/artifact-services", nil, &bindings); err != nil {
		return err
	}
	for _, binding := range bindings {
		if binding.ServiceID != client.artifactServiceID {
			continue
		}
		if !artifact.ValidOrigin(binding.Origin) || binding.Mode != "customer" && binding.Mode != "hosted" || binding.Mode == "hosted" && !binding.HostedOptIn {
			return artifact.ErrDenied
		}
		a.reservation.ServiceID, a.reservation.Mode, a.reservation.HostedOptIn = binding.ServiceID, binding.Mode, binding.HostedOptIn
		a.client = &artifact.Client{Origin: binding.Origin, Token: func(ctx context.Context) (string, error) {
			if client.runner != nil {
				return client.runnerToken(ctx)
			}
			return client.tokenSource(), nil
		}}
	}
	if a.client == nil {
		return artifact.ErrMissing
	}
	spoolDir := filepath.Join(directory, ".detent", "artifacts", e.data.AttemptID)
	if err := os.MkdirAll(spoolDir, 0o700); err != nil {
		return err
	}
	a.reservation = artifact.Reservation{ServiceID: a.reservation.ServiceID, Mode: a.reservation.Mode, HostedOptIn: a.reservation.HostedOptIn, Scope: artifact.Scope{OrganizationID: string(native.organization), ProjectID: string(native.project), WorkItemID: string(e.claim.lease.WorkItemID), RunID: e.data.RunID, AttemptID: e.data.AttemptID}, Key: e.data.AttemptID + ":log", Kind: "log", Bytes: client.artifactBytes, LeaseID: string(e.claim.lease.ID), FencingToken: int64(e.claim.lease.FencingToken)}
	basePath := filepath.Join(spoolDir, "base")
	saved, err := os.ReadFile(basePath)
	if err == nil {
		if !artifact.ValidHash(string(saved), 40) {
			return artifact.ErrIntegrity
		}
		a.base = string(saved)
		a.incomplete = true
	} else if errors.Is(err, os.ErrNotExist) {
		capture, err := artifact.CaptureGit(ctx, directory, "HEAD", "HEAD", 3)
		if err != nil {
			return err
		}
		a.base = capture.Capture.Head
		if err := writeArtifactFile(basePath, []byte(a.base)); err != nil {
			return err
		}
	} else {
		return err
	}
	a.directory = spoolDir
	return nil
}

func writeArtifactFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	return errors.Join(writeErr, file.Sync(), file.Close())
}

func (e *nativeExecution) ArtifactLog(ctx context.Context, delta string) error {
	a := &e.artifacts
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.directory == "" || delta == "" {
		return nil
	}
	if !utf8.ValidString(delta) {
		a.incomplete = true
		return artifact.ErrInvalid
	}
	file, err := os.OpenFile(filepath.Join(a.directory, "log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		a.incomplete = true
		return err
	}
	info, err := file.Stat()
	if err != nil {
		return errors.Join(err, file.Close())
	}
	if info.Size()+int64(len(delta)) > a.reservation.Bytes-artifact.MaxManifestBytes {
		a.incomplete = true
		return errors.Join(artifact.ErrQuota, file.Close())
	}
	_, writeErr := file.WriteString(delta)
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		a.incomplete = true
		return err
	}
	err = a.flush(ctx, false)
	if errors.Is(err, artifact.ErrQuota) || errors.Is(err, artifact.ErrInvalid) {
		a.incomplete = true
	}
	return err
}

func (a *nativeArtifacts) flush(ctx context.Context, final bool) (resultErr error) {
	if a.log.ArtifactID == "" {
		u, err := a.client.Reserve(ctx, a.reservation)
		if err != nil {
			return err
		}
		a.log = u
	}
	if a.log.State == "complete" || a.log.State == "interrupted" {
		return nil
	}
	file, err := os.Open(filepath.Join(a.directory, "log"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	for a.uploaded < info.Size() {
		remaining := info.Size() - a.uploaded
		chunkPath := filepath.Join(a.directory, "part-"+strconv.Itoa(a.sequence))
		frozen, frozenErr := os.ReadFile(chunkPath)
		if frozenErr != nil && !errors.Is(frozenErr, os.ErrNotExist) {
			return frozenErr
		}
		if !final && remaining < 64<<10 && frozenErr != nil {
			return nil
		}
		size := min(remaining, 64<<10)
		if frozenErr == nil {
			if len(frozen) == 0 || len(frozen) > 64<<10 || int64(len(frozen)) > remaining {
				return artifact.ErrIntegrity
			}
			size = int64(len(frozen))
		}
		data := make([]byte, size)
		if _, err := file.ReadAt(data, a.uploaded); err != nil {
			return err
		}
		if !utf8.Valid(data) {
			end := len(data) - 1
			for end > 0 && !utf8.RuneStart(data[end]) {
				end--
			}
			data = data[:end]
			if len(data) == 0 || !utf8.Valid(data) {
				return artifact.ErrInvalid
			}
		}
		if frozenErr == nil {
			if !bytes.Equal(data, frozen) {
				return artifact.ErrIntegrity
			}
		} else if err := writeArtifactFile(chunkPath, data); err != nil {
			return err
		}
		p := artifact.Part{Sequence: a.sequence, MediaType: "text/plain; charset=utf-8", SHA256: artifact.Digest(data), Data: data}
		if _, err := a.client.Append(ctx, a.log.ArtifactID, p); err != nil {
			return err
		}
		a.sequence++
		a.uploaded += int64(len(data))
	}
	return nil
}

func (e *nativeExecution) FinalizeArtifacts(ctx context.Context, directory string) error {
	a := &e.artifacts
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.directory == "" || a.finished {
		return nil
	}
	if err := a.flush(ctx, true); err != nil {
		if !errors.Is(err, artifact.ErrQuota) {
			return err
		}
		a.incomplete = true
	}
	state := "complete"
	if a.incomplete {
		state = "interrupted"
	}
	if a.log.State != "complete" && a.log.State != "interrupted" {
		if _, err := a.client.Finalize(ctx, a.log.ArtifactID, state, nil); err != nil {
			return err
		}
		a.log.State = state
	}
	bundle, err := artifact.CaptureGit(ctx, directory, a.base, "HEAD", 3)
	if err != nil {
		return err
	}
	r := a.reservation
	r.Kind, r.Key = "diff", e.data.AttemptID+":diff"
	u, err := a.client.Reserve(ctx, r)
	if err != nil {
		return err
	}
	if u.State != "complete" {
		for _, part := range bundle.Parts {
			if _, err := a.client.Append(ctx, u.ArtifactID, part); err != nil {
				return err
			}
		}
	}
	if _, err := a.client.Finalize(ctx, u.ArtifactID, "complete", &bundle.Capture); err != nil {
		return err
	}
	a.finished = true
	return nil
}
