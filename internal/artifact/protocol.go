package artifact

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"
)

type Binding struct {
	ServiceID        string `json:"service_id"`
	Origin           string `json:"origin"`
	Mode             string `json:"mode"`
	HostedOptIn      bool   `json:"hosted_opt_in"`
	PublisherTokenID string `json:"publisher_token_id"`
}

type Grant struct {
	Token      string    `json:"token"`
	Origin     string    `json:"origin"`
	ArtifactID string    `json:"artifact_id"`
	Revision   int64     `json:"revision"`
	SHA256     string    `json:"sha256"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type ReadAuthorization struct {
	ProjectID  string `json:"project_id,omitempty"`
	Token      string `json:"token"`
	ArtifactID string `json:"artifact_id"`
	Revision   int64  `json:"revision"`
}

type Authorizer interface {
	Upload(context.Context, string, Reservation) error
	Read(context.Context, ReadAuthorization) error
}

type RemoteHub struct {
	Origin         string
	ServiceID      string
	OrganizationID string
	ProjectID      string
	PublisherToken func() string
	Client         *http.Client
}

func (h *RemoteHub) base(projectID string) string {
	return h.Origin + "/api/v2/organizations/" + h.OrganizationID + "/projects/" + projectID
}

func (h *RemoteHub) Upload(ctx context.Context, token string, r Reservation) error {
	if r.OrganizationID != h.OrganizationID || !ValidID(r.ProjectID, "prj") || h.ProjectID != "" && r.ProjectID != h.ProjectID {
		return ErrDenied
	}
	return h.request(ctx, token, h.base(r.ProjectID)+"/work-items/"+r.WorkItemID+"/artifact-authority", r)
}

func (h *RemoteHub) Read(ctx context.Context, r ReadAuthorization) error {
	if r.ProjectID == "" {
		r.ProjectID = h.ProjectID
	}
	if !ValidID(r.ProjectID, "prj") || h.ProjectID != "" && r.ProjectID != h.ProjectID {
		return ErrDenied
	}
	return h.request(ctx, h.PublisherToken(), h.base(r.ProjectID)+"/artifact-services/"+h.ServiceID+"/authorize", r)
}

func (h *RemoteHub) Publish(ctx context.Context, r Reference) error {
	if r.OrganizationID != h.OrganizationID || !ValidID(r.ProjectID, "prj") || h.ProjectID != "" && r.ProjectID != h.ProjectID {
		return ErrDenied
	}
	return h.request(ctx, h.PublisherToken(), h.base(r.ProjectID)+"/artifact-services/"+h.ServiceID+"/receipts", r)
}

func (h *RemoteHub) request(ctx context.Context, token, target string, input any) error {
	if !ValidOrigin(h.Origin) || token == "" {
		return ErrAuthorization
	}
	body, err := json.Marshal(input)
	if err != nil {
		return ErrInvalid
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return ErrAuthorization
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	client := h.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	response, err := client.Do(req)
	if err != nil {
		return ErrAuthorization
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden || response.StatusCode == http.StatusNotFound {
		return ErrDenied
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return ErrAuthorization
	}
	return nil
}

func Decode(r io.Reader, target any, limit int64) error {
	body, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil || int64(len(body)) > limit {
		return ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return ErrInvalid
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return ErrInvalid
	}
	return nil
}

func Revision(value string) (int64, error) {
	r, err := strconv.ParseInt(value, 10, 64)
	if err != nil || r < 1 {
		return 0, ErrInvalid
	}
	return r, nil
}
