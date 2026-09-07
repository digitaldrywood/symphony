package artifact

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

type Client struct {
	Origin string
	Token  func(context.Context) (string, error)
}

func (c *Client) request(ctx context.Context, method, path string, input, output any) error {
	if !ValidOrigin(c.Origin) || c.Token == nil {
		return ErrInvalid
	}
	token, err := c.Token(ctx)
	if err != nil {
		return ErrAuthorization
	}
	body, err := json.Marshal(input)
	if err != nil {
		return ErrInvalid
	}
	req, err := http.NewRequestWithContext(ctx, method, c.Origin+path, bytes.NewReader(body))
	if err != nil {
		return ErrInvalid
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return ErrStorage
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var failure struct {
			Code string `json:"code"`
		}
		if err := Decode(resp.Body, &failure, 1024); err != nil {
			return ErrStorage
		}
		for _, known := range []error{ErrInvalid, ErrIntegrity, ErrMissing, ErrStorage, ErrUnsupported, ErrConflict, ErrQuota, ErrExpired, ErrDenied, ErrAuthorization} {
			if known.Error() == failure.Code {
				return known
			}
		}
		return ErrStorage
	}
	if output == nil {
		_, err := io.Copy(io.Discard, io.LimitReader(resp.Body, MaxManifestBytes))
		return err
	}
	return Decode(resp.Body, output, MaxManifestBytes)
}

func (c *Client) Reserve(ctx context.Context, r Reservation) (Upload, error) {
	var u Upload
	err := c.request(ctx, http.MethodPost, "/v1/uploads", r, &u)
	return u, err
}

func (c *Client) Append(ctx context.Context, id string, p Part) (Object, error) {
	var obj Object
	if !ValidID(id, "artifact") {
		return obj, ErrInvalid
	}
	err := c.request(ctx, http.MethodPut, "/v1/uploads/"+id+"/parts", p, &obj)
	return obj, err
}

func (c *Client) Finalize(ctx context.Context, id, state string, capture *Capture) (Reference, error) {
	var ref Reference
	if !ValidID(id, "artifact") {
		return ref, ErrInvalid
	}
	err := c.request(ctx, http.MethodPost, "/v1/uploads/"+id+"/finalize", struct {
		State   string   `json:"state"`
		Capture *Capture `json:"capture,omitempty"`
	}{state, capture}, &ref)
	return ref, err
}
