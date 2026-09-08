package artifact

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type allowanceFunction func(context.Context, string) (Limits, error)

func (f allowanceFunction) Limits(ctx context.Context, organization string) (Limits, error) {
	return f(ctx, organization)
}

func TestHostedTrafficAndAdmittedCompletion(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"customer", "hosted"} {
		t.Run(mode, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.Mode = mode
			cfg.HostedOptIn = mode == "hosted"
			limits := cfg.Policy.Limits
			limits.WindowSeconds = 3600
			limits.TelemetrySeconds = 86400
			limits.RelayBytes = 8 << 20
			calls := 0
			allow := allowanceFunction(func(context.Context, string) (Limits, error) { calls++; return limits, nil })
			s, err := NewService(t.Context(), cfg, &memoryStorage{data: map[string][]byte{}}, allow)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			reservation := testReservation(s)
			u, err := s.Reserve(t.Context(), reservation)
			if err != nil {
				t.Fatal(err)
			}
			again, err := s.Reserve(t.Context(), reservation)
			if err != nil || again.ArtifactID != u.ArtifactID {
				t.Fatalf("retry: %v %v", again, err)
			}
			limits.RetainedBytes = 0
			limits.UploadBytes = 0
			limits.RelayBytes = 0
			part := testPart(0, "customer-content-sentinel")
			if _, err := s.Append(t.Context(), u.ArtifactID, part); err != nil {
				t.Fatalf("downgrade prevented admitted completion: %v", err)
			}
			before, err := s.Usage(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Append(t.Context(), u.ArtifactID, part); err != nil {
				t.Fatal(err)
			}
			after, err := s.Usage(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if after.RelayBytes != before.RelayBytes {
				t.Fatal("duplicate part counted twice")
			}
			if mode == "customer" {
				if err := s.ReportHostedUsage(t.Context()); err != nil || calls != 0 {
					t.Fatalf("customer storage contacted allowances: %d %v", calls, err)
				}
				return
			}
			if after.RelayBytes != 2*int64(len(part.Data)) || after.StorageRequests < 2 {
				t.Fatalf("traffic = %+v", after)
			}
			request := testReservation(s)
			request.Key = "new"
			if _, err := s.Reserve(t.Context(), request); !errors.Is(err, ErrQuota) {
				t.Fatalf("new allocation after downgrade: %v", err)
			}
			if _, err := s.Finalize(t.Context(), u.ArtifactID, "complete", nil); err != nil {
				t.Fatal(err)
			}
			var rows int
			s.now = func() time.Time { return time.Now().Add(48 * time.Hour) }
			if err := s.recordTraffic(t.Context(), 1, 0, 1); err != nil {
				t.Fatal(err)
			}
			if err := s.catalog.db.QueryRowContext(t.Context(), "SELECT count(*) FROM hosted_traffic").Scan(&rows); err != nil || rows != 1 {
				t.Fatalf("telemetry retention %d: %v", rows, err)
			}
		})
	}
}

func TestHostedRelayQuotaAndRemoteLimits(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		status int
		want   error
	}{{"allowed", 200, nil}, {"quota", 429, ErrQuota}, {"denied", 403, ErrDenied}, {"unavailable", 503, ErrAuthorization}} {
		t.Run(test.name, func(t *testing.T) {
			organization, service := NewID("org"), NewID("service")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v2/organizations/"+organization+"/artifact-allowances/"+service || r.Header.Get("Authorization") != "Bearer publisher" {
					t.Error("wrong allowance scope")
				}
				var usage Usage
				if err := Decode(r.Body, &usage, 4096); err != nil || usage.RetainedBytes != 123 {
					t.Errorf("usage=%+v err=%v", usage, err)
				}
				w.WriteHeader(test.status)
				if test.status == 200 {
					if err := json.NewEncoder(w).Encode(Limits{RelayBytes: 1, WindowSeconds: 3600, TelemetrySeconds: 86400}); err != nil {
						t.Error(err)
					}
				}
			}))
			defer server.Close()
			h := &RemoteHub{Origin: server.URL, ServiceID: service, OrganizationID: organization, PublisherToken: func() string { return "publisher" }, Usage: func(context.Context) (Usage, error) { return Usage{RetainedBytes: 123}, nil }}
			_, err := h.Limits(t.Context(), organization)
			if !errors.Is(err, test.want) {
				t.Fatalf("Limits=%v", err)
			}
			if _, err := h.Limits(t.Context(), NewID("org")); !errors.Is(err, ErrDenied) {
				t.Fatal("cross organization allowance call")
			}
		})
	}
	cfg := testConfig(t)
	cfg.Mode = "hosted"
	cfg.HostedOptIn = true
	limits := cfg.Policy.Limits
	limits.WindowSeconds = 3600
	limits.TelemetrySeconds = 86400
	limits.RelayBytes = 1
	s, err := NewService(t.Context(), cfg, &memoryStorage{data: map[string][]byte{}}, allowanceFunction(func(context.Context, string) (Limits, error) { return limits, nil }))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Reserve(t.Context(), testReservation(s)); !errors.Is(err, ErrQuota) {
		t.Fatalf("relay exhaustion: %v", err)
	}
	usage, err := s.Usage(t.Context())
	if err != nil || usage.ReservedBytes != 0 {
		t.Fatalf("failed allocation retained reservation: %+v %v", usage, err)
	}
}

func TestHostedTrafficSettingsSurviveRestart(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	cfg.Mode, cfg.HostedOptIn = "hosted", true
	storage := &memoryStorage{data: map[string][]byte{}}
	limits := cfg.Policy.Limits
	limits.RelayBytes, limits.WindowSeconds, limits.TelemetrySeconds = 1<<30, 60, 3600
	allow := allowanceFunction(func(context.Context, string) (Limits, error) { return limits, nil })
	service, err := NewService(t.Context(), cfg, storage, allow)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.ReportHostedUsage(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewService(t.Context(), cfg, storage, allow)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.trafficWindow.Load() != 60 || reopened.trafficRetention.Load() != 3600 {
		t.Fatal("restart replaced configured telemetry windows")
	}
}
