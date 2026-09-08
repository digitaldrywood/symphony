package artifact

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestPilotArtifactTraffic(t *testing.T) {
	t.Parallel()
	for _, size := range []int{1024, 64 << 10} {
		t.Run(fmt.Sprintf("bytes_%d", size), func(t *testing.T) {
			t.Parallel()
			config := testConfig(t)
			config.Mode, config.HostedOptIn = "hosted", true
			service, err := NewService(t.Context(), config, &memoryStorage{data: map[string][]byte{}}, &testAllowances{limits: config.Policy.Limits})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := service.Close(); err != nil {
					t.Error(err)
				}
			})
			upload, err := service.Reserve(t.Context(), testReservation(service))
			if err != nil {
				t.Fatal(err)
			}
			part := testPart(0, strings.Repeat("x", size))
			object, err := service.Append(t.Context(), upload.ArtifactID, part)
			if err != nil {
				t.Fatal(err)
			}
			before, err := service.Usage(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.Append(t.Context(), upload.ArtifactID, part); err != nil {
				t.Fatal(err)
			}
			after, err := service.Usage(t.Context())
			if err != nil || after.RelayBytes != before.RelayBytes || after.StorageRequests-before.StorageRequests != 1 {
				t.Fatalf("retry must only probe the deletion marker: %+v %+v %v", before, after, err)
			}
			ref, err := service.Finalize(t.Context(), upload.ArtifactID, "complete", nil)
			if err != nil || ref.State != "complete" {
				t.Fatalf("finalization: %+v %v", ref, err)
			}
			_, data, err := service.ReadObject(t.Context(), upload.ArtifactID, ref.Revision, object.ID)
			if err != nil || !bytes.Equal(data, part.Data) {
				t.Fatalf("durable object read: bytes=%d error=%v", len(data), err)
			}
			usage, err := service.Usage(t.Context())
			if err != nil || usage.RelayBytes < int64(size*2) || usage.StorageRequests < 2 {
				t.Fatalf("storage traffic: %+v %v", usage, err)
			}
			raw, err := json.Marshal(usage)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("PILOT artifact uploaded_bytes=%d duplicate_append=1 retry_storage_requests=1 usage=%s", size, raw)
		})
	}
}
