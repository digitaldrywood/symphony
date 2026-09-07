package budget

import (
	"bytes"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultPricingTable(t *testing.T) {
	t.Parallel()

	pricing := DefaultPricingTable()

	tests := []struct {
		model      string
		wantInput  float64
		wantCached float64
		wantOutput float64
	}{
		{model: "gpt-5.6-sol", wantInput: 0.000004, wantCached: 0.0000004, wantOutput: 0.000020},
		{model: "gpt-5.6", wantInput: 0.000004, wantCached: 0.0000004, wantOutput: 0.000020},
		{model: "gpt-6-astra", wantInput: 0.000010, wantCached: 0.000001, wantOutput: 0.000050},
		{
			model:      " GPT-5.5 ",
			wantInput:  0.000005,
			wantCached: 0.0000005,
			wantOutput: 0.000030,
		},
		{
			model:      "default",
			wantInput:  0.000005,
			wantCached: 0.0000005,
			wantOutput: 0.000030,
		},
		{
			model:      "gpt-5.4",
			wantInput:  0.0000025,
			wantCached: 0.00000025,
			wantOutput: 0.000015,
		},
		{
			model:      "gpt-5.4-mini",
			wantInput:  0.00000075,
			wantCached: 0.000000075,
			wantOutput: 0.0000045,
		},
		{
			model:      "gpt-5.3-codex",
			wantInput:  0.00000175,
			wantCached: 0.000000175,
			wantOutput: 0.000014,
		},
		{
			model:      "claude-fable-5",
			wantInput:  0.000010,
			wantCached: 0.000001,
			wantOutput: 0.000050,
		},
		{
			model:      "fable",
			wantInput:  0.000010,
			wantCached: 0.000001,
			wantOutput: 0.000050,
		},
		{
			model:      "claude-opus-4-8",
			wantInput:  0.000005,
			wantCached: 0.0000005,
			wantOutput: 0.000025,
		},
		{
			model:      "opus",
			wantInput:  0.000005,
			wantCached: 0.0000005,
			wantOutput: 0.000025,
		},
		{
			model:      "claude-sonnet-5",
			wantInput:  0.000003,
			wantCached: 0.0000003,
			wantOutput: 0.000015,
		},
		{
			model:      "sonnet",
			wantInput:  0.000003,
			wantCached: 0.0000003,
			wantOutput: 0.000015,
		},
		{
			model:      "claude-haiku-4-5",
			wantInput:  0.000001,
			wantCached: 0.0000001,
			wantOutput: 0.000005,
		},
		{
			model:      "haiku",
			wantInput:  0.000001,
			wantCached: 0.0000001,
			wantOutput: 0.000005,
		},
	}

	for _, tt := range tests {
		t.Run(tt.model, func(t *testing.T) {
			t.Parallel()

			row, ok := pricing.Lookup(tt.model)
			if !ok {
				t.Fatal("DefaultPricingTable().Lookup() ok = false, want true")
			}
			assertInDelta(t, row.USDPerInputToken, tt.wantInput)
			assertInDelta(t, row.USDPerCachedInputToken, tt.wantCached)
			assertInDelta(t, row.USDPerOutputToken, tt.wantOutput)
		})
	}
}

func TestSolAstraExternalPricingOverride(t *testing.T) {
	for _, model := range []string{"gpt-5.6-sol", "gpt-5.6", "gpt-6-astra"} {
		t.Run(model, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "prices.yaml")
			raw := "models:\n  " + model + ":\n    input_usd_per_1m_tokens: 2\n    cached_input_usd_per_1m_tokens: 0.2\n    output_usd_per_1m_tokens: 6\n"
			if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
				t.Fatal(err)
			}
			pricing, err := PricingForConfig(Config{PricingPath: path})
			if err != nil {
				t.Fatal(err)
			}
			cost, ok := UsageCostUSD(pricing, model, 1_000_000, 500_000, 1_000_000)
			if !ok || math.Abs(cost-7.1) > 1e-9 {
				t.Fatalf("override cost = %v, %v; want 7.1", cost, ok)
			}
		})
	}
}

func TestDefaultPricingTableCachedInputRatesAreDiscounted(t *testing.T) {
	t.Parallel()

	for model, row := range DefaultPricingTable() {
		if row.USDPerCachedInputToken >= row.USDPerInputToken {
			t.Fatalf("%s cached input rate = %.12f, want less than input rate %.12f", model, row.USDPerCachedInputToken, row.USDPerInputToken)
		}
	}
}

func TestDecodePricing(t *testing.T) {
	t.Parallel()

	raw := []byte(`
models:
  gpt-test:
    input_usd_per_1m_tokens: "10000"
    cached_input_usd_per_1m_tokens: 2500
    output_usd_per_1m_tokens: 20000
  gpt-per-token:
    usd_per_input_token: "0.01"
    usd_per_cached_input_token: "0.003"
    usd_per_output_token: 0.02
  gpt-fallback:
    usd_per_input_token: 0.04
    usd_per_output_token: 0.05
  default:
    input_usd_per_1m_tokens: 6000
    cached_input_usd_per_1m_tokens: 600
    output_usd_per_1m_tokens: 36000
  invalid-row: bad
  missing-output:
    usd_per_input_token: 0.01
  negative:
    usd_per_input_token: -0.01
    usd_per_output_token: 0.02
`)

	pricing, err := DecodePricing(raw)
	if err != nil {
		t.Fatalf("DecodePricing() error = %v", err)
	}

	million, ok := pricing.Lookup("gpt-test")
	if !ok {
		t.Fatal("pricing.Lookup(gpt-test) ok = false, want true")
	}
	assertInDelta(t, million.USDPerInputToken, 0.01)
	assertInDelta(t, million.USDPerCachedInputToken, 0.0025)
	assertInDelta(t, million.USDPerOutputToken, 0.02)

	perToken, ok := pricing.Lookup("GPT-PER-TOKEN")
	if !ok {
		t.Fatal("pricing.Lookup(GPT-PER-TOKEN) ok = false, want true")
	}
	assertInDelta(t, perToken.USDPerInputToken, 0.01)
	assertInDelta(t, perToken.USDPerCachedInputToken, 0.003)
	assertInDelta(t, perToken.USDPerOutputToken, 0.02)

	fallback, ok := pricing.Lookup("gpt-fallback")
	if !ok {
		t.Fatal("pricing.Lookup(gpt-fallback) ok = false, want true")
	}
	assertInDelta(t, fallback.USDPerInputToken, 0.04)
	assertInDelta(t, fallback.USDPerCachedInputToken, 0.04)
	assertInDelta(t, fallback.USDPerOutputToken, 0.05)

	unknown, ok := pricing.Lookup("gpt-future")
	if !ok {
		t.Fatal("pricing.Lookup(gpt-future) ok = false, want configurable default")
	}
	assertInDelta(t, unknown.USDPerInputToken, 0.006)
	assertInDelta(t, unknown.USDPerCachedInputToken, 0.0006)
	assertInDelta(t, unknown.USDPerOutputToken, 0.036)

	for _, model := range []string{"invalid-row", "missing-output", "negative"} {
		if _, ok := pricing[normalizeModel(model)]; ok {
			t.Fatalf("pricing[%q] exists, want invalid row skipped", model)
		}
	}
}

func TestDecodePricingWarnsWhenCachedInputRateMissing(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})

	raw := []byte(`
models:
  gpt-fallback:
    usd_per_input_token: 0.04
    usd_per_output_token: 0.05
`)

	pricing, err := DecodePricing(raw)
	if err != nil {
		t.Fatalf("DecodePricing() error = %v", err)
	}

	row, ok := pricing.Lookup("gpt-fallback")
	if !ok {
		t.Fatal("pricing.Lookup(gpt-fallback) ok = false, want true")
	}
	assertInDelta(t, row.USDPerCachedInputToken, 0.04)

	output := logs.String()
	if !strings.Contains(output, "pricing row missing cached input rate; using input rate") ||
		!strings.Contains(output, "model=gpt-fallback") {
		t.Fatalf("log output = %q, want missing cached input warning", output)
	}
}

func TestLoadPricing(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "models.yaml")
	if err := os.WriteFile(path, []byte(`
models:
  gpt-file:
    usd_per_input_token: 0.03
    usd_per_cached_input_token: 0.003
    usd_per_output_token: 0.04
`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	pricing, err := LoadPricing(path)
	if err != nil {
		t.Fatalf("LoadPricing() error = %v", err)
	}
	row, ok := pricing.Lookup("gpt-file")
	if !ok {
		t.Fatal("pricing.Lookup(gpt-file) ok = false, want true")
	}
	assertInDelta(t, row.USDPerInputToken, 0.03)
	assertInDelta(t, row.USDPerCachedInputToken, 0.003)
	assertInDelta(t, row.USDPerOutputToken, 0.04)

	if _, err := LoadPricing(filepath.Join(dir, "missing.yaml")); err == nil {
		t.Fatal("LoadPricing(missing) error = nil, want error")
	}
}

func TestPricingForConfigUsesEmbeddedDefaultPath(t *testing.T) {
	t.Parallel()

	pricing, err := PricingForConfig(Config{PricingPath: DefaultPricingPath})
	if err != nil {
		t.Fatalf("PricingForConfig() error = %v", err)
	}
	if _, ok := pricing.Lookup("gpt-5.5"); !ok {
		t.Fatal("PricingForConfig(default).Lookup(gpt-5.5) ok = false, want true")
	}
}

func TestUsageCostUSD(t *testing.T) {
	t.Parallel()

	pricing := PricingTable{
		"gpt-test": {
			USDPerInputToken:       0.01,
			USDPerCachedInputToken: 0.001,
			USDPerOutputToken:      0.02,
		},
		"default": {
			USDPerInputToken:       0.03,
			USDPerCachedInputToken: 0.003,
			USDPerOutputToken:      0.04,
		},
	}

	tests := []struct {
		name              string
		model             string
		inputTokens       int64
		cachedInputTokens int64
		outputTokens      int64
		want              float64
		wantOK            bool
	}{
		{
			name:              "zero cached input",
			model:             " GPT-TEST ",
			inputTokens:       10,
			cachedInputTokens: 0,
			outputTokens:      5,
			want:              0.20,
			wantOK:            true,
		},
		{
			name:              "mixed cached input",
			model:             "gpt-test",
			inputTokens:       10,
			cachedInputTokens: 4,
			outputTokens:      5,
			want:              0.164,
			wantOK:            true,
		},
		{
			name:              "all cached input",
			model:             "gpt-test",
			inputTokens:       10,
			cachedInputTokens: 10,
			outputTokens:      5,
			want:              0.11,
			wantOK:            true,
		},
		{
			name:              "cached input clamps to input",
			model:             "gpt-test",
			inputTokens:       10,
			cachedInputTokens: 12,
			outputTokens:      5,
			want:              0.11,
			wantOK:            true,
		},
		{
			name:              "unknown model",
			model:             "missing",
			inputTokens:       10,
			cachedInputTokens: 4,
			outputTokens:      5,
			want:              0.392,
			wantOK:            true,
		},
		{
			name:              "empty model",
			model:             "",
			inputTokens:       10,
			cachedInputTokens: 4,
			outputTokens:      5,
			want:              0.392,
			wantOK:            true,
		},
		{
			name:              "negative tokens are ignored",
			model:             "gpt-test",
			inputTokens:       -10,
			cachedInputTokens: -4,
			outputTokens:      5,
			want:              0.10,
			wantOK:            true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := UsageCostUSD(pricing, tt.model, tt.inputTokens, tt.cachedInputTokens, tt.outputTokens)
			if ok != tt.wantOK {
				t.Fatalf("UsageCostUSD() ok = %v, want %v", ok, tt.wantOK)
			}
			assertInDelta(t, got, tt.want)
		})
	}
}

func TestUsageCostUSDDefaultGPT55CachedSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		model             string
		inputTokens       int64
		cachedInputTokens int64
		outputTokens      int64
		want              float64
	}{
		{
			name:              "prices live cached session near provider cost",
			model:             "gpt-5.5",
			inputTokens:       12_590_000,
			cachedInputTokens: 12_380_000,
			outputTokens:      41_000,
			want:              8.47,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := UsageCostUSD(nil, tt.model, tt.inputTokens, tt.cachedInputTokens, tt.outputTokens)
			if !ok {
				t.Fatal("UsageCostUSD() ok = false, want true")
			}
			assertInDelta(t, got, tt.want)
			if got >= 60 {
				t.Fatalf("UsageCostUSD() = %.2f, want cached-rate pricing", got)
			}
		})
	}
}

func TestUsageCostUSDDefaultClaudePricing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		model  string
		want   float64
		wantOK bool
	}{
		{
			model:  "claude-fable-5",
			want:   60,
			wantOK: true,
		},
		{
			model:  "fable",
			want:   60,
			wantOK: true,
		},
		{
			model:  "claude-opus-4-8",
			want:   30,
			wantOK: true,
		},
		{
			model:  "opus",
			want:   30,
			wantOK: true,
		},
		{
			model:  "claude-sonnet-5",
			want:   18,
			wantOK: true,
		},
		{
			model:  "sonnet",
			want:   18,
			wantOK: true,
		},
		{
			model:  "claude-haiku-4-5",
			want:   6,
			wantOK: true,
		},
		{
			model:  "haiku",
			want:   6,
			wantOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.model, func(t *testing.T) {
			t.Parallel()

			got, ok := UsageCostUSD(nil, tt.model, 1_000_000, 0, 1_000_000)
			if ok != tt.wantOK {
				t.Fatalf("UsageCostUSD() ok = %v, want %v", ok, tt.wantOK)
			}
			assertInDelta(t, got, tt.want)
		})
	}
}
