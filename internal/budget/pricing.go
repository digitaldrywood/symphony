package budget

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	DefaultPricingPath          = "priv/pricing/models.yaml"
	defaultPricingFallbackModel = "default"
)

type ModelPricing struct {
	USDPerInputToken float64
	// Cache-write premiums are not modeled until telemetry records cache creation
	// tokens separately.
	USDPerCachedInputToken float64
	USDPerOutputToken      float64
}

type PricingTable map[string]ModelPricing

// DefaultPricingTable returns published API token rates. Cached input rates are
// provider cache-read prices and must be verified against published pricing
// when models are added or updated. Under subscription auth, computed USD is
// notional but still used for budget pacing.
func DefaultPricingTable() PricingTable {
	sol := ModelPricing{
		USDPerInputToken:       0.000004,
		USDPerCachedInputToken: 0.0000004,
		USDPerOutputToken:      0.000020,
	}
	astra := ModelPricing{
		USDPerInputToken:       0.000010,
		USDPerCachedInputToken: 0.000001,
		USDPerOutputToken:      0.000050,
	}
	gpt55 := ModelPricing{
		USDPerInputToken:       0.000005,
		USDPerCachedInputToken: 0.0000005,
		USDPerOutputToken:      0.000030,
	}
	claudeFable5 := ModelPricing{
		USDPerInputToken:       0.000010,
		USDPerCachedInputToken: 0.000001,
		USDPerOutputToken:      0.000050,
	}
	claudeOpus48 := ModelPricing{
		USDPerInputToken:       0.000005,
		USDPerCachedInputToken: 0.0000005,
		USDPerOutputToken:      0.000025,
	}
	claudeSonnet5 := ModelPricing{
		USDPerInputToken:       0.000003,
		USDPerCachedInputToken: 0.0000003,
		USDPerOutputToken:      0.000015,
	}
	claudeHaiku45 := ModelPricing{
		USDPerInputToken:       0.000001,
		USDPerCachedInputToken: 0.0000001,
		USDPerOutputToken:      0.000005,
	}

	return PricingTable{
		"gpt-5.6-sol":               sol,
		"gpt-5.6":                   sol,
		"gpt-6-astra":               astra,
		defaultPricingFallbackModel: gpt55,
		"gpt-5.5":                   gpt55,
		"gpt-5.4": {
			USDPerInputToken:       0.0000025,
			USDPerCachedInputToken: 0.00000025,
			USDPerOutputToken:      0.000015,
		},
		"gpt-5.4-mini": {
			USDPerInputToken:       0.00000075,
			USDPerCachedInputToken: 0.000000075,
			USDPerOutputToken:      0.0000045,
		},
		"gpt-5.3-codex": {
			USDPerInputToken:       0.00000175,
			USDPerCachedInputToken: 0.000000175,
			USDPerOutputToken:      0.000014,
		},
		"claude-fable-5":   claudeFable5,
		"fable":            claudeFable5,
		"claude-opus-4-8":  claudeOpus48,
		"opus":             claudeOpus48,
		"claude-sonnet-5":  claudeSonnet5,
		"sonnet":           claudeSonnet5,
		"claude-haiku-4-5": claudeHaiku45,
		"haiku":            claudeHaiku45,
	}
}

func LoadPricing(path string) (PricingTable, error) {
	if strings.TrimSpace(path) == "" {
		return DefaultPricingTable(), nil
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read pricing file: %w", err)
	}

	pricing, err := DecodePricing(raw)
	if err != nil {
		return nil, fmt.Errorf("decode pricing file: %w", err)
	}
	return pricing, nil
}

func PricingForConfig(cfg Config) (PricingTable, error) {
	path := strings.TrimSpace(cfg.PricingPath)
	if path == "" || path == DefaultPricingPath {
		return DefaultPricingTable(), nil
	}
	return LoadPricing(path)
}

func DecodePricing(raw []byte) (PricingTable, error) {
	var decoded map[string]any
	if err := yaml.Unmarshal(raw, &decoded); err != nil {
		return nil, err
	}

	models := decoded
	if value, ok := decoded["models"]; ok {
		models = mapValue(value)
	}

	pricing := PricingTable{}
	for model, row := range models {
		model = normalizeModel(model)
		if model == "" {
			continue
		}
		modelPricing, ok := normalizePricingRow(model, row)
		if !ok {
			continue
		}
		pricing[model] = modelPricing
	}
	return pricing, nil
}

func (p PricingTable) Lookup(model string) (ModelPricing, bool) {
	if p == nil {
		p = DefaultPricingTable()
	}

	if row, ok := p[normalizeModel(model)]; ok {
		return row, true
	}
	if row, ok := p[defaultPricingFallbackModel]; ok {
		return row, true
	}
	row, ok := DefaultPricingTable()[defaultPricingFallbackModel]
	return row, ok
}

func UsageCostUSD(pricing PricingTable, model string, inputTokens int64, cachedInputTokens int64, outputTokens int64) (float64, bool) {
	modelPricing, ok := pricing.Lookup(model)
	if !ok {
		return 0, false
	}
	input := nonNegative(inputTokens)
	cached := min(nonNegative(cachedInputTokens), input)
	uncached := input - cached
	return float64(uncached)*modelPricing.USDPerInputToken +
		float64(cached)*modelPricing.USDPerCachedInputToken +
		float64(nonNegative(outputTokens))*modelPricing.USDPerOutputToken, true
}

func normalizePricingRow(model string, value any) (ModelPricing, bool) {
	row := mapValue(value)
	if row == nil {
		return ModelPricing{}, false
	}

	input, inputOK := numericValue(row["usd_per_input_token"])
	if !inputOK {
		input, inputOK = perTokenFromMillion(row["input_usd_per_1m_tokens"])
	}

	output, outputOK := numericValue(row["usd_per_output_token"])
	if !outputOK {
		output, outputOK = perTokenFromMillion(row["output_usd_per_1m_tokens"])
	}

	cached, cachedOK := numericValue(row["usd_per_cached_input_token"])
	if !cachedOK {
		cached, cachedOK = perTokenFromMillion(row["cached_input_usd_per_1m_tokens"])
	}
	if !cachedOK && inputOK {
		slog.Warn("pricing row missing cached input rate; using input rate", "model", model)
		cached = input
		cachedOK = true
	}

	if !inputOK || !cachedOK || !outputOK || input < 0 || cached < 0 || output < 0 {
		return ModelPricing{}, false
	}

	return ModelPricing{
		USDPerInputToken:       input,
		USDPerCachedInputToken: cached,
		USDPerOutputToken:      output,
	}, true
}

func perTokenFromMillion(value any) (float64, bool) {
	number, ok := numericValue(value)
	if !ok {
		return 0, false
	}
	return number / 1_000_000, true
}

func mapValue(value any) map[string]any {
	switch typed := value.(type) {
	case map[string]any:
		return typed
	default:
		return nil
	}
}

func numericValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float64:
		return typed, true
	case string:
		number, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return 0, false
		}
		return number, true
	default:
		return 0, false
	}
}

func normalizeModel(model string) string {
	return strings.ToLower(strings.TrimSpace(model))
}
