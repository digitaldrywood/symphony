package claudecode

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/telemetry"
)

type streamItem struct {
	event claudeEvent
	err   error
}

type claudeEvent struct {
	Type         string         `json:"type"`
	Subtype      string         `json:"subtype"`
	SessionID    string         `json:"session_id"`
	Model        string         `json:"model"`
	Message      *claudeMessage `json:"message"`
	Usage        *claudeUsage   `json:"usage"`
	StreamEvent  *streamEvent   `json:"event"`
	InputTokens  int64          `json:"input_tokens"`
	OutputTokens int64          `json:"output_tokens"`
	TotalTokens  int64          `json:"total_tokens"`
	IsError      bool           `json:"is_error"`
	Result       string         `json:"result"`
	DurationMS   int64          `json:"duration_ms"`
	TotalCostUSD float64        `json:"total_cost_usd"`
	RateLimit    *rateLimitInfo `json:"rate_limit_info"`
}

type rateLimitInfo struct {
	Status        string  `json:"status"`
	ResetsAt      int64   `json:"resetsAt"`
	RateLimitType string  `json:"rateLimitType"`
	Utilization   float64 `json:"utilization"`
}

type claudeMessage struct {
	ID      string         `json:"id"`
	Role    string         `json:"role"`
	Model   string         `json:"model"`
	Content []contentBlock `json:"content"`
	Usage   *claudeUsage   `json:"usage"`
}

type contentBlock struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	Name      string          `json:"name"`
	ToolUseID string          `json:"tool_use_id"`
	Input     json.RawMessage `json:"input"`
	Content   json.RawMessage `json:"content"`
}

type streamEvent struct {
	Type         string         `json:"type"`
	Message      *claudeMessage `json:"message"`
	ContentBlock *contentBlock  `json:"content_block"`
	Delta        *streamDelta   `json:"delta"`
	Usage        *claudeUsage   `json:"usage"`
}

type streamDelta struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type claudeUsage struct {
	InputTokens              int64 `json:"input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	TotalTokens              int64 `json:"total_tokens"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
	CachedInputTokens        int64 `json:"cached_input_tokens"`
	ReasoningOutputTokens    int64 `json:"reasoning_output_tokens"`
}

type turnState struct {
	commandItems    map[string]bool
	sessionID       string
	model           string
	partialItemID   string
	usage           runner.AgentTokenUsage
	sawResult       bool
	resultSubtype   string
	resultText      string
	resultIsError   bool
	turnStartedSent bool
}

func scanClaudeStream(ctx context.Context, r io.Reader, maxTokenSize int) <-chan streamItem {
	out := make(chan streamItem, 64)
	go func() {
		defer close(out)

		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, 0, 64*1024), maxTokenSize)
		for scanner.Scan() {
			line := strings.TrimSpace(string(scanner.Bytes()))
			if line == "" {
				continue
			}
			var event claudeEvent
			if err := json.Unmarshal([]byte(line), &event); err != nil {
				continue
			}
			select {
			case out <- streamItem{event: event}:
			case <-ctx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case out <- streamItem{err: fmt.Errorf("scan claude stream: %w", err)}:
			case <-ctx.Done():
			}
		}
	}()
	return out
}

func (s *turnState) apply(event claudeEvent, includePartialMessages bool, onUpdate runner.AgentUpdateHandler) error {
	if event.SessionID != "" {
		s.sessionID = event.SessionID
	}
	previousModel := s.model
	s.observeModel(event)

	switch event.Type {
	case "system", "init":
		return s.applyInit(event, onUpdate)
	case "assistant":
		if err := s.emitModelChange(previousModel, onUpdate); err != nil {
			return err
		}
		return s.applyAssistant(event, includePartialMessages, onUpdate)
	case "user":
		return s.applyUser(event, onUpdate)
	case "stream_event":
		if err := s.emitModelChange(previousModel, onUpdate); err != nil {
			return err
		}
		return s.applyStreamEvent(event, includePartialMessages, onUpdate)
	case "rate_limit_event":
		return s.applyRateLimit(event, onUpdate)
	case "result":
		if err := s.emitModelChange(previousModel, onUpdate); err != nil {
			return err
		}
		return s.applyResult(event, onUpdate)
	default:
		return nil
	}
}

func (s *turnState) applyRateLimit(event claudeEvent, onUpdate runner.AgentUpdateHandler) error {
	if event.RateLimit == nil {
		return nil
	}
	info := event.RateLimit
	observedAt := time.Now().UTC()
	bucket := &telemetry.RateLimitBucket{Status: strings.TrimSpace(info.Status), ObservedAt: &observedAt}
	if strings.EqualFold(bucket.Status, "rejected") {
		bucket.Status = telemetry.RateLimitStatusExhausted
	}
	if info.ResetsAt > 0 {
		resetAt := time.Unix(info.ResetsAt, 0).UTC()
		bucket.ResetAt = &resetAt
	}
	if info.Utilization > 0 {
		bucket.Limit = 100
		bucket.Used = min(max(int64(info.Utilization*100), 0), 100)
		bucket.Remaining = 100 - bucket.Used
	}
	limitType := strings.TrimSpace(info.RateLimitType)
	limits := &telemetry.RateLimits{LimitID: "claude", LimitName: "Claude"}
	if strings.EqualFold(strings.TrimSpace(info.Status), "rejected") {
		limits.ReachedType = limitType
	}
	if limitType == "" || strings.EqualFold(limitType, "five_hour") {
		limits.Primary = bucket
	} else {
		limits.Secondary = bucket
	}
	return emitUpdate(onUpdate, runner.AgentUpdate{
		Type:       runner.AgentUpdateRateLimits,
		ThreadID:   s.sessionID,
		TurnID:     s.sessionID,
		Model:      s.model,
		RateLimits: limits,
	})
}

func (s *turnState) applyInit(event claudeEvent, onUpdate runner.AgentUpdateHandler) error {
	if event.Type == "system" && strings.TrimSpace(event.Subtype) != "init" {
		return nil
	}
	if event.SessionID == "" || s.turnStartedSent {
		return nil
	}
	s.turnStartedSent = true
	return emitUpdate(onUpdate, runner.AgentUpdate{
		Type:              runner.AgentUpdateTurnStarted,
		ThreadID:          event.SessionID,
		TurnID:            event.SessionID,
		ProviderSessionID: event.SessionID,
		Model:             s.model,
		RuntimeIdentity:   agentidentity.RuntimeUpdate(s.model, "", "", "", time.Time{}),
	})
}

func (s *turnState) emitModelChange(previousModel string, onUpdate runner.AgentUpdateHandler) error {
	model := strings.TrimSpace(s.model)
	if model == "" || model == strings.TrimSpace(previousModel) {
		return nil
	}
	return emitUpdate(onUpdate, runner.AgentUpdate{
		Type:            runner.AgentUpdateModelUpdated,
		ThreadID:        s.sessionID,
		TurnID:          s.sessionID,
		Model:           model,
		RuntimeIdentity: agentidentity.RuntimeUpdate(model, "", "", "", time.Time{}),
	})
}

func (s *turnState) applyAssistant(
	event claudeEvent,
	includePartialMessages bool,
	onUpdate runner.AgentUpdateHandler,
) error {
	if event.Message == nil {
		return nil
	}
	if event.Message.ID != "" {
		s.partialItemID = event.Message.ID
	}
	for _, block := range event.Message.Content {
		if includePartialMessages && (claudeBlockCommand(block) == "" || s.commandItems[strings.TrimSpace(block.ID)]) {
			continue
		}
		if err := s.emitContentBlock(block, event.Message.ID, onUpdate); err != nil {
			return err
		}
	}
	if event.Message.Usage != nil && !event.Message.Usage.empty() {
		addUsage(&s.usage, *event.Message.Usage)
		return s.emitUsage(onUpdate)
	}
	return nil
}

func (s *turnState) applyUser(event claudeEvent, onUpdate runner.AgentUpdateHandler) error {
	if event.Message == nil {
		return nil
	}
	for _, block := range event.Message.Content {
		if block.Type != "tool_result" {
			continue
		}
		if err := s.emitContentBlock(block, event.Message.ID, onUpdate); err != nil {
			return err
		}
	}
	return nil
}

func (s *turnState) applyStreamEvent(
	event claudeEvent,
	includePartialMessages bool,
	onUpdate runner.AgentUpdateHandler,
) error {
	if event.StreamEvent == nil {
		return nil
	}
	if event.StreamEvent.Message != nil && event.StreamEvent.Message.ID != "" {
		s.partialItemID = event.StreamEvent.Message.ID
	}
	if includePartialMessages && event.StreamEvent.Delta != nil &&
		event.StreamEvent.Delta.Type == "text_delta" &&
		event.StreamEvent.Delta.Text != "" {
		if err := emitUpdate(onUpdate, runner.AgentUpdate{
			Type:     runner.AgentUpdateMessageDelta,
			ThreadID: s.sessionID,
			TurnID:   s.sessionID,
			ItemID:   s.partialItemID,
			Delta:    event.StreamEvent.Delta.Text,
			Model:    s.model,
		}); err != nil {
			return err
		}
	}
	if event.StreamEvent.ContentBlock != nil && event.StreamEvent.ContentBlock.Type != "text" {
		if err := s.emitContentBlock(*event.StreamEvent.ContentBlock, s.partialItemID, onUpdate); err != nil {
			return err
		}
	}
	if event.StreamEvent.Usage != nil && !event.StreamEvent.Usage.empty() {
		addUsage(&s.usage, *event.StreamEvent.Usage)
		return s.emitUsage(onUpdate)
	}
	return nil
}

func (s *turnState) emitContentBlock(block contentBlock, fallbackItemID string, onUpdate runner.AgentUpdateHandler) error {
	itemID := strings.TrimSpace(block.ID)
	if itemID == "" {
		itemID = strings.TrimSpace(fallbackItemID)
	}
	switch block.Type {
	case "text":
		if block.Text == "" {
			return nil
		}
		return emitUpdate(onUpdate, runner.AgentUpdate{
			Type:     runner.AgentUpdateMessageDelta,
			ThreadID: s.sessionID,
			TurnID:   s.sessionID,
			ItemID:   itemID,
			Delta:    block.Text,
			Model:    s.model,
		})
	case "tool_use":
		command := claudeBlockCommand(block)
		if command != "" {
			if s.commandItems == nil {
				s.commandItems = make(map[string]bool)
			}
			s.commandItems[itemID] = true
		}
		return emitUpdate(onUpdate, runner.AgentUpdate{
			Command:  command,
			Type:     runner.AgentUpdateToolStarted,
			ThreadID: s.sessionID,
			TurnID:   s.sessionID,
			ItemID:   itemID,
			Tool:     strings.TrimSpace(block.Name),
			Delta:    claudeBlockContent(block.Input),
			Status:   "started",
			Model:    s.model,
		})
	case "tool_result":
		return emitUpdate(onUpdate, runner.AgentUpdate{
			Type:     runner.AgentUpdateToolOutput,
			ThreadID: s.sessionID,
			TurnID:   s.sessionID,
			ItemID:   firstNonBlankString(block.ToolUseID, itemID),
			Tool:     "tool_result",
			Delta:    claudeBlockContent(block.Content),
			Status:   "completed",
			Model:    s.model,
		})
	default:
		return nil
	}
}

func claudeBlockCommand(block contentBlock) string {
	if block.Type != "tool_use" || !strings.EqualFold(strings.TrimSpace(block.Name), "Bash") {
		return ""
	}
	var input struct {
		Command string `json:"command"`
	}
	if err := json.Unmarshal(block.Input, &input); err != nil {
		return ""
	}
	return strings.TrimSpace(input.Command)
}

func claudeBlockContent(raw json.RawMessage) string {
	raw = json.RawMessage(strings.TrimSpace(string(raw)))
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err == nil {
		return compact.String()
	}
	return string(raw)
}

func firstNonBlankString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func (s *turnState) applyResult(event claudeEvent, onUpdate runner.AgentUpdateHandler) error {
	s.sawResult = true
	s.resultSubtype = event.Subtype
	s.resultText = strings.TrimSpace(event.Result)
	s.resultIsError = event.IsError
	// Non-goals: --resume continuity and total_cost_usd budget ingest.
	if event.Usage != nil && !event.Usage.empty() {
		s.usage = event.Usage.agentUsage()
		return s.emitUsage(onUpdate)
	}
	if usage := event.topLevelUsage(); !usage.empty() {
		s.usage = usage.agentUsage()
		return s.emitUsage(onUpdate)
	}
	return nil
}

func (s *turnState) emitUsage(onUpdate runner.AgentUpdateHandler) error {
	return emitUpdate(onUpdate, runner.AgentUpdate{
		Type:     runner.AgentUpdateTokenUsage,
		ThreadID: s.sessionID,
		TurnID:   s.sessionID,
		Model:    s.model,
		Tokens:   s.usage,
	})
}

func (s *turnState) observeModel(event claudeEvent) {
	if model := event.modelName(); model != "" {
		s.model = model
	}
}

func (e claudeEvent) modelName() string {
	if model := strings.TrimSpace(e.Model); model != "" {
		return model
	}
	if e.Message != nil {
		if model := strings.TrimSpace(e.Message.Model); model != "" {
			return model
		}
	}
	if e.StreamEvent != nil && e.StreamEvent.Message != nil {
		if model := strings.TrimSpace(e.StreamEvent.Message.Model); model != "" {
			return model
		}
	}
	return ""
}

func (e claudeEvent) topLevelUsage() claudeUsage {
	return claudeUsage{
		InputTokens:  e.InputTokens,
		OutputTokens: e.OutputTokens,
		TotalTokens:  e.TotalTokens,
	}
}

func (u claudeUsage) empty() bool {
	return u.InputTokens == 0 &&
		u.OutputTokens == 0 &&
		u.TotalTokens == 0 &&
		u.CacheCreationInputTokens == 0 &&
		u.CacheReadInputTokens == 0 &&
		u.CachedInputTokens == 0 &&
		u.ReasoningOutputTokens == 0
}

func (u claudeUsage) agentUsage() runner.AgentTokenUsage {
	cached := u.CacheReadInputTokens + u.CachedInputTokens
	input := u.InputTokens + u.CacheCreationInputTokens + cached
	total := u.TotalTokens
	if minimumTotal := input + u.OutputTokens; total < minimumTotal {
		total = minimumTotal
	}
	return runner.AgentTokenUsage{
		InputTokens:           input,
		CachedInputTokens:     cached,
		OutputTokens:          u.OutputTokens,
		ReasoningOutputTokens: u.ReasoningOutputTokens,
		TotalTokens:           total,
	}
}

func addUsage(t *runner.AgentTokenUsage, u claudeUsage) {
	next := u.agentUsage()
	t.InputTokens += next.InputTokens
	t.CachedInputTokens += next.CachedInputTokens
	t.OutputTokens += next.OutputTokens
	t.ReasoningOutputTokens += next.ReasoningOutputTokens
	t.TotalTokens += next.TotalTokens
}
