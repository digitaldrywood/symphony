package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/backendcapacity"
)

func TestAppServerStartupFailuresIdentifyStageAndDeadline(t *testing.T) {
	t.Parallel()

	const deadline = 5 * time.Millisecond
	tests := []struct {
		name  string
		stage string
		run   func(*AppServer, Transport) error
	}{
		{name: "initialize", stage: "initialize", run: func(server *AppServer, transport Transport) error {
			return server.initialize(t.Context(), transport, nil)
		}},
		{name: "thread start", stage: "thread/start", run: func(server *AppServer, transport Transport) error {
			_, _, err := server.startThread(t.Context(), transport, RunTurnRequest{}, nil)
			return err
		}},
		{name: "thread resume", stage: "thread/resume", run: func(server *AppServer, transport Transport) error {
			_, _, err := server.resumeThread(t.Context(), transport, RunTurnRequest{}, "thread-1", nil)
			return err
		}},
		{name: "turn start", stage: "turn/start", run: func(server *AppServer, transport Transport) error {
			_, err := server.startTurn(t.Context(), transport, RunTurnRequest{}, "thread-1", nil, nil)
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			transport := newBlockingAppServerTransport(nil)
			server, err := NewAppServer(staticTransportFactory{transport: transport}, WithReadTimeout(deadline))
			if err != nil {
				t.Fatalf("NewAppServer() error = %v", err)
			}
			factory := newControlledTimeoutFactory()
			server.timeoutContext = factory.context
			now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
			server.now = func() time.Time {
				current := now
				now = now.Add(deadline)
				return current
			}
			err = runWithTimeoutExpiration(t, factory, deadline, nil, func() error {
				return tt.run(server, transport)
			})
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("startup error = %v, want deadline exceeded", err)
			}
			evidence, ok := startupEvidence(err)
			if !ok {
				t.Fatalf("startup evidence missing from %v", err)
			}
			if evidence.Stage != tt.stage || evidence.DeadlineMS != deadline.Milliseconds() || evidence.ElapsedMS < 1 {
				t.Fatalf("startup evidence = %#v, want stage %q and deadline %s", evidence, tt.stage, deadline)
			}
			details, ok := ClassifyCapacityError(err, nil, time.Now())
			if !ok || details.Kind != backendcapacity.StartupTimeoutKind || details.Startup == nil || details.Startup.Stage != tt.stage {
				t.Fatalf("capacity details = %#v, want exact startup stage", details)
			}
		})
	}
}

func TestAppServerUpdateHandlerFailureIsNotStartupFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		response Message
		run      func(*AppServer, Transport, UpdateHandler) error
	}{
		{
			name:     "thread start",
			response: responseMessage(t, threadStartRequestID, `{"thread":{"id":"thread-1"}}`),
			run: func(server *AppServer, transport Transport, onUpdate UpdateHandler) error {
				_, _, err := server.startThread(t.Context(), transport, RunTurnRequest{}, onUpdate)
				return err
			},
		},
		{
			name:     "thread resume",
			response: responseMessage(t, threadResumeRequestID, `{"thread":{"id":"thread-1"}}`),
			run: func(server *AppServer, transport Transport, onUpdate UpdateHandler) error {
				_, _, err := server.resumeThread(t.Context(), transport, RunTurnRequest{}, "thread-1", onUpdate)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			transport := newFakeAppServerTransport([]Message{tt.response})
			server, err := NewAppServer(staticTransportFactory{transport: transport})
			if err != nil {
				t.Fatalf("NewAppServer() error = %v", err)
			}
			err = tt.run(server, transport, func(Update) error { return context.DeadlineExceeded })
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("startup call error = %v, want update handler deadline", err)
			}
			if _, ok := startupEvidence(err); ok {
				t.Fatalf("startup evidence present on update handler error %v", err)
			}
			if details, ok := ClassifyCapacityError(err, nil, time.Now()); ok && details.Kind == backendcapacity.StartupTimeoutKind {
				t.Fatalf("update handler error classified as startup timeout: %#v", details)
			}
		})
	}
}

func TestAppServerStartupFailureRetainsProcessReadinessAndExit(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	transport := &startupEvidenceTransport{
		blockingAppServerTransport: newBlockingAppServerTransport([]Message{
			responseMessage(t, initializeRequestID, `{"userAgent":"codex-cli/test"}`),
		}),
		evidence: backendcapacity.StartupProcessEvidence{StartedAt: &startedAt},
	}
	server, err := NewAppServer(staticTransportFactory{transport: transport}, WithReadTimeout(5*time.Millisecond))
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}
	factory := newControlledTimeoutFactory()
	server.timeoutContext = factory.context
	now := startedAt
	server.now = func() time.Time {
		current := now
		now = now.Add(5 * time.Millisecond)
		return current
	}

	err = runWithTimeoutExpiration(t, factory, 5*time.Millisecond, nil, func() error {
		_, runErr := server.RunTurn(t.Context(), RunTurnRequest{}, nil)
		return runErr
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunTurn() error = %v, want deadline exceeded", err)
	}
	evidence, ok := startupEvidence(err)
	if !ok || evidence.Stage != "thread/start" {
		t.Fatalf("startup evidence = %#v, %v", evidence, ok)
	}
	if !evidence.Process.Ready || evidence.Process.ReadyAt == nil || !evidence.Process.ExitObserved || evidence.Process.ExitedAt == nil || evidence.Process.ExitStatus != "closed after startup failure" {
		t.Fatalf("process evidence = %#v, want ready and exit observations", evidence.Process)
	}
}

func TestAppServerRunTurnStartsLifecycleAndStreamsUpdates(t *testing.T) {
	t.Parallel()

	transport := newFakeAppServerTransport([]Message{
		responseMessage(t, 1, `{"userAgent":"codex-cli/0.135.0"}`),
		responseMessage(t, 2, `{"thread":{"id":"thread-1"},"model":"gpt-5-codex-resolved","modelProvider":"openai","reasoningEffort":"xhigh","serviceTier":"priority"}`),
		responseMessage(t, 3, `{"turn":{"id":"turn-1"}}`),
		notificationMessage(t, "item/agentMessage/delta", `{
			"threadId":"thread-1",
			"turnId":"turn-1",
			"itemId":"item-1",
			"delta":"hello"
		}`),
		notificationMessage(t, "thread/tokenUsage/updated", `{
			"threadId":"thread-1",
			"turnId":"turn-1",
			"tokenUsage":{
				"total":{
					"inputTokens":20,
					"cachedInputTokens":5,
					"outputTokens":7,
					"reasoningOutputTokens":3,
					"totalTokens":27
				},
				"last":{
					"inputTokens":2,
					"cachedInputTokens":1,
					"outputTokens":3,
					"reasoningOutputTokens":1,
					"totalTokens":5
				},
				"modelContextWindow":200000
			}
		}`),
		notificationMessage(t, "account/rateLimits/updated", `{
			"rateLimits":{
				"limitId":"codex-primary",
				"limitName":"Codex primary",
				"primary":{
					"usedPercent":12.5,
					"windowDurationMins":300,
					"resetsAt":1780000000
				},
				"secondary":null,
				"credits":{
					"hasCredits":true,
					"unlimited":false,
					"balance":"7.25"
				},
				"planType":null,
				"rateLimitReachedType":null
			}
		}`),
		notificationMessage(t, "turn/completed", `{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed"}}`),
	})
	transport.processIdentity = "4242"
	server, err := NewAppServer(staticTransportFactory{transport: transport},
		WithClientInfo(ClientInfo{
			Name:    "detent-test",
			Title:   "Detent Test",
			Version: "0.1.0",
		}),
		WithReadTimeout(time.Second),
		WithTurnTimeout(time.Second),
	)
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	var updates []Update
	result, err := server.RunTurn(context.Background(), RunTurnRequest{
		Workspace:         "/tmp/detent-workspace",
		Prompt:            "Ship issue #18",
		ApprovalPolicy:    json.RawMessage(`"never"`),
		ThreadSandbox:     "workspace-write",
		TurnSandboxPolicy: json.RawMessage(`{"type":"workspaceWrite","networkAccess":true}`),
		ModelProvider:     "local_ollama",
		ServiceTier:       "priority",
	}, func(update Update) error {
		updates = append(updates, update)
		return nil
	})
	if err != nil {
		t.Fatalf("RunTurn() error = %v", err)
	}

	if result.ThreadID != "thread-1" || result.TurnID != "turn-1" || result.SessionID != "thread-1-turn-1" {
		t.Fatalf("RunTurn() result = %#v", result)
	}

	sent := transport.sentMessages()
	if len(sent) != 4 {
		t.Fatalf("sent messages = %d, want 4", len(sent))
	}

	assertRequest(t, sent[0], 1, "initialize")
	assertJSONContains(t, sent[0].Params, "clientInfo.name", "detent-test")
	assertJSONContains(t, sent[0].Params, "capabilities.experimentalApi", true)

	if sent[1].Method != "initialized" || len(sent[1].ID) != 0 {
		t.Fatalf("sent[1] = %#v, want initialized notification", sent[1])
	}

	assertRequest(t, sent[2], 2, "thread/start")
	assertJSONContains(t, sent[2].Params, "cwd", "/tmp/detent-workspace")
	assertJSONContains(t, sent[2].Params, "approvalPolicy", "never")
	assertJSONContains(t, sent[2].Params, "sandbox", "workspace-write")
	assertJSONContains(t, sent[2].Params, "modelProvider", "local_ollama")
	assertJSONContains(t, sent[2].Params, "serviceTier", "priority")

	assertRequest(t, sent[3], 3, "turn/start")
	assertJSONContains(t, sent[3].Params, "threadId", "thread-1")
	assertJSONContains(t, sent[3].Params, "input.0.type", "text")
	assertJSONContains(t, sent[3].Params, "input.0.text", "Ship issue #18")
	assertJSONContains(t, sent[3].Params, "cwd", "/tmp/detent-workspace")
	assertJSONContains(t, sent[3].Params, "approvalPolicy", "never")
	assertJSONContains(t, sent[3].Params, "sandboxPolicy.type", "workspaceWrite")
	assertJSONContains(t, sent[3].Params, "serviceTier", "priority")

	if len(updates) != 7 {
		t.Fatalf("updates = %d, want 7: %#v", len(updates), updates)
	}
	if updates[0].Type != UpdateProcessStarted || updates[0].ProcessIdentity != "4242" {
		t.Fatalf("updates[0] = %#v, want process identity", updates[0])
	}
	if updates[1].Type != UpdateRuntimeIdentity || updates[1].RuntimeIdentity.Provider.Value != "openai" || updates[1].RuntimeIdentity.ReasoningEffort.Value != "xhigh" || updates[1].RuntimeIdentity.ServiceTier.Value != "priority" {
		t.Fatalf("updates[1] = %#v, want runtime identity", updates[1])
	}
	if updates[2].Type != UpdateTurnStarted || updates[2].ThreadID != "thread-1" || updates[2].TurnID != "turn-1" || updates[2].Model != "gpt-5-codex-resolved" {
		t.Fatalf("updates[2] = %#v, want turn started", updates[2])
	}
	if updates[3].Type != UpdateAgentMessageDelta || updates[3].Delta != "hello" {
		t.Fatalf("updates[3] = %#v, want agent message delta", updates[3])
	}
	if updates[4].Type != UpdateTokenUsage || updates[4].Tokens.TotalTokens != 27 {
		t.Fatalf("updates[4] = %#v, want cumulative token usage total 27", updates[4])
	}
	if updates[4].Tokens.CachedInputTokens != 5 || updates[4].Tokens.ReasoningOutputTokens != 3 {
		t.Fatalf("updates[4].Tokens = %#v", updates[4].Tokens)
	}
	if updates[4].Tokens.Last == nil || updates[4].Tokens.Last.TotalTokens != 5 || updates[4].Tokens.Last.CachedInputTokens != 1 {
		t.Fatalf("updates[4].Tokens.Last = %#v, want last-call token usage", updates[4].Tokens.Last)
	}
	if updates[4].Tokens.ModelContextWindow == nil || *updates[4].Tokens.ModelContextWindow != 200000 {
		t.Fatalf("updates[4].Tokens.ModelContextWindow = %#v", updates[4].Tokens.ModelContextWindow)
	}
	if updates[5].Type != UpdateRateLimits || updates[5].RateLimits == nil {
		t.Fatalf("updates[5] = %#v, want rate limits", updates[5])
	}
	if updates[5].RateLimits.LimitID != "codex-primary" || updates[5].RateLimits.Primary == nil {
		t.Fatalf("updates[5].RateLimits = %#v", updates[5].RateLimits)
	}
	if updates[5].RateLimits.Primary.UsedPercent != 12.5 {
		t.Fatalf("Primary.UsedPercent = %f, want 12.5", updates[5].RateLimits.Primary.UsedPercent)
	}
	if updates[6].Type != UpdateTurnCompleted || updates[6].TurnID != "turn-1" {
		t.Fatalf("updates[6] = %#v, want turn completed", updates[6])
	}
}

func TestUpdateFromMessagePreservesCumulativeAndLastTokenUsage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tokenUsage string
		want       TokenUsage
		wantLast   *TokenUsageBreakdown
	}{
		{
			name: "last turn available",
			tokenUsage: `{
				"total":{"inputTokens":20,"cachedInputTokens":5,"outputTokens":7,"reasoningOutputTokens":3,"totalTokens":27},
				"last":{"inputTokens":2,"cachedInputTokens":1,"outputTokens":3,"reasoningOutputTokens":1,"totalTokens":5},
				"modelContextWindow":200000
			}`,
			want: TokenUsage{
				InputTokens:           20,
				CachedInputTokens:     5,
				OutputTokens:          7,
				ReasoningOutputTokens: 3,
				TotalTokens:           27,
			},
			wantLast: &TokenUsageBreakdown{
				InputTokens:           2,
				CachedInputTokens:     1,
				OutputTokens:          3,
				ReasoningOutputTokens: 1,
				TotalTokens:           5,
			},
		},
		{
			name: "legacy total fallback",
			tokenUsage: `{
				"total":{"inputTokens":20,"cachedInputTokens":5,"outputTokens":7,"reasoningOutputTokens":3,"totalTokens":27},
				"modelContextWindow":200000
			}`,
			want: TokenUsage{
				InputTokens:           20,
				CachedInputTokens:     5,
				OutputTokens:          7,
				ReasoningOutputTokens: 3,
				TotalTokens:           27,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			message := notificationMessage(t, "thread/tokenUsage/updated", `{
				"threadId":"thread-resumed",
				"turnId":"turn-new",
				"tokenUsage":`+tt.tokenUsage+`
			}`)
			update, ok, err := updateFromMessage(message)
			if err != nil {
				t.Fatalf("updateFromMessage() error = %v", err)
			}
			if !ok {
				t.Fatal("updateFromMessage() ok = false, want true")
			}
			if update.Tokens.InputTokens != tt.want.InputTokens ||
				update.Tokens.CachedInputTokens != tt.want.CachedInputTokens ||
				update.Tokens.OutputTokens != tt.want.OutputTokens ||
				update.Tokens.ReasoningOutputTokens != tt.want.ReasoningOutputTokens ||
				update.Tokens.TotalTokens != tt.want.TotalTokens {
				t.Fatalf("Tokens = %#v, want %#v", update.Tokens, tt.want)
			}
			if !reflect.DeepEqual(update.Tokens.Last, tt.wantLast) {
				t.Fatalf("Tokens.Last = %#v, want %#v", update.Tokens.Last, tt.wantLast)
			}
			if update.Tokens.ModelContextWindow == nil || *update.Tokens.ModelContextWindow != 200000 {
				t.Fatalf("ModelContextWindow = %#v, want 200000", update.Tokens.ModelContextWindow)
			}
		})
	}
}

func TestAppServerCheckHealthCompletesInitializeHandshake(t *testing.T) {
	t.Parallel()

	transport := newFakeAppServerTransport([]Message{
		responseMessage(t, initializeRequestID, `{"userAgent":"codex-cli/test"}`),
	})
	server, err := NewAppServer(staticTransportFactory{transport: transport}, WithReadTimeout(time.Second))
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}
	if err := server.CheckHealth(context.Background()); err != nil {
		t.Fatalf("CheckHealth() error = %v", err)
	}

	sent := transport.sentMessages()
	if len(sent) != 2 || sent[0].Method != "initialize" || sent[1].Method != "initialized" {
		t.Fatalf("sent messages = %#v, want initialize handshake", sent)
	}
}

func TestAppServerAccountReadsResolvedAuthentication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		response         string
		want             Account
		wantSubscription bool
	}{
		{
			name:             "flat ChatGPT plan is subscription based",
			response:         `{"account":{"type":"chatgpt","email":"operator@example.com","planType":"pro"},"requiresOpenaiAuth":true}`,
			want:             Account{Type: "chatgpt", PlanType: "pro", RequiresOpenAIAuth: true},
			wantSubscription: true,
		},
		{
			name:     "API key is metered",
			response: `{"account":{"type":"apiKey"},"requiresOpenaiAuth":true}`,
			want:     Account{Type: "apiKey", RequiresOpenAIAuth: true},
		},
		{
			name:     "usage based ChatGPT plan is metered",
			response: `{"account":{"type":"chatgpt","email":"operator@example.com","planType":"self_serve_business_usage_based"},"requiresOpenaiAuth":true}`,
			want:     Account{Type: "chatgpt", PlanType: "self_serve_business_usage_based", RequiresOpenAIAuth: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transport := newFakeAppServerTransport([]Message{
				responseMessage(t, initializeRequestID, `{"userAgent":"codex-cli/test"}`),
				responseMessage(t, accountReadRequestID, tt.response),
			})
			server, err := NewAppServer(staticTransportFactory{transport: transport}, WithReadTimeout(time.Second))
			if err != nil {
				t.Fatalf("NewAppServer() error = %v", err)
			}
			account, err := server.Account(t.Context())
			if err != nil {
				t.Fatalf("Account() error = %v", err)
			}
			if account != tt.want || account.SubscriptionBased() != tt.wantSubscription {
				t.Fatalf("Account() = %#v, subscription=%t; want %#v, subscription=%t", account, account.SubscriptionBased(), tt.want, tt.wantSubscription)
			}
			sent := transport.sentMessages()
			if len(sent) != 3 || sent[2].Method != "account/read" {
				t.Fatalf("sent messages = %#v, want initialize, initialized, account/read", sent)
			}
		})
	}
}

func TestAppServerRunTurnRequiresSubscriptionAuthenticationInSameTransport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		account      string
		responses    []Message
		wantErr      error
		wantAuthMode string
		wantMethods  []string
	}{
		{
			name:    "ChatGPT subscription continues to review",
			account: `{"account":{"type":"chatgpt","planType":"pro"},"requiresOpenaiAuth":true}`,
			responses: []Message{
				responseMessage(t, threadStartRequestID, `{"thread":{"id":"thread-audit"}}`),
				responseMessage(t, turnStartRequestID, `{"turn":{"id":"turn-audit"}}`),
				notificationMessage(t, "turn/completed", `{"threadId":"thread-audit","turn":{"id":"turn-audit","status":"completed"}}`),
			},
			wantAuthMode: AuthenticationModeChatGPTSubscription,
			wantMethods:  []string{"initialize", "initialized", "account/read", "thread/start", "turn/start"},
		},
		{
			name:        "API key stops before thread start",
			account:     `{"account":{"type":"apiKey"},"requiresOpenaiAuth":true}`,
			wantErr:     ErrSubscriptionAuthRequired,
			wantMethods: []string{"initialize", "initialized", "account/read"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			messages := []Message{
				responseMessage(t, initializeRequestID, `{"userAgent":"codex-cli/test"}`),
				responseMessage(t, accountReadRequestID, tt.account),
			}
			messages = append(messages, tt.responses...)
			transport := newFakeAppServerTransport(messages)
			server, err := NewAppServer(staticTransportFactory{transport: transport}, WithReadTimeout(time.Second), WithTurnTimeout(time.Second))
			if err != nil {
				t.Fatalf("NewAppServer() error = %v", err)
			}
			result, err := server.RunTurn(t.Context(), RunTurnRequest{
				Workspace:               t.TempDir(),
				Prompt:                  "Audit the supplied diff.",
				Model:                   "gpt-test",
				RequireSubscriptionAuth: true,
			}, nil)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("RunTurn() error = %v, want %v", err, tt.wantErr)
			}
			if result.AuthenticationMode != tt.wantAuthMode {
				t.Fatalf("AuthenticationMode = %q, want %q", result.AuthenticationMode, tt.wantAuthMode)
			}
			sent := transport.sentMessages()
			methods := make([]string, 0, len(sent))
			for _, message := range sent {
				methods = append(methods, message.Method)
			}
			if !reflect.DeepEqual(methods, tt.wantMethods) {
				t.Fatalf("sent methods = %#v, want %#v", methods, tt.wantMethods)
			}
		})
	}
}

func TestAppServerRunTurnExecutesDynamicToolRequest(t *testing.T) {
	t.Parallel()
	transport := newFakeAppServerTransport([]Message{
		responseMessage(t, 1, `{"userAgent":"codex-cli/0.144.5"}`),
		responseMessage(t, 2, `{"thread":{"id":"thread-chat"}}`),
		responseMessage(t, 3, `{"turn":{"id":"turn-chat"}}`),
		serverRequestMessage(t, 99, "item/tool/call", `{"threadId":"thread-chat","turnId":"turn-chat","callId":"call-1","tool":"board_state","arguments":{"state":"Blocked"}}`),
		notificationMessage(t, "item/agentMessage/delta", `{"threadId":"thread-chat","turnId":"turn-chat","itemId":"item-1","delta":"Two items are blocked."}`),
		notificationMessage(t, "turn/completed", `{"threadId":"thread-chat","turn":{"id":"turn-chat","status":"completed"}}`),
	})
	server, err := NewAppServer(staticTransportFactory{transport: transport}, WithReadTimeout(time.Second), WithTurnTimeout(time.Second))
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	var call DynamicToolCall
	_, err = server.RunTurn(context.Background(), RunTurnRequest{
		Workspace:             "/tmp/detent-chat",
		Prompt:                "What is blocked?",
		Model:                 "gpt-test",
		DeveloperInstructions: "Use Detent tools only.",
		DynamicTools: []DynamicTool{{
			Type:        "function",
			Name:        "board_state",
			Description: "Read the board.",
			InputSchema: json.RawMessage(`{"type":"object"}`),
		}},
		ToolHandler: func(_ context.Context, candidate DynamicToolCall) (DynamicToolResult, error) {
			call = candidate
			return DynamicToolResult{Content: `{"blocked":2}`, Success: true}, nil
		},
	}, nil)
	if err != nil {
		t.Fatalf("RunTurn() error = %v", err)
	}
	if call.Name != "board_state" || string(call.Arguments) != `{"state":"Blocked"}` {
		t.Fatalf("tool call = %#v", call)
	}

	sent := transport.sentMessages()
	if len(sent) != 5 {
		t.Fatalf("sent messages = %d, want 5", len(sent))
	}
	assertJSONContains(t, sent[2].Params, "developerInstructions", "Use Detent tools only.")
	assertJSONContains(t, sent[2].Params, "dynamicTools.0.name", "board_state")
	assertResponseResultContains(t, sent[4], 99, "success", true)
	assertResponseResultContains(t, sent[4], 99, "contentItems.0.text", `{"blocked":2}`)
}

func TestAppServerRunTurnRepliesToMalformedDynamicToolRequest(t *testing.T) {
	t.Parallel()
	transport := newFakeAppServerTransport([]Message{
		responseMessage(t, 1, `{"userAgent":"codex-cli/0.144.5"}`),
		responseMessage(t, 2, `{"thread":{"id":"thread-chat"}}`),
		responseMessage(t, 3, `{"turn":{"id":"turn-chat"}}`),
		serverRequestMessage(t, 99, "item/tool/call", `{"threadId":"thread-chat","turnId":"turn-chat","callId":"call-1","tool":42,"arguments":{}}`),
		notificationMessage(t, "turn/completed", `{"threadId":"thread-chat","turn":{"id":"turn-chat","status":"completed"}}`),
	})
	server, err := NewAppServer(staticTransportFactory{transport: transport}, WithReadTimeout(time.Second), WithTurnTimeout(time.Second))
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	_, err = server.RunTurn(context.Background(), RunTurnRequest{
		Workspace: "/tmp/detent-chat",
		Prompt:    "What is blocked?",
		Model:     "gpt-test",
		DynamicTools: []DynamicTool{{
			Type:        "function",
			Name:        "board_state",
			Description: "Read the board.",
			InputSchema: json.RawMessage(`{"type":"object"}`),
		}},
		ToolHandler: func(context.Context, DynamicToolCall) (DynamicToolResult, error) {
			t.Fatal("ToolHandler() called for malformed request")
			return DynamicToolResult{}, nil
		},
	}, nil)
	if err != nil {
		t.Fatalf("RunTurn() error = %v", err)
	}

	sent := transport.sentMessages()
	if len(sent) != 5 {
		t.Fatalf("sent messages = %d, want 5", len(sent))
	}
	assertResponseResultContains(t, sent[4], 99, "success", false)
	var response struct {
		ContentItems []struct {
			Text string `json:"text"`
		} `json:"contentItems"`
	}
	if err := json.Unmarshal(sent[4].Result, &response); err != nil {
		t.Fatalf("unmarshal malformed tool response: %v", err)
	}
	if len(response.ContentItems) != 1 || !strings.Contains(response.ContentItems[0].Text, "decode dynamic tool call") {
		t.Fatalf("malformed tool response = %#v", response)
	}
}

func TestAppServerListModelsPaginatesCatalog(t *testing.T) {
	t.Parallel()

	transport := newFakeAppServerTransport([]Message{
		responseMessage(t, 1, `{"userAgent":"codex-cli/0.135.0"}`),
		responseMessage(t, 6, `{"data":[{"id":"gpt-default","model":"gpt-default","isDefault":true,"supportedReasoningEfforts":[{"reasoningEffort":"low"},{"reasoningEffort":"high"}]}],"nextCursor":"page-2"}`),
		responseMessage(t, 6, `{"data":[{"id":"gpt-5.5","model":"gpt-5.5","isDefault":false,"upgrade":"gpt-5.6","supportedReasoningEfforts":[{"reasoningEffort":"medium"}]}],"nextCursor":null}`),
	})
	server, err := NewAppServer(staticTransportFactory{transport: transport}, WithReadTimeout(time.Second))
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	models, err := server.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels() error = %v", err)
	}
	if len(models) != 2 {
		t.Fatalf("models = %#v, want two entries", models)
	}
	if models[0].ID != "gpt-default" || !models[0].Default || !reflect.DeepEqual(models[0].SupportedReasoningEfforts, []string{"low", "high"}) {
		t.Fatalf("models[0] = %#v", models[0])
	}
	if models[1].Model != "gpt-5.5" || models[1].Upgrade != "gpt-5.6" || !reflect.DeepEqual(models[1].SupportedReasoningEfforts, []string{"medium"}) {
		t.Fatalf("models[1] = %#v", models[1])
	}

	sent := transport.sentMessages()
	if len(sent) != 4 {
		t.Fatalf("sent messages = %d, want 4", len(sent))
	}
	assertRequest(t, sent[2], 6, "model/list")
	assertJSONContains(t, sent[2].Params, "includeHidden", true)
	assertJSONContains(t, sent[2].Params, "limit", float64(100))
	assertRequest(t, sent[3], 6, "model/list")
	assertJSONContains(t, sent[3].Params, "cursor", "page-2")
}

func TestAppServerDefaultModelReadsWorkspaceConfig(t *testing.T) {
	t.Parallel()

	transport := newFakeAppServerTransport([]Message{
		responseMessage(t, 1, `{"userAgent":"codex-cli/0.144.0"}`),
		responseMessage(t, 5, `{"config":{"model":"gpt-5.5"}}`),
	})
	server, err := NewAppServer(staticTransportFactory{transport: transport}, WithReadTimeout(time.Second))
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	model, err := server.DefaultModel(context.Background(), "/tmp/detent-workspace")
	if err != nil {
		t.Fatalf("DefaultModel() error = %v", err)
	}
	if model != "gpt-5.5" {
		t.Fatalf("DefaultModel() = %q, want gpt-5.5", model)
	}

	sent := transport.sentMessages()
	if len(sent) != 3 {
		t.Fatalf("sent messages = %d, want 3", len(sent))
	}
	assertRequest(t, sent[2], 5, "config/read")
	assertJSONContains(t, sent[2].Params, "cwd", "/tmp/detent-workspace")
}

func TestAppServerRunTurnResumesThreadBeforeStartingTurn(t *testing.T) {
	t.Parallel()

	transport := newFakeAppServerTransport([]Message{
		responseMessage(t, 1, `{"userAgent":"codex-cli/0.142.5"}`),
		responseMessage(t, 4, `{"thread":{"id":"thread-existing","model":"gpt-5-codex-resumed"}}`),
		responseMessage(t, 3, `{"turn":{"id":"turn-2"}}`),
		notificationMessage(t, "turn/completed", `{"threadId":"thread-existing","turn":{"id":"turn-2","status":"completed"}}`),
	})
	server, err := NewAppServer(staticTransportFactory{transport: transport},
		WithReadTimeout(time.Second),
		WithTurnTimeout(time.Second),
	)
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	var updates []Update
	result, err := server.RunTurn(context.Background(), RunTurnRequest{
		Workspace:             "/tmp/detent-workspace",
		Prompt:                "Continue issue #18",
		ResumeThreadID:        "thread-existing",
		DeveloperInstructions: "Use only Detent tools.",
		Model:                 "gpt-5-codex",
	}, func(update Update) error {
		updates = append(updates, update)
		return nil
	})
	if err != nil {
		t.Fatalf("RunTurn() error = %v", err)
	}
	if result.ThreadID != "thread-existing" || result.TurnID != "turn-2" || result.SessionID != "thread-existing-turn-2" {
		t.Fatalf("RunTurn() result = %#v, want resumed thread and new turn", result)
	}

	sent := transport.sentMessages()
	if len(sent) != 4 {
		t.Fatalf("sent messages = %d, want 4", len(sent))
	}
	assertRequest(t, sent[0], 1, "initialize")
	if sent[1].Method != "initialized" || len(sent[1].ID) != 0 {
		t.Fatalf("sent[1] = %#v, want initialized notification", sent[1])
	}
	assertRequest(t, sent[2], 4, "thread/resume")
	assertJSONContains(t, sent[2].Params, "threadId", "thread-existing")
	assertJSONContains(t, sent[2].Params, "cwd", "/tmp/detent-workspace")
	assertJSONContains(t, sent[2].Params, "developerInstructions", "Use only Detent tools.")
	assertJSONContains(t, sent[2].Params, "model", "gpt-5-codex")
	assertRequest(t, sent[3], 3, "turn/start")
	assertJSONContains(t, sent[3].Params, "threadId", "thread-existing")
	assertJSONContains(t, sent[3].Params, "input.0.text", "Continue issue #18")

	if len(updates) != 3 {
		t.Fatalf("updates = %d, want identity, turn started, and completed: %#v", len(updates), updates)
	}
	if updates[0].Type != UpdateRuntimeIdentity || updates[0].Model != "gpt-5-codex-resumed" {
		t.Fatalf("updates[0] = %#v, want resumed runtime identity", updates[0])
	}
	if updates[1].Type != UpdateTurnStarted || updates[1].ThreadID != "thread-existing" || updates[1].TurnID != "turn-2" || updates[1].Model != "gpt-5-codex-resumed" {
		t.Fatalf("updates[1] = %#v, want resumed turn started", updates[1])
	}
}

func TestAppServerRunTurnPublishesFreshThreadIdentityBeforeTurnStart(t *testing.T) {
	t.Parallel()

	transport := newFakeAppServerTransport([]Message{
		responseMessage(t, 1, `{"userAgent":"codex-cli/0.144.1"}`),
		responseMessage(t, 2, `{"thread":{"id":"thread-fresh"}}`),
		responseMessage(t, 3, `{"turn":{"id":"turn-1"}}`),
		notificationMessage(t, "turn/completed", `{"threadId":"thread-fresh","turn":{"id":"turn-1","status":"completed"}}`),
	})
	server, err := NewAppServer(staticTransportFactory{transport: transport}, WithReadTimeout(time.Second), WithTurnTimeout(time.Second))
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	var updates []Update
	_, err = server.RunTurn(context.Background(), RunTurnRequest{
		Workspace: "/tmp/detent-workspace",
		Prompt:    "Implement issue #1155",
		Model:     "gpt-5.6-codex",
	}, func(update Update) error {
		updates = append(updates, update)
		return nil
	})
	if err != nil {
		t.Fatalf("RunTurn() error = %v", err)
	}
	if len(updates) < 2 || updates[0].Type != UpdateRuntimeIdentity || updates[0].ThreadID != "thread-fresh" || updates[0].Model != "gpt-5.6-codex" {
		t.Fatalf("updates = %#v, want provider thread identity before turn start", updates)
	}
	if updates[1].Type != UpdateTurnStarted {
		t.Fatalf("updates[1] = %#v, want turn started after identity", updates[1])
	}
}

func TestAppServerVerifyThreadReadsPersistedTurns(t *testing.T) {
	t.Parallel()

	transport := newFakeAppServerTransport([]Message{
		responseMessage(t, 1, `{"userAgent":"codex-cli/0.144.1"}`),
		responseMessage(t, 7, `{"thread":{"id":"thread-existing","turns":[{"id":"turn-1"}]}}`),
	})
	server, err := NewAppServer(staticTransportFactory{transport: transport}, WithReadTimeout(time.Second))
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	if err := server.VerifyThread(context.Background(), "thread-existing"); err != nil {
		t.Fatalf("VerifyThread() error = %v", err)
	}
	sent := transport.sentMessages()
	if len(sent) != 3 {
		t.Fatalf("sent messages = %d, want initialize, initialized, and thread/read", len(sent))
	}
	assertRequest(t, sent[2], 7, "thread/read")
	assertJSONContains(t, sent[2].Params, "threadId", "thread-existing")
	assertJSONContains(t, sent[2].Params, "includeTurns", true)
}

func TestAppServerVerifyThreadReturnsMissingRolloutError(t *testing.T) {
	t.Parallel()

	transport := newFakeAppServerTransport([]Message{
		responseMessage(t, 1, `{"userAgent":"codex-cli/0.144.1"}`),
		errorResponseMessage(t, 7, -32602, "rollout file not found"),
	})
	server, err := NewAppServer(staticTransportFactory{transport: transport}, WithReadTimeout(time.Second))
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	err = server.VerifyThread(context.Background(), "thread-missing")
	if err == nil {
		t.Fatal("VerifyThread() error = nil, want missing rollout response error")
	}
	if !errors.Is(err, ErrResponseError) || !strings.Contains(err.Error(), "rollout file not found") {
		t.Fatalf("VerifyThread() error = %v, want missing rollout response error", err)
	}
}

func TestAppServerRunTurnUsesConfigReadModelWhenThreadResponseOmitsModel(t *testing.T) {
	t.Parallel()

	transport := newFakeAppServerTransport([]Message{
		responseMessage(t, 1, `{"userAgent":"codex-cli/0.143.0"}`),
		responseMessage(t, 2, `{"thread":{"id":"thread-1"}}`),
		responseMessage(t, 5, `{"config":{"model":"gpt-5.6"}}`),
		responseMessage(t, 3, `{"turn":{"id":"turn-1"}}`),
		notificationMessage(t, "turn/completed", `{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed"}}`),
	})
	server, err := NewAppServer(staticTransportFactory{transport: transport},
		WithReadTimeout(time.Second),
		WithTurnTimeout(time.Second),
	)
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	var updates []Update
	_, err = server.RunTurn(context.Background(), RunTurnRequest{
		Workspace: "/tmp/detent-workspace",
		Prompt:    "Ship issue #1103",
	}, func(update Update) error {
		updates = append(updates, update)
		return nil
	})
	if err != nil {
		t.Fatalf("RunTurn() error = %v", err)
	}

	sent := transport.sentMessages()
	if len(sent) != 5 {
		t.Fatalf("sent messages = %d, want 5", len(sent))
	}
	assertRequest(t, sent[2], 2, "thread/start")
	assertRequest(t, sent[3], 5, "config/read")
	assertJSONContains(t, sent[3].Params, "cwd", "/tmp/detent-workspace")
	assertRequest(t, sent[4], 3, "turn/start")

	if len(updates) != 4 {
		t.Fatalf("updates = %d, want provider identity, configured identity, turn started, and completed: %#v", len(updates), updates)
	}
	if updates[0].Type != UpdateProviderIdentity || updates[0].ThreadID != "thread-1" {
		t.Fatalf("updates[0] = %#v, want provider identity", updates[0])
	}
	if updates[1].Type != UpdateRuntimeIdentity || updates[1].Model != "gpt-5.6" || updates[1].RuntimeIdentity.ResolvedModel.Provenance != "configured" {
		t.Fatalf("updates[1] = %#v, want configured fallback identity", updates[1])
	}
	if updates[2].Type != UpdateTurnStarted || updates[2].Model != "gpt-5.6" {
		t.Fatalf("updates[2] = %#v, want config-read fallback model", updates[2])
	}
}

func TestAppServerRunTurnContinuesWhenConfigReadFails(t *testing.T) {
	t.Parallel()

	transport := newFakeAppServerTransport([]Message{
		responseMessage(t, 1, `{"userAgent":"codex-cli/0.143.0"}`),
		responseMessage(t, 2, `{"thread":{"id":"thread-1"}}`),
		errorResponseMessage(t, 5, -32602, "invalid config/read params"),
		responseMessage(t, 3, `{"turn":{"id":"turn-1"}}`),
		notificationMessage(t, "turn/completed", `{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed"}}`),
	})
	server, err := NewAppServer(staticTransportFactory{transport: transport},
		WithReadTimeout(time.Second),
		WithTurnTimeout(time.Second),
	)
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	var updates []Update
	_, err = server.RunTurn(context.Background(), RunTurnRequest{
		Workspace: "/tmp/detent-workspace",
		Prompt:    "Ship issue #1103",
	}, func(update Update) error {
		updates = append(updates, update)
		return nil
	})
	if err != nil {
		t.Fatalf("RunTurn() error = %v, want config/read failure to be non-fatal", err)
	}

	if len(updates) != 3 {
		t.Fatalf("updates = %d, want provider identity, turn started, and completed: %#v", len(updates), updates)
	}
	if updates[0].Type != UpdateProviderIdentity || updates[0].ThreadID != "thread-1" {
		t.Fatalf("updates[0] = %#v, want provider identity", updates[0])
	}
	if updates[1].Type != UpdateTurnStarted || updates[1].Model != "" {
		t.Fatalf("updates[1] = %#v, want unpriced fallback turn started", updates[1])
	}
}

func TestAppServerRunTurnPreservesTurnStartedRuntimeModel(t *testing.T) {
	t.Parallel()

	transport := newFakeAppServerTransport([]Message{
		responseMessage(t, 1, `{"userAgent":"codex-cli/0.143.0"}`),
		responseMessage(t, 2, `{"thread":{"id":"thread-1","model":"gpt-5.5"}}`),
		notificationMessage(t, "turn/started", `{"threadId":"thread-1","turn":{"id":"turn-1","model":"gpt-5.6"}}`),
		responseMessage(t, 3, `{"turn":{"id":"turn-1"}}`),
		notificationMessage(t, "turn/completed", `{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed"}}`),
	})
	server, err := NewAppServer(staticTransportFactory{transport: transport},
		WithReadTimeout(time.Second),
		WithTurnTimeout(time.Second),
	)
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	var updates []Update
	_, err = server.RunTurn(context.Background(), RunTurnRequest{
		Workspace: "/tmp/detent-workspace",
		Prompt:    "Ship issue #1103",
	}, func(update Update) error {
		updates = append(updates, update)
		return nil
	})
	if err != nil {
		t.Fatalf("RunTurn() error = %v", err)
	}

	var started []Update
	for _, update := range updates {
		if update.Type == UpdateTurnStarted {
			started = append(started, update)
		}
	}
	if len(started) != 1 {
		t.Fatalf("started updates = %d, want only app-server turn/started event: %#v", len(started), started)
	}
	if started[0].Model != "gpt-5.6" {
		t.Fatalf("started[0].Model = %q, want runtime model", started[0].Model)
	}
}

func TestAppServerRunTurnPreservesLatestSettingsIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		effort        string
		settings      string
		settingsModel string
		started       bool
		threadEffort  string
		wantEffort    agentidentity.Value
		wantProvider  string
		wantTier      agentidentity.Value
	}{
		{name: "low override", effort: "low", settings: `"effort":"low","serviceTier":"priority"`, threadEffort: "medium", wantEffort: agentidentity.NewValue("low", agentidentity.ProvenanceRuntime), wantProvider: "azure", wantTier: agentidentity.NewValue("priority", agentidentity.ProvenanceRuntime)},
		{name: "high override", effort: "high", settings: `"effort":"high","serviceTier":"priority"`, threadEffort: "medium", wantEffort: agentidentity.NewValue("high", agentidentity.ProvenanceRuntime), wantProvider: "azure", wantTier: agentidentity.NewValue("priority", agentidentity.ProvenanceRuntime)},
		{name: "xhigh override", effort: "xhigh", settings: `"effort":"xhigh","serviceTier":"priority"`, threadEffort: "medium", wantEffort: agentidentity.NewValue("xhigh", agentidentity.ProvenanceRuntime), wantProvider: "azure", wantTier: agentidentity.NewValue("priority", agentidentity.ProvenanceRuntime)},
		{name: "real turn started", effort: "xhigh", settings: `"effort":"xhigh","serviceTier":"priority"`, started: true, threadEffort: "medium", wantEffort: agentidentity.NewValue("xhigh", agentidentity.ProvenanceRuntime), wantProvider: "azure", wantTier: agentidentity.NewValue("priority", agentidentity.ProvenanceRuntime)},
		{name: "settings unavailable", effort: "xhigh", settings: `"effort":null,"serviceTier":null`, threadEffort: "medium", wantEffort: agentidentity.UnknownValue(), wantProvider: "azure", wantTier: agentidentity.UnknownValue()},
		{name: "no runtime effort evidence", effort: "xhigh", wantProvider: "openai", wantTier: agentidentity.NewValue("default", agentidentity.ProvenanceRuntime)},
		{name: "thread evidence only", effort: "medium", threadEffort: "medium", wantEffort: agentidentity.NewValue("medium", agentidentity.ProvenanceRuntime), wantProvider: "openai", wantTier: agentidentity.NewValue("default", agentidentity.ProvenanceRuntime)},
		{name: "runtime model update", effort: "high", settings: `"effort":"high","serviceTier":"priority"`, settingsModel: "gpt-5.6-sol", threadEffort: "medium", wantEffort: agentidentity.NewValue("high", agentidentity.ProvenanceRuntime), wantProvider: "azure", wantTier: agentidentity.NewValue("priority", agentidentity.ProvenanceRuntime)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			messages := []Message{
				responseMessage(t, 1, `{"userAgent":"codex-cli/0.143.0"}`),
				responseMessage(t, threadResumeRequestID, `{"thread":{"id":"thread-1"},"model":"gpt-6-astra","modelProvider":"openai","reasoningEffort":"`+tt.threadEffort+`","serviceTier":"default"}`),
			}
			wantModel := "gpt-6-astra"
			if tt.settingsModel != "" {
				wantModel = tt.settingsModel
			}
			if tt.settings != "" {
				messages = append(messages, notificationMessage(t, "thread/settings/updated", `{"threadId":"thread-1","threadSettings":{"model":"`+wantModel+`","modelProvider":"azure",`+tt.settings+`}}`))
			}
			if tt.started {
				messages = append(messages, notificationMessage(t, "turn/started", `{"threadId":"thread-1","turn":{"id":"turn-1","model":"gpt-6-astra"}}`))
			}
			messages = append(messages,
				responseMessage(t, 3, `{"turn":{"id":"turn-1"}}`),
				notificationMessage(t, "turn/completed", `{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed"}}`),
			)
			transport := newFakeAppServerTransport(messages)
			server, err := NewAppServer(staticTransportFactory{transport: transport})
			if err != nil {
				t.Fatal(err)
			}
			var identity agentidentity.Identity
			var started []Update
			_, err = server.RunTurn(t.Context(), RunTurnRequest{
				Workspace: t.TempDir(), ResumeThreadID: "thread-1", Model: "gpt-6-astra", ReasoningEffort: tt.effort,
			}, func(update Update) error {
				identity = identity.Merge(update.RuntimeIdentity)
				if update.Type == UpdateTurnStarted {
					started = append(started, update)
				}
				return nil
			})
			if err != nil {
				t.Fatalf("RunTurn() error = %v", err)
			}
			if len(started) != 1 {
				t.Fatalf("turn started updates = %d, want 1", len(started))
			}
			wantMethod := "turn/start"
			if tt.started {
				wantMethod = "turn/started"
			}
			if started[0].Method != wantMethod {
				t.Fatalf("turn started method = %q, want %q", started[0].Method, wantMethod)
			}
			if identity.Model() != wantModel || started[0].Model != wantModel {
				t.Fatalf("final model = %q, turn started model = %q, want %q", identity.Model(), started[0].Model, wantModel)
			}
			if identity.ReasoningEffort != tt.wantEffort || identity.Provider != agentidentity.NewValue(tt.wantProvider, agentidentity.ProvenanceRuntime) || identity.ServiceTier != tt.wantTier {
				t.Fatalf("final identity = %+v, want effort=%+v provider=%s tier=%+v", identity, tt.wantEffort, tt.wantProvider, tt.wantTier)
			}
			for _, message := range transport.sent {
				if message.Method != "turn/start" {
					continue
				}
				var params struct {
					Effort string `json:"effort"`
				}
				if err := json.Unmarshal(message.Params, &params); err != nil {
					t.Fatal(err)
				}
				if params.Effort != tt.effort {
					t.Fatalf("requested effort = %q, want %q", params.Effort, tt.effort)
				}
			}
		})
	}
}

func TestUpdateFromMessageCapturesModelReroute(t *testing.T) {
	t.Parallel()

	update, ok, err := updateFromMessage(notificationMessage(t, "model/rerouted", `{
		"threadId":"thread-1",
		"turnId":"turn-1",
		"fromModel":"gpt-5.5",
		"toModel":"gpt-5.6",
		"reason":"highRiskCyberActivity"
	}`))
	if err != nil {
		t.Fatalf("updateFromMessage() error = %v", err)
	}
	if !ok {
		t.Fatal("updateFromMessage() ok = false, want true")
	}
	if update.Type != UpdateModelUpdated || update.ThreadID != "thread-1" || update.TurnID != "turn-1" || update.Model != "gpt-5.6" {
		t.Fatalf("update = %#v, want model reroute update", update)
	}
	if got := update.RuntimeIdentity.ResolvedModel; got != (agentidentity.Value{Value: "gpt-5.6", Provenance: agentidentity.ProvenanceRuntime}) {
		t.Fatalf("runtime resolved model = %#v, want gpt-5.6 runtime", got)
	}
}

func TestUpdateFromMessageCapturesThreadSettingsIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		payload      string
		wantModel    agentidentity.Value
		wantProvider agentidentity.Value
		wantEffort   agentidentity.Value
		wantTier     agentidentity.Value
	}{
		{
			name:         "known runtime settings",
			payload:      `{"threadId":"thread-1","threadSettings":{"model":"gpt-5.6-sol","modelProvider":"local_ollama","effort":"xhigh","serviceTier":"priority"}}`,
			wantModel:    agentidentity.Value{Value: "gpt-5.6-sol", Provenance: agentidentity.ProvenanceRuntime},
			wantProvider: agentidentity.Value{Value: "local_ollama", Provenance: agentidentity.ProvenanceRuntime},
			wantEffort:   agentidentity.Value{Value: "xhigh", Provenance: agentidentity.ProvenanceRuntime},
			wantTier:     agentidentity.Value{Value: "priority", Provenance: agentidentity.ProvenanceRuntime},
		},
		{
			name:         "optional settings unavailable",
			payload:      `{"threadId":"thread-1","threadSettings":{"model":"gpt-5.6-sol","modelProvider":"openai"}}`,
			wantModel:    agentidentity.Value{Value: "gpt-5.6-sol", Provenance: agentidentity.ProvenanceRuntime},
			wantProvider: agentidentity.Value{Value: "openai", Provenance: agentidentity.ProvenanceRuntime},
			wantEffort:   agentidentity.UnknownValue(),
			wantTier:     agentidentity.UnknownValue(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			update, ok, err := updateFromMessage(notificationMessage(t, "thread/settings/updated", tt.payload))
			if err != nil {
				t.Fatalf("updateFromMessage() error = %v", err)
			}
			if !ok || update.Type != UpdateRuntimeIdentity || update.ThreadID != "thread-1" {
				t.Fatalf("update = %#v, want runtime identity update", update)
			}
			identity := update.RuntimeIdentity
			if identity.ResolvedModel != tt.wantModel || identity.Provider != tt.wantProvider || identity.ReasoningEffort != tt.wantEffort || identity.ServiceTier != tt.wantTier {
				t.Fatalf("runtime identity = %#v, want model=%#v provider=%#v effort=%#v tier=%#v", identity, tt.wantModel, tt.wantProvider, tt.wantEffort, tt.wantTier)
			}
		})
	}
}

func TestAppServerRunTurnRequestTurnTimeoutOverridesDefault(t *testing.T) {
	t.Parallel()

	transport := newBlockingAppServerTransport([]Message{
		responseMessage(t, 1, `{"userAgent":"codex-cli/0.135.0"}`),
		responseMessage(t, 2, `{"thread":{"id":"thread-timeout"}}`),
		responseMessage(t, 5, `{"config":{"model":"gpt-5.6"}}`),
		responseMessage(t, 3, `{"turn":{"id":"turn-timeout"}}`),
	})
	server, err := NewAppServer(staticTransportFactory{transport: transport},
		WithReadTimeout(time.Second),
		WithTurnTimeout(time.Hour),
	)
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	factory := newControlledTimeoutFactory()
	server.timeoutContext = factory.context
	err = runWithTimeoutExpiration(t, factory, 10*time.Millisecond, nil, func() error {
		_, runErr := server.RunTurn(context.Background(), RunTurnRequest{
			Workspace:   "/tmp/detent-workspace",
			Prompt:      "timeout",
			TurnTimeout: 10 * time.Millisecond,
		}, nil)
		return runErr
	})
	if err == nil {
		t.Fatal("RunTurn() error = nil, want request timeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunTurn() error = %v, want context deadline exceeded", err)
	}
	if errors.Is(err, ErrStreamStalled) {
		t.Fatalf("RunTurn() error = %v, want disabled stall timeout", err)
	}
}

func TestAppServerRunTurnEnforcesStallTimeout(t *testing.T) {
	t.Parallel()

	transport := newBlockingAppServerTransport([]Message{
		responseMessage(t, 1, `{"userAgent":"codex-cli/0.135.0"}`),
		responseMessage(t, 2, `{"thread":{"id":"thread-stall"}}`),
		responseMessage(t, 5, `{"config":{"model":"gpt-5.6"}}`),
		responseMessage(t, 3, `{"turn":{"id":"turn-stall"}}`),
	})
	server, err := NewAppServer(staticTransportFactory{transport: transport},
		WithReadTimeout(time.Second),
		WithTurnTimeout(time.Hour),
	)
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	factory := newControlledTimeoutFactory()
	server.timeoutContext = factory.context
	err = runWithTimeoutExpiration(t, factory, 10*time.Millisecond, ErrStreamStalled, func() error {
		_, runErr := server.RunTurn(context.Background(), RunTurnRequest{
			Workspace:    "/tmp/detent-workspace",
			Prompt:       "stall",
			StallTimeout: 10 * time.Millisecond,
		}, nil)
		return runErr
	})
	if !errors.Is(err, ErrStreamStalled) {
		t.Fatalf("RunTurn() error = %v, want ErrStreamStalled", err)
	}
	if !strings.Contains(err.Error(), "after 10ms") {
		t.Fatalf("RunTurn() error = %v, want configured stall duration", err)
	}
}

func TestAppServerStreamTurnResetsStallTimeoutAfterActivity(t *testing.T) {
	t.Parallel()

	transport := &deadlineRecordingAppServerTransport{
		fakeAppServerTransport: newFakeAppServerTransport([]Message{
			notificationMessage(t, "item/agentMessage/delta", `{
				"threadId":"thread-1",
				"turnId":"turn-1",
				"itemId":"item-1",
				"delta":"still working"
			}`),
			notificationMessage(t, "turn/completed", `{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed"}}`),
		}),
	}
	server, err := NewAppServer(staticTransportFactory{transport: transport})
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	if err := server.streamTurn(context.Background(), transport, "thread-1", "turn-1", time.Hour, time.Second, nil, nil, nil); err != nil {
		t.Fatalf("streamTurn() error = %v", err)
	}
	if len(transport.deadlines) != 2 {
		t.Fatalf("Receive() deadlines = %v, want two stall deadlines", transport.deadlines)
	}
	if !transport.deadlines[1].After(transport.deadlines[0]) {
		t.Fatalf("Receive() deadlines = %v, want activity to reset stall deadline", transport.deadlines)
	}
}

func TestAppServerStreamTurnResetsTurnTimeoutAfterActivity(t *testing.T) {
	t.Parallel()

	transport := &deadlineRecordingAppServerTransport{
		fakeAppServerTransport: newFakeAppServerTransport([]Message{
			notificationMessage(t, "item/agentMessage/delta", `{
				"threadId":"thread-1",
				"turnId":"turn-1",
				"itemId":"item-1",
				"delta":"still working"
			}`),
			notificationMessage(t, "turn/completed", `{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed"}}`),
		}),
	}
	server, err := NewAppServer(staticTransportFactory{transport: transport})
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	if err := server.streamTurn(context.Background(), transport, "thread-1", "turn-1", time.Second, 0, nil, nil, nil); err != nil {
		t.Fatalf("streamTurn() error = %v", err)
	}
	if len(transport.deadlines) != 2 {
		t.Fatalf("Receive() deadlines = %v, want two turn timeout deadlines", transport.deadlines)
	}
	if !transport.deadlines[1].After(transport.deadlines[0]) {
		t.Fatalf("Receive() deadlines = %v, want activity to reset turn timeout deadline", transport.deadlines)
	}
}

func TestReceiveTurnMessageTimeoutSelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		context      func() context.Context
		turnTimeout  time.Duration
		stallTimeout time.Duration
		want         error
		wantStall    bool
	}{
		{
			name:        "stall disabled",
			context:     context.Background,
			turnTimeout: 10 * time.Millisecond,
			want:        context.DeadlineExceeded,
		},
		{
			name:         "turn timeout is shorter",
			context:      context.Background,
			turnTimeout:  10 * time.Millisecond,
			stallTimeout: time.Hour,
			want:         context.DeadlineExceeded,
		},
		{
			name:         "stall timeout is shorter",
			context:      context.Background,
			turnTimeout:  time.Hour,
			stallTimeout: 10 * time.Millisecond,
			want:         ErrStreamStalled,
			wantStall:    true,
		},
		{
			name: "parent canceled",
			context: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			turnTimeout:  time.Hour,
			stallTimeout: time.Second,
			want:         context.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newBlockingAppServerTransport(nil)
			factory := newControlledTimeoutFactory()
			run := func() error {
				_, err := receiveTurnMessage(tt.context(), transport, tt.turnTimeout, tt.stallTimeout, factory.context)
				return err
			}
			var err error
			if errors.Is(tt.want, context.Canceled) {
				err = run()
			} else {
				timeout := tt.turnTimeout
				var cause error
				if tt.stallTimeout > 0 && (tt.turnTimeout <= 0 || tt.stallTimeout <= tt.turnTimeout) {
					timeout = tt.stallTimeout
					cause = ErrStreamStalled
				}
				err = runWithTimeoutExpiration(t, factory, timeout, cause, run)
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("receiveTurnMessage() error = %v, want %v", err, tt.want)
			}
			if got := errors.Is(err, ErrStreamStalled); got != tt.wantStall {
				t.Fatalf("receiveTurnMessage() ErrStreamStalled = %t, want %t", got, tt.wantStall)
			}
		})
	}
}

func TestAppServerRunTurnRespondsToServerRequests(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	transport := newFakeAppServerTransport([]Message{
		responseMessage(t, 1, `{"userAgent":"codex-cli/0.135.0"}`),
		responseMessage(t, 2, `{"thread":{"id":"thread-1"}}`),
		responseMessage(t, 5, `{"config":{"model":"gpt-5.6"}}`),
		serverRequestMessage(t, 40, "item/tool/requestUserInput", `{"threadId":"thread-1","turnId":"turn-1"}`),
		responseMessage(t, 3, `{"turn":{"id":"turn-1"}}`),
		serverRequestMessage(t, 41, "item/commandExecution/requestApproval", `{"threadId":"thread-1","turnId":"turn-1"}`),
		serverRequestMessage(t, 42, "item/fileChange/requestApproval", `{"threadId":"thread-1","turnId":"turn-1"}`),
		serverRequestMessage(t, 43, "item/permissions/requestApproval", `{"threadId":"thread-1","turnId":"turn-1"}`),
		serverRequestMessage(t, 44, "mcpServer/elicitation/request", `{
			"threadId":"thread-1",
			"turnId":"turn-1",
			"serverName":"chrome-devtools",
			"mode":"form",
			"message":"Allow the chrome-devtools MCP server to run tool \"navigate_page\"?",
			"requestedSchema":{"type":"object","properties":{}},
			"_meta":{"codex_approval_kind":"mcp_tool_call"}
		}`),
		serverRequestMessage(t, 45, "mcpServer/elicitation/request", `{
			"threadId":"thread-1",
			"turnId":"turn-1",
			"serverName":"codex_apps",
			"mode":"form",
			"message":"Allow the GitHub connector to create a pull request?",
			"requestedSchema":{"type":"object","properties":{}},
			"_meta":{"codex_approval_kind":"mcp_tool_call"}
		}`),
		serverRequestMessage(t, 46, "custom/request", `{"threadId":"thread-1","turnId":"turn-1"}`),
		notificationMessage(t, "turn/completed", `{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed"}}`),
	})
	server, err := NewAppServer(staticTransportFactory{transport: transport},
		WithLogger(slog.New(slog.NewTextHandler(&logs, nil))),
		WithReadTimeout(time.Second),
		WithTurnTimeout(time.Second),
	)
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	_, err = server.RunTurn(context.Background(), RunTurnRequest{
		Workspace:      "/tmp/detent-workspace",
		Prompt:         "Ship issue #18",
		ApprovalPolicy: "never",
	}, nil)
	if err != nil {
		t.Fatalf("RunTurn() error = %v", err)
	}

	sent := transport.sentMessages()
	if len(sent) != 12 {
		t.Fatalf("sent messages = %d, want 12: %#v", len(sent), sent)
	}

	assertRequest(t, sent[3], 5, "config/read")
	assertResponseResultContains(t, sent[5], 40, "answers", map[string]any{})
	assertResponseResultContains(t, sent[6], 41, "decision", "decline")
	assertResponseResultContains(t, sent[7], 42, "decision", "decline")
	assertResponseResultContains(t, sent[8], 43, "permissions", map[string]any{})
	assertResponseResultContains(t, sent[9], 44, "action", "accept")
	assertResponseResultContains(t, sent[9], 44, "content", map[string]any{})
	assertResponseResultContains(t, sent[10], 45, "action", "decline")
	assertResponseResultContains(t, sent[10], 45, "content", nil)
	assertErrorResponse(t, sent[11], 46, methodNotFoundCode, "unsupported server request: custom/request")

	for _, want := range []string{
		"method=mcpServer/elicitation/request server=chrome-devtools tool=\"\" repository=\"\" mode=form approval_kind=mcp_tool_call action=accept reason=supported_browser_tool_approval",
		"method=mcpServer/elicitation/request server=codex_apps tool=\"\" repository=\"\" mode=form approval_kind=\"\" action=decline reason=unsupported_server",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("logs = %q, missing %q", logs.String(), want)
		}
	}
}

func TestMCPElicitationResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		params         string
		policy         MCPElicitationPolicy
		pending        []string
		wantAction     string
		wantReason     string
		wantTool       string
		wantRepository string
	}{
		{
			name: "browser tool approval",
			params: `{
				"serverName":"chrome-devtools",
				"mode":"form",
				"requestedSchema":{"type":"object","properties":{}},
				"_meta":{"codex_approval_kind":"mcp_tool_call"}
			}`,
			wantAction: "accept",
			wantReason: "supported_browser_tool_approval",
		},
		{
			name: "approval policy never with empty allowlist preserves decline",
			params: `{
				"threadId":"thread-1",
				"turnId":"turn-1",
				"serverName":"codex_apps",
				"mode":"form",
				"requestedSchema":{"type":"object","properties":{}},
				"_meta":{"codex_approval_kind":"mcp_tool_call"}
			}`,
			policy: MCPElicitationPolicy{DeliverableKind: "pull_request", IssueRepository: "acme/widgets"},
			pending: []string{`{
				"threadId":"thread-1","turnId":"turn-1",
				"item":{"id":"item-1","type":"mcpToolCall","server":"codex_apps","tool":"github.create_issue","arguments":{"repository_full_name":"acme/widgets","title":"Follow-up"}}
			}`},
			wantAction: "decline",
			wantReason: "unsupported_server",
			wantTool:   "github.create_issue",
		},
		{
			name: "allowlisted deliverable tuple",
			params: `{
				"threadId":"thread-1",
				"turnId":"turn-1",
				"serverName":"codex_apps",
				"mode":"form",
				"requestedSchema":{"type":"object","properties":{}},
				"_meta":{"codex_approval_kind":"mcp_tool_call"}
			}`,
			policy: MCPElicitationPolicy{
				DeliverableKind: "pull_request",
				Repository:      "acme/widgets",
				Allowlist: []MCPElicitationRule{{
					Server: "codex_apps", Tool: "github.create_pull_request", Repository: "acme/widgets",
				}},
			},
			pending: []string{`{
				"threadId":"thread-1","turnId":"turn-1",
				"item":{"id":"item-1","type":"mcpToolCall","server":"codex_apps","tool":"github.create_pull_request","arguments":{"repository_full_name":"acme/widgets"}}
			}`},
			wantAction:     "accept",
			wantReason:     "allowlisted_deliverable_tool",
			wantTool:       "github.create_pull_request",
			wantRepository: "acme/widgets",
		},
		{
			name: "whole server rule without tool match",
			params: `{
				"threadId":"thread-1","turnId":"turn-1","serverName":"codex_apps","mode":"form",
				"requestedSchema":{"type":"object","properties":{}},"_meta":{"codex_approval_kind":"mcp_tool_call"}
			}`,
			policy: MCPElicitationPolicy{
				DeliverableKind: "pull_request",
				Repository:      "acme/widgets",
				Allowlist:       []MCPElicitationRule{{Server: "codex_apps"}},
			},
			pending: []string{`{
				"threadId":"thread-1","turnId":"turn-1",
				"item":{"id":"item-1","type":"mcpToolCall","server":"codex_apps","tool":"github.create_pull_request","arguments":{"repository_full_name":"acme/widgets"}}
			}`},
			wantAction: "decline",
			wantReason: "tool_not_allowlisted",
			wantTool:   "github.create_pull_request",
		},
		{
			name: "mismatched repository",
			params: `{
				"threadId":"thread-1","turnId":"turn-1","serverName":"codex_apps","mode":"form",
				"requestedSchema":{"type":"object","properties":{}},"_meta":{"codex_approval_kind":"mcp_tool_call"}
			}`,
			policy: MCPElicitationPolicy{
				DeliverableKind: "pull_request",
				Repository:      "acme/widgets",
				Allowlist: []MCPElicitationRule{{
					Server: "codex_apps", Tool: "github.update_pull_request", Repository: "acme/widgets",
				}},
			},
			pending: []string{`{
				"threadId":"thread-1","turnId":"turn-1",
				"item":{"id":"item-1","type":"mcpToolCall","server":"codex_apps","tool":"github.update_pull_request","arguments":{"repository_full_name":"acme/other","pr_number":18}}
			}`},
			wantAction:     "decline",
			wantReason:     "repository_mismatch",
			wantTool:       "github.update_pull_request",
			wantRepository: "acme/other",
		},
		{
			name: "non-deliverable mutation",
			params: `{
				"threadId":"thread-1","turnId":"turn-1","serverName":"codex_apps","mode":"form",
				"requestedSchema":{"type":"object","properties":{}},"_meta":{"codex_approval_kind":"mcp_tool_call"}
			}`,
			policy: MCPElicitationPolicy{
				DeliverableKind: "pull_request",
				Repository:      "acme/widgets",
				Allowlist: []MCPElicitationRule{{
					Server: "codex_apps", Tool: "github.delete_issue", Repository: "acme/widgets",
				}},
			},
			pending: []string{`{
				"threadId":"thread-1","turnId":"turn-1",
				"item":{"id":"item-1","type":"mcpToolCall","server":"codex_apps","tool":"github.delete_issue","arguments":{"repository_full_name":"acme/widgets","issue_number":18}}
			}`},
			wantAction: "decline",
			wantReason: "non_deliverable_mutation",
			wantTool:   "github.delete_issue",
		},
		{
			name: "missing correlation",
			params: `{
				"threadId":"thread-1","turnId":"turn-1","serverName":"codex_apps","mode":"form",
				"requestedSchema":{"type":"object","properties":{}},"_meta":{"codex_approval_kind":"mcp_tool_call"}
			}`,
			policy: MCPElicitationPolicy{
				DeliverableKind: "pull_request",
				Repository:      "acme/widgets",
				Allowlist: []MCPElicitationRule{{
					Server: "codex_apps", Tool: "github.create_pull_request", Repository: "acme/widgets",
				}},
			},
			wantAction: "decline",
			wantReason: "missing_correlation",
		},
		{
			name: "ambiguous correlation",
			params: `{
				"threadId":"thread-1","turnId":"turn-1","serverName":"codex_apps","mode":"form",
				"requestedSchema":{"type":"object","properties":{}},"_meta":{"codex_approval_kind":"mcp_tool_call"}
			}`,
			policy: MCPElicitationPolicy{
				DeliverableKind: "pull_request",
				Repository:      "acme/widgets",
				Allowlist: []MCPElicitationRule{
					{Server: "codex_apps", Tool: "github.create_pull_request", Repository: "acme/widgets"},
					{Server: "codex_apps", Tool: "github.update_pull_request", Repository: "acme/widgets"},
				},
			},
			pending: []string{
				`{"threadId":"thread-1","turnId":"turn-1","item":{"id":"item-1","type":"mcpToolCall","server":"codex_apps","tool":"github.create_pull_request","arguments":{"repository_full_name":"acme/widgets"}}}`,
				`{"threadId":"thread-1","turnId":"turn-1","item":{"id":"item-2","type":"mcpToolCall","server":"codex_apps","tool":"github.update_pull_request","arguments":{"repository_full_name":"acme/widgets","pr_number":18}}}`,
			},
			wantAction: "decline",
			wantReason: "ambiguous_correlation",
		},
		{
			name: "unsupported server",
			params: `{
				"serverName":"filesystem",
				"mode":"form",
				"requestedSchema":{"type":"object","properties":{}},
				"_meta":{"codex_approval_kind":"mcp_tool_call"}
			}`,
			wantAction: "decline",
			wantReason: "unsupported_server",
		},
		{
			name: "browser URL elicitation",
			params: `{
				"serverName":"chrome-devtools",
				"mode":"url",
				"url":"https://example.com/authorize",
				"elicitationId":"elicitation-1"
			}`,
			wantAction: "decline",
			wantReason: "unsupported_mode",
		},
		{
			name: "browser input form",
			params: `{
				"serverName":"chrome-devtools",
				"mode":"form",
				"requestedSchema":{"type":"object","properties":{"value":{"type":"string"}}},
				"_meta":{"codex_approval_kind":"mcp_tool_call"}
			}`,
			wantAction: "decline",
			wantReason: "unsupported_schema",
		},
		{
			name: "browser constrained empty form",
			params: `{
				"serverName":"chrome-devtools",
				"mode":"form",
				"requestedSchema":{"type":"object","properties":{},"required":["confirmation"]},
				"_meta":{"codex_approval_kind":"mcp_tool_call"}
			}`,
			wantAction: "decline",
			wantReason: "unsupported_schema",
		},
		{
			name: "unsupported browser form",
			params: `{
				"serverName":"chrome-devtools",
				"mode":"form",
				"requestedSchema":{"type":"object","properties":{}},
				"_meta":{"codex_approval_kind":"tool_suggestion"}
			}`,
			wantAction: "decline",
			wantReason: "unsupported_approval_kind",
		},
		{
			name:   "malformed request",
			params: `{`,
			policy: MCPElicitationPolicy{Allowlist: []MCPElicitationRule{{
				Server: "codex_apps", Tool: "github.create_pull_request", Repository: "acme/widgets",
			}}},
			wantAction: "decline",
			wantReason: "invalid_request",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			state := newMCPElicitationState(tt.policy, nil)
			for _, params := range tt.pending {
				if err := state.observe(notificationMessage(t, "item/started", params)); err != nil {
					t.Fatalf("observe() error = %v", err)
				}
			}
			response, decision := mcpElicitationResponse(json.RawMessage(tt.params), state)
			if got := response["action"]; got != tt.wantAction {
				t.Errorf("action = %v, want %q", got, tt.wantAction)
			}
			if decision.Action != tt.wantAction {
				t.Errorf("decision action = %q, want %q", decision.Action, tt.wantAction)
			}
			if decision.Reason != tt.wantReason {
				t.Errorf("decision reason = %q, want %q", decision.Reason, tt.wantReason)
			}
			if decision.Tool != tt.wantTool {
				t.Errorf("decision tool = %q, want %q", decision.Tool, tt.wantTool)
			}
			if decision.Repository != tt.wantRepository {
				t.Errorf("decision repository = %q, want %q", decision.Repository, tt.wantRepository)
			}
			if tt.wantAction == "accept" {
				if got, ok := response["content"].(map[string]any); !ok || len(got) != 0 {
					t.Errorf("content = %#v, want empty object", response["content"])
				}
			} else if response["content"] != nil {
				t.Errorf("content = %#v, want nil", response["content"])
			}
		})
	}
}

func TestMCPElicitationResponseAllowsWorkflowDeliverableMutations(t *testing.T) {
	t.Parallel()

	const request = `{
		"threadId":"thread-1","turnId":"turn-1","serverName":"codex_apps","mode":"form",
		"requestedSchema":{"type":"object","properties":{}},"_meta":{"codex_approval_kind":"mcp_tool_call"}
	}`
	tests := []struct {
		name             string
		tool             string
		arguments        string
		sourceRepository string
		wantAction       string
		wantReason       string
		wantRepository   string
	}{
		{
			name:           "create same project issue",
			tool:           "github.create_issue",
			arguments:      `{"repository_full_name":"acme/widgets","title":"Follow-up"}`,
			wantAction:     "accept",
			wantReason:     "allowlisted_deliverable_tool",
			wantRepository: "acme/widgets",
		},
		{
			name:           "add same project issue comment",
			tool:           "github.add_comment_to_issue",
			arguments:      `{"repo_full_name":"acme/widgets","pr_number":18,"comment":"workpad"}`,
			wantAction:     "accept",
			wantReason:     "allowlisted_deliverable_tool",
			wantRepository: "acme/widgets",
		},
		{
			name:           "update same project issue comment",
			tool:           "github.update_issue_comment",
			arguments:      `{"repo_full_name":"acme/widgets","comment_id":42,"comment":"updated workpad"}`,
			wantAction:     "accept",
			wantReason:     "allowlisted_deliverable_tool",
			wantRepository: "acme/widgets",
		},
		{
			name:           "update same project issue",
			tool:           "github.update_issue",
			arguments:      `{"repository_full_name":"acme/widgets","issue_number":18,"labels":["detent:in-progress"]}`,
			wantAction:     "accept",
			wantReason:     "allowlisted_deliverable_tool",
			wantRepository: "acme/widgets",
		},
		{
			name:             "create cross project issue fails closed",
			tool:             "github.create_issue",
			arguments:        `{"repository_full_name":"acme/widgets","title":"Follow-up"}`,
			sourceRepository: "acme/delivery",
			wantAction:       "decline",
			wantReason:       "cross_project_issue_publication_unsupported",
			wantRepository:   "acme/widgets",
		},
		{
			name:             "add cross project issue comment fails closed",
			tool:             "github.add_comment_to_issue",
			arguments:        `{"repo_full_name":"acme/widgets","pr_number":18,"comment":"workpad"}`,
			sourceRepository: "acme/delivery",
			wantAction:       "decline",
			wantReason:       "cross_project_issue_publication_unsupported",
			wantRepository:   "acme/widgets",
		},
		{
			name:             "update cross project issue comment fails closed",
			tool:             "github.update_issue_comment",
			arguments:        `{"repo_full_name":"acme/widgets","comment_id":42,"comment":"updated workpad"}`,
			sourceRepository: "acme/delivery",
			wantAction:       "decline",
			wantReason:       "cross_project_issue_publication_unsupported",
			wantRepository:   "acme/widgets",
		},
		{
			name:             "update cross project issue fails closed",
			tool:             "github.update_issue",
			arguments:        `{"repository_full_name":"acme/widgets","issue_number":18,"labels":["detent:in-progress"]}`,
			sourceRepository: "acme/delivery",
			wantAction:       "decline",
			wantReason:       "cross_project_issue_publication_unsupported",
			wantRepository:   "acme/widgets",
		},
		{
			name:           "create issue mismatched repository",
			tool:           "github.create_issue",
			arguments:      `{"repository_full_name":"acme/other","title":"Follow-up"}`,
			wantAction:     "decline",
			wantReason:     "repository_mismatch",
			wantRepository: "acme/other",
		},
		{
			name:           "add issue comment mismatched repository",
			tool:           "github.add_comment_to_issue",
			arguments:      `{"repo_full_name":"acme/other","pr_number":18,"comment":"workpad"}`,
			wantAction:     "decline",
			wantReason:     "repository_mismatch",
			wantRepository: "acme/other",
		},
		{
			name:           "update issue comment mismatched repository",
			tool:           "github.update_issue_comment",
			arguments:      `{"repo_full_name":"acme/other","comment_id":42,"comment":"updated workpad"}`,
			wantAction:     "decline",
			wantReason:     "repository_mismatch",
			wantRepository: "acme/other",
		},
		{
			name:           "update issue mismatched repository",
			tool:           "github.update_issue",
			arguments:      `{"repository_full_name":"acme/other","issue_number":18,"labels":["detent:in-progress"]}`,
			wantAction:     "decline",
			wantReason:     "repository_mismatch",
			wantRepository: "acme/other",
		},
		{
			name:       "create issue wrong repository argument",
			tool:       "github.create_issue",
			arguments:  `{"repo_full_name":"acme/widgets","title":"Follow-up"}`,
			wantAction: "decline",
			wantReason: "invalid_tool_arguments",
		},
		{
			name:       "add issue comment wrong repository argument",
			tool:       "github.add_comment_to_issue",
			arguments:  `{"repository_full_name":"acme/widgets","pr_number":18,"comment":"workpad"}`,
			wantAction: "decline",
			wantReason: "invalid_tool_arguments",
		},
		{
			name:       "update issue comment wrong repository argument",
			tool:       "github.update_issue_comment",
			arguments:  `{"repository_full_name":"acme/widgets","comment_id":42,"comment":"updated workpad"}`,
			wantAction: "decline",
			wantReason: "invalid_tool_arguments",
		},
		{
			name:       "update issue wrong repository argument",
			tool:       "github.update_issue",
			arguments:  `{"repo_full_name":"acme/widgets","issue_number":18,"labels":["detent:in-progress"]}`,
			wantAction: "decline",
			wantReason: "invalid_tool_arguments",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sourceRepository := tt.sourceRepository
			if sourceRepository == "" {
				sourceRepository = "acme/widgets"
			}
			state := newMCPElicitationState(MCPElicitationPolicy{
				DeliverableKind: "pull_request",
				Repository:      sourceRepository,
				IssueRepository: "acme/widgets",
				Allowlist: []MCPElicitationRule{{
					Server: "codex_apps", Tool: tt.tool, Repository: "acme/widgets",
				}},
			}, nil)
			pending := `{"threadId":"thread-1","turnId":"turn-1","item":{"id":"item-1","type":"mcpToolCall","server":"codex_apps","tool":"` + tt.tool + `","arguments":` + tt.arguments + `}}`
			if err := state.observe(notificationMessage(t, "item/started", pending)); err != nil {
				t.Fatalf("observe() error = %v", err)
			}

			response, decision := mcpElicitationResponse(json.RawMessage(request), state)
			if got := response["action"]; got != tt.wantAction {
				t.Errorf("action = %v, want %q", got, tt.wantAction)
			}
			if decision.Action != tt.wantAction {
				t.Errorf("decision action = %q, want %q", decision.Action, tt.wantAction)
			}
			if decision.Reason != tt.wantReason {
				t.Errorf("decision reason = %q, want %q", decision.Reason, tt.wantReason)
			}
			if decision.Tool != tt.tool {
				t.Errorf("decision tool = %q, want %q", decision.Tool, tt.tool)
			}
			if decision.Repository != tt.wantRepository {
				t.Errorf("decision repository = %q, want %q", decision.Repository, tt.wantRepository)
			}
		})
	}
}

func TestAppServerRunTurnRecordsDeclinedMCPElicitations(t *testing.T) {
	t.Parallel()

	const request = `{
		"threadId":"thread-1","turnId":"turn-1","serverName":"codex_apps","mode":"form",
		"requestedSchema":{"type":"object","properties":{}},"_meta":{"codex_approval_kind":"mcp_tool_call"}
	}`
	tests := []struct {
		name       string
		started    []string
		repository string
		wantReason string
		wantTool   string
	}{
		{
			name:       "missing correlation",
			repository: "acme/widgets",
			wantReason: "missing_correlation",
		},
		{
			name: "ambiguous correlation",
			started: []string{
				`{"threadId":"thread-1","turnId":"turn-1","item":{"id":"item-1","type":"mcpToolCall","server":"codex_apps","tool":"github.create_pull_request","arguments":{"repository_full_name":"acme/widgets"}}}`,
				`{"threadId":"thread-1","turnId":"turn-1","item":{"id":"item-2","type":"mcpToolCall","server":"codex_apps","tool":"github.update_pull_request","arguments":{"repository_full_name":"acme/widgets","pr_number":18}}}`,
			},
			repository: "acme/widgets",
			wantReason: "ambiguous_correlation",
		},
		{
			name: "correlated repository mismatch",
			started: []string{
				`{"threadId":"thread-1","turnId":"turn-1","item":{"id":"item-1","type":"mcpToolCall","server":"codex_apps","tool":"github.create_pull_request","arguments":{"repository_full_name":"acme/other"}}}`,
			},
			repository: "acme/widgets",
			wantReason: "repository_mismatch",
			wantTool:   "codex_apps/github.create_pull_request",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			received := []Message{
				responseMessage(t, 1, `{"userAgent":"codex-cli/0.135.0"}`),
				responseMessage(t, 2, `{"thread":{"id":"thread-1","model":"gpt-5.6"}}`),
				responseMessage(t, 3, `{"turn":{"id":"turn-1"}}`),
			}
			for _, params := range tt.started {
				received = append(received, notificationMessage(t, "item/started", params))
			}
			received = append(received,
				serverRequestMessage(t, 44, "mcpServer/elicitation/request", request),
				notificationMessage(t, "turn/completed", `{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed"}}`),
			)
			transport := newFakeAppServerTransport(received)
			server, err := NewAppServer(staticTransportFactory{transport: transport}, WithReadTimeout(time.Second), WithTurnTimeout(time.Second))
			if err != nil {
				t.Fatalf("NewAppServer() error = %v", err)
			}

			var decisions []Update
			_, err = server.RunTurn(context.Background(), RunTurnRequest{
				Workspace:      "/tmp/detent-workspace",
				Prompt:         "Ship issue #18",
				ApprovalPolicy: "never",
				MCPElicitationPolicy: MCPElicitationPolicy{
					DeliverableKind: "pull_request",
					Repository:      tt.repository,
					Allowlist: []MCPElicitationRule{
						{Server: "codex_apps", Tool: "github.create_pull_request", Repository: "acme/widgets"},
						{Server: "codex_apps", Tool: "github.update_pull_request", Repository: "acme/widgets"},
					},
				},
			}, func(update Update) error {
				if update.Type == UpdateMCPElicitation {
					decisions = append(decisions, update)
				}
				return nil
			})
			if err != nil {
				t.Fatalf("RunTurn() error = %v", err)
			}

			sent := transport.sentMessages()
			if len(sent) != 5 {
				t.Fatalf("sent messages = %d, want 5: %#v", len(sent), sent)
			}
			assertResponseResultContains(t, sent[4], 44, "action", "decline")
			if len(decisions) != 1 {
				t.Fatalf("elicitation updates = %#v, want one", decisions)
			}
			decision := decisions[0]
			if decision.Status != "decline" || decision.Tool != tt.wantTool {
				t.Errorf("elicitation update = %#v, want declined tool %q", decision, tt.wantTool)
			}
			for _, fragment := range []string{"server=codex_apps", "reason=" + tt.wantReason} {
				if !strings.Contains(decision.Delta, fragment) {
					t.Errorf("elicitation content = %q, want containing %q", decision.Delta, fragment)
				}
			}
			if tt.wantTool != "" && !strings.Contains(decision.Delta, "tool=github.create_pull_request") {
				t.Errorf("elicitation content = %q, want correlated tool", decision.Delta)
			}
		})
	}
}

func TestAppServerRunTurnAllowsConfiguredDeliverableElicitationAcrossApprovalPolicies(t *testing.T) {
	t.Parallel()

	for _, approvalPolicy := range []string{"never", "on-request"} {
		t.Run(approvalPolicy, func(t *testing.T) {
			t.Parallel()

			transport := newFakeAppServerTransport([]Message{
				responseMessage(t, 1, `{"userAgent":"codex-cli/0.135.0"}`),
				responseMessage(t, 2, `{"thread":{"id":"thread-1","model":"gpt-5.6"}}`),
				responseMessage(t, 3, `{"turn":{"id":"turn-1"}}`),
				notificationMessage(t, "item/started", `{
					"threadId":"thread-1","turnId":"turn-1",
					"item":{"id":"item-1","type":"mcpToolCall","server":"codex_apps","tool":"github.create_pull_request","arguments":{"repository_full_name":"acme/widgets"}}
				}`),
				serverRequestMessage(t, 44, "mcpServer/elicitation/request", `{
					"threadId":"thread-1","turnId":"turn-1","serverName":"codex_apps","mode":"form",
					"requestedSchema":{"type":"object","properties":{}},"_meta":{"codex_approval_kind":"mcp_tool_call"}
				}`),
				notificationMessage(t, "turn/completed", `{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed"}}`),
			})
			server, err := NewAppServer(staticTransportFactory{transport: transport}, WithReadTimeout(time.Second), WithTurnTimeout(time.Second))
			if err != nil {
				t.Fatalf("NewAppServer() error = %v", err)
			}

			_, err = server.RunTurn(context.Background(), RunTurnRequest{
				Workspace:      "/tmp/detent-workspace",
				Prompt:         "Ship issue #18",
				ApprovalPolicy: approvalPolicy,
				MCPElicitationPolicy: MCPElicitationPolicy{
					DeliverableKind: "pull_request",
					Repository:      "acme/widgets",
					Allowlist: []MCPElicitationRule{{
						Server: "codex_apps", Tool: "github.create_pull_request", Repository: "acme/widgets",
					}},
				},
			}, nil)
			if err != nil {
				t.Fatalf("RunTurn() error = %v", err)
			}

			sent := transport.sentMessages()
			if len(sent) != 5 {
				t.Fatalf("sent messages = %d, want 5: %#v", len(sent), sent)
			}
			assertJSONContains(t, sent[2].Params, "approvalPolicy", approvalPolicy)
			assertJSONContains(t, sent[3].Params, "approvalPolicy", approvalPolicy)
			assertResponseResultContains(t, sent[4], 44, "action", "accept")
		})
	}
}

func TestAppServerRunTurnReportsResponseErrors(t *testing.T) {
	t.Parallel()

	transport := newFakeAppServerTransport([]Message{
		errorResponseMessage(t, 1, -32000, "initialize failed"),
	})
	server, err := NewAppServer(staticTransportFactory{transport: transport})
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	_, err = server.RunTurn(context.Background(), RunTurnRequest{
		Workspace: "/tmp/detent-workspace",
		Prompt:    "Ship issue #18",
	}, nil)
	if err == nil {
		t.Fatal("RunTurn() error = nil, want response error")
	}
	if !errors.Is(err, ErrResponseError) {
		t.Fatalf("RunTurn() error = %v, want ErrResponseError", err)
	}
}

func TestAppServerRunTurnReportsTurnErrorBody(t *testing.T) {
	t.Parallel()

	backendError := `{"type":"error","status":400,"error":{"type":"invalid_request_error","message":"The 'gpt-5-codex' model is not supported when using Codex with a ChatGPT account."}}`
	transport := newFakeAppServerTransport([]Message{
		responseMessage(t, 1, `{"userAgent":"codex-cli/0.142.5"}`),
		responseMessage(t, 2, `{"thread":{"id":"thread-1","model":"gpt-5-codex"}}`),
		responseMessage(t, 3, `{"turn":{"id":"turn-1"}}`),
		notificationMessage(t, "turn/completed", backendError),
	})
	server, err := NewAppServer(staticTransportFactory{transport: transport},
		WithReadTimeout(time.Second),
		WithTurnTimeout(time.Second),
	)
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	var updates []Update
	_, err = server.RunTurn(context.Background(), RunTurnRequest{
		Workspace: "/tmp/detent-workspace",
		Prompt:    "Ship issue #927",
		Model:     "gpt-5-codex",
	}, func(update Update) error {
		updates = append(updates, update)
		return nil
	})
	if !errors.Is(err, ErrTurnFailed) {
		t.Fatalf("RunTurn() error = %v, want ErrTurnFailed", err)
	}
	var turnErr *TurnFailedError
	if !errors.As(err, &turnErr) {
		t.Fatalf("RunTurn() error = %T, want TurnFailedError", err)
	}
	if turnErr.BackendErrorBody() != backendError {
		t.Fatalf("BackendErrorBody = %q, want %q", turnErr.BackendErrorBody(), backendError)
	}
	if !strings.Contains(err.Error(), backendError) {
		t.Fatalf("RunTurn() error = %v, want backend body", err)
	}
	if len(updates) != 3 {
		t.Fatalf("updates = %#v, want identity, turn started, and failed turn completed", updates)
	}
	if updates[2].Status != "failed" || updates[2].BackendErrorBody != backendError {
		t.Fatalf("failed update = %#v, want status failed with backend error", updates[2])
	}
}

func TestAppServerRunTurnWaitsForRootCompletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		childCompletion string
	}{
		{
			name:            "completed child",
			childCompletion: `{"threadId":"thread-child","turn":{"id":"turn-child","status":"completed"}}`,
		},
		{
			name:            "interrupted child",
			childCompletion: `{"threadId":"thread-child","turn":{"id":"turn-child","status":"interrupted","error":null}}`,
		},
		{
			name:            "failed child",
			childCompletion: `{"threadId":"thread-child","turn":{"id":"turn-child","status":"failed","error":{"message":"child failed"}}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			transport := newFakeAppServerTransport([]Message{
				responseMessage(t, 1, `{"userAgent":"codex-cli/0.151.0"}`),
				responseMessage(t, 2, `{"thread":{"id":"thread-root","model":"gpt-5.6-sol"}}`),
				responseMessage(t, 3, `{"turn":{"id":"turn-root"}}`),
				notificationMessage(t, "turn/completed", tt.childCompletion),
				notificationMessage(t, "turn/completed", `{"threadId":"thread-root","turn":{"id":"turn-root","status":"completed"}}`),
			})
			server, err := NewAppServer(staticTransportFactory{transport: transport},
				WithReadTimeout(time.Second),
				WithTurnTimeout(time.Second),
			)
			if err != nil {
				t.Fatalf("NewAppServer() error = %v", err)
			}

			var completions []Update
			result, err := server.RunTurn(context.Background(), RunTurnRequest{
				Workspace: "/tmp/detent-workspace",
				Prompt:    "Ship issue #2059",
			}, func(update Update) error {
				if update.Type == UpdateTurnCompleted {
					completions = append(completions, update)
				}
				return nil
			})
			if err != nil {
				t.Fatalf("RunTurn() error = %v", err)
			}
			if result.ThreadID != "thread-root" || result.TurnID != "turn-root" {
				t.Fatalf("RunTurn() result = %#v", result)
			}
			if len(transport.received) != 0 {
				t.Fatalf("unread transport messages = %d, want root completion consumed", len(transport.received))
			}
			if len(completions) != 2 {
				t.Fatalf("completion updates = %#v, want child and root completions", completions)
			}
			if completions[0].ThreadID != "thread-child" || completions[0].TurnID != "turn-child" || !completions[0].AuxiliaryTurn {
				t.Fatalf("child completion = %#v", completions[0])
			}
			if completions[1].ThreadID != "thread-root" || completions[1].TurnID != "turn-root" || completions[1].AuxiliaryTurn {
				t.Fatalf("root completion = %#v", completions[1])
			}
		})
	}
}

func TestAppServerRunTurnReportsInterruptedTurnWithoutNullDetail(t *testing.T) {
	t.Parallel()

	transport := newFakeAppServerTransport([]Message{
		responseMessage(t, 1, `{"userAgent":"codex-cli/0.142.5"}`),
		responseMessage(t, 2, `{"thread":{"id":"thread-1","model":"gpt-5-codex"}}`),
		responseMessage(t, 3, `{"turn":{"id":"turn-1"}}`),
		notificationMessage(t, "turn/completed", `{"threadId":"thread-1","turn":{"id":"turn-1","status":"interrupted","error":null}}`),
	})
	server, err := NewAppServer(staticTransportFactory{transport: transport},
		WithReadTimeout(time.Second),
		WithTurnTimeout(time.Second),
	)
	if err != nil {
		t.Fatalf("NewAppServer() error = %v", err)
	}

	_, err = server.RunTurn(context.Background(), RunTurnRequest{
		Workspace: "/tmp/detent-workspace",
		Prompt:    "Ship issue #1928",
	}, nil)
	if !errors.Is(err, ErrTurnFailed) {
		t.Fatalf("RunTurn() error = %v, want ErrTurnFailed", err)
	}
	var turnErr *TurnFailedError
	if !errors.As(err, &turnErr) {
		t.Fatalf("RunTurn() error = %T, want TurnFailedError", err)
	}
	if got := turnErr.BackendErrorBody(); got != `{"status":"interrupted","detail":"backend supplied no error"}` {
		t.Fatalf("BackendErrorBody = %q", got)
	}
	if got := turnErr.BackendErrorMessage(); got != "codex reported an interrupted turn without error detail" {
		t.Fatalf("BackendErrorMessage = %q", got)
	}
	if strings.HasSuffix(err.Error(), ": null") {
		t.Fatalf("RunTurn() error = %q, want actionable interruption detail", err)
	}
}

type staticTransportFactory struct {
	transport Transport
}

func (f staticTransportFactory) NewTransport(context.Context) (Transport, error) {
	return f.transport, nil
}

type fakeAppServerTransport struct {
	received        []Message
	sent            []Message
	processIdentity string
}

func newFakeAppServerTransport(received []Message) *fakeAppServerTransport {
	return &fakeAppServerTransport{received: append([]Message(nil), received...)}
}

func (t *fakeAppServerTransport) Send(_ context.Context, msg Message) error {
	t.sent = append(t.sent, msg)
	return nil
}

func (t *fakeAppServerTransport) Receive(context.Context) (Message, error) {
	if len(t.received) == 0 {
		return Message{}, io.EOF
	}
	msg := t.received[0]
	t.received = t.received[1:]
	return msg, nil
}

func (t *fakeAppServerTransport) Close(context.Context) error {
	return nil
}

func (t *fakeAppServerTransport) ProcessIdentity() string {
	return t.processIdentity
}

func (t *fakeAppServerTransport) sentMessages() []Message {
	return append([]Message(nil), t.sent...)
}

type blockingAppServerTransport struct {
	*fakeAppServerTransport
}

type controlledTimeoutFactory struct {
	active chan *controlledTimeoutContext
}

type controlledTimeoutContext struct {
	duration        time.Duration
	configuredCause error
	deadline        time.Time
	done            chan struct{}
	factory         *controlledTimeoutFactory
	activeOnce      sync.Once
	once            sync.Once
	mu              sync.Mutex
	err             error
}

type startupEvidenceTransport struct {
	*blockingAppServerTransport
	evidence backendcapacity.StartupProcessEvidence
}

func (t *startupEvidenceTransport) MarkStartupReady(readyAt time.Time) {
	t.evidence.Ready = true
	t.evidence.ReadyAt = &readyAt
}

func (t *startupEvidenceTransport) Close(context.Context) error {
	exitedAt := time.Now().UTC()
	t.evidence.ExitObserved = true
	t.evidence.ExitedAt = &exitedAt
	t.evidence.ExitStatus = "closed after startup failure"
	return nil
}

func (t *startupEvidenceTransport) StartupProcessEvidence() backendcapacity.StartupProcessEvidence {
	return t.evidence
}

func newBlockingAppServerTransport(received []Message) *blockingAppServerTransport {
	return &blockingAppServerTransport{fakeAppServerTransport: newFakeAppServerTransport(received)}
}

func newControlledTimeoutFactory() *controlledTimeoutFactory {
	return &controlledTimeoutFactory{active: make(chan *controlledTimeoutContext, 64)}
}

func (f *controlledTimeoutFactory) context(parent context.Context, duration time.Duration, cause error) (context.Context, context.CancelFunc) {
	controlled := &controlledTimeoutContext{
		duration:        duration,
		configuredCause: cause,
		deadline:        time.Now().Add(duration),
		done:            make(chan struct{}),
		factory:         f,
	}
	go func() {
		select {
		case <-parent.Done():
			controlled.expire(parent.Err())
		case <-controlled.Done():
		}
	}()
	return controlled, func() { controlled.expire(context.Canceled) }
}

func (c *controlledTimeoutContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

func (c *controlledTimeoutContext) Done() <-chan struct{} {
	return c.done
}

func (c *controlledTimeoutContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *controlledTimeoutContext) Value(any) any {
	return nil
}

func (c *controlledTimeoutContext) expire(err error) {
	c.once.Do(func() {
		c.mu.Lock()
		c.err = err
		c.mu.Unlock()
		close(c.done)
	})
}

func (c *controlledTimeoutContext) activate() {
	c.activeOnce.Do(func() { c.factory.active <- c })
}

func runWithTimeoutExpiration(
	t *testing.T,
	factory *controlledTimeoutFactory,
	duration time.Duration,
	cause error,
	run func() error,
) error {
	t.Helper()

	done := make(chan error, 1)
	go func() { done <- run() }()
	controlled := waitForControlledTimeout(t, factory, duration, cause)
	if controlled.configuredCause != nil {
		controlled.expire(controlled.configuredCause)
	} else {
		controlled.expire(context.DeadlineExceeded)
	}

	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		t.Fatal("timed out waiting for operation after controlled timeout")
		return nil
	}
}

func waitForControlledTimeout(
	t *testing.T,
	factory *controlledTimeoutFactory,
	duration time.Duration,
	cause error,
) *controlledTimeoutContext {
	t.Helper()

	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	for {
		select {
		case controlled := <-factory.active:
			if controlled.duration != duration {
				continue
			}
			if cause == nil && controlled.configuredCause == nil || cause != nil && errors.Is(controlled.configuredCause, cause) {
				return controlled
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for controlled timeout %s", duration)
			return nil
		}
	}
}

func (t *blockingAppServerTransport) Receive(ctx context.Context) (Message, error) {
	if len(t.received) > 0 {
		msg := t.received[0]
		t.received = t.received[1:]
		return msg, nil
	}
	if controlled, ok := ctx.(*controlledTimeoutContext); ok {
		controlled.activate()
	}
	<-ctx.Done()
	return Message{}, ctx.Err()
}

type deadlineRecordingAppServerTransport struct {
	*fakeAppServerTransport
	deadlines []time.Time
}

func (t *deadlineRecordingAppServerTransport) Receive(ctx context.Context) (Message, error) {
	deadline, ok := ctx.Deadline()
	if ok {
		t.deadlines = append(t.deadlines, deadline)
	}
	time.Sleep(time.Millisecond)
	return t.fakeAppServerTransport.Receive(ctx)
}

func responseMessage(t *testing.T, id int, result string) Message {
	t.Helper()

	return Message{
		JSONRPC: JSONRPCVersion,
		ID:      json.RawMessage(mustMarshalJSON(t, id)),
		Result:  json.RawMessage(result),
	}
}

func errorResponseMessage(t *testing.T, id int, code int, message string) Message {
	t.Helper()

	return Message{
		JSONRPC: JSONRPCVersion,
		ID:      json.RawMessage(mustMarshalJSON(t, id)),
		Error: &RPCError{
			Code:    code,
			Message: message,
		},
	}
}

func notificationMessage(t *testing.T, method string, params string) Message {
	t.Helper()

	return Message{
		JSONRPC: JSONRPCVersion,
		Method:  method,
		Params:  json.RawMessage(params),
	}
}

func serverRequestMessage(t *testing.T, id int, method string, params string) Message {
	t.Helper()

	msg := notificationMessage(t, method, params)
	msg.ID = json.RawMessage(mustMarshalJSON(t, id))
	return msg
}

func assertRequest(t *testing.T, msg Message, id int, method string) {
	t.Helper()

	if msg.Method != method {
		t.Fatalf("Method = %q, want %q", msg.Method, method)
	}
	if string(msg.ID) != mustMarshalJSON(t, id) {
		t.Fatalf("ID = %s, want %d", msg.ID, id)
	}
	if len(msg.Params) == 0 {
		t.Fatalf("Params empty for %s", method)
	}
}

func assertResponseResultContains(t *testing.T, msg Message, id int, path string, want any) {
	t.Helper()

	assertResponseID(t, msg, id)
	if msg.Error != nil {
		t.Fatalf("Error = %#v, want result", msg.Error)
	}
	if len(msg.Result) == 0 {
		t.Fatalf("Result empty for response %d", id)
	}
	assertJSONContains(t, msg.Result, path, want)
}

func assertErrorResponse(t *testing.T, msg Message, id int, code int, message string) {
	t.Helper()

	assertResponseID(t, msg, id)
	if msg.Error == nil {
		t.Fatalf("Error = nil, want code %d", code)
	}
	if msg.Error.Code != code || msg.Error.Message != message {
		t.Fatalf("Error = %#v, want code %d message %q", msg.Error, code, message)
	}
}

func assertResponseID(t *testing.T, msg Message, id int) {
	t.Helper()

	if msg.Method != "" {
		t.Fatalf("Method = %q, want response", msg.Method)
	}
	if string(msg.ID) != mustMarshalJSON(t, id) {
		t.Fatalf("ID = %s, want %d", msg.ID, id)
	}
}

func assertJSONContains(t *testing.T, data json.RawMessage, path string, want any) {
	t.Helper()

	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal JSON: %v", err)
	}

	got := lookupJSONPath(t, decoded, path)
	if !jsonValuesEqual(got, want) {
		t.Fatalf("%s = %#v, want %#v in %s", path, got, want, string(data))
	}
}

func assertJSONOmits(t *testing.T, data json.RawMessage, key string) {
	t.Helper()

	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal JSON: %v", err)
	}
	if _, ok := decoded[key]; ok {
		t.Fatalf("key %q unexpectedly present in %s", key, string(data))
	}
}

func lookupJSONPath(t *testing.T, value any, path string) any {
	t.Helper()

	parts := strings.Split(path, ".")
	current := value
	for _, part := range parts {
		switch node := current.(type) {
		case map[string]any:
			var ok bool
			current, ok = node[part]
			if !ok {
				t.Fatalf("path %q missing key %q in %#v", path, part, value)
			}
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(node) {
				t.Fatalf("path %q invalid index %q in %#v", path, part, value)
			}
			current = node[index]
		default:
			t.Fatalf("path %q hit non-container %#v", path, current)
		}
	}

	return current
}

func jsonValuesEqual(got any, want any) bool {
	gotData, err := json.Marshal(got)
	if err != nil {
		return false
	}
	wantData, err := json.Marshal(want)
	if err != nil {
		return false
	}
	var gotCanonical any
	if err := json.Unmarshal(gotData, &gotCanonical); err != nil {
		return false
	}
	var wantCanonical any
	if err := json.Unmarshal(wantData, &wantCanonical); err != nil {
		return false
	}
	return reflect.DeepEqual(gotCanonical, wantCanonical)
}

func mustMarshalJSON(t *testing.T, value any) string {
	t.Helper()

	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON: %v", err)
	}
	return string(data)
}
