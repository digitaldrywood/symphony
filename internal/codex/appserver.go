package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/agentidentity"
	"github.com/digitaldrywood/detent/internal/procgroup"
)

const (
	initializeRequestID   = 1
	threadStartRequestID  = 2
	turnStartRequestID    = 3
	threadResumeRequestID = 4
	configReadRequestID   = 5
	modelListRequestID    = 6
	threadReadRequestID   = 7
	accountReadRequestID  = 8
	methodNotFoundCode    = -32601
	chromeDevToolsServer  = "chrome-devtools"
	mcpToolApprovalKind   = "mcp_tool_call"

	defaultClientName    = "detent-orchestrator"
	defaultClientTitle   = "Detent Orchestrator"
	defaultClientVersion = "0.1.0"

	defaultReadTimeout = 5 * time.Second
	defaultTurnTimeout = time.Hour

	maxToolFailureDiagnosticBytes = 2048
)

var (
	ErrResponseError            = errors.New("codex response error")
	ErrInvalidResponse          = errors.New("invalid codex response")
	ErrStreamStalled            = errors.New("codex stream stalled")
	ErrTurnFailed               = errors.New("codex turn failed")
	ErrTransportClose           = errors.New("close codex app-server transport")
	ErrSubscriptionAuthRequired = errors.New("codex ChatGPT subscription authentication is required")
)

const AuthenticationModeChatGPTSubscription = "chatgpt_subscription"

type ResponseError struct {
	Request string
	Code    int
	Message string
	Body    string
}

func (e *ResponseError) Error() string {
	request := strings.TrimSpace(e.Request)
	if request == "" {
		request = "response"
	}
	message := strings.TrimSpace(e.Message)
	if message == "" {
		message = "unknown error"
	}
	out := ErrResponseError.Error() + ": " + request + ": " + message
	if body := strings.TrimSpace(e.Body); body != "" {
		out += ": " + body
	}
	return out
}

func (e *ResponseError) Unwrap() error {
	return ErrResponseError
}

type TransportCloseError struct {
	Err error
}

func (e *TransportCloseError) Error() string {
	if e == nil || e.Err == nil {
		return ErrTransportClose.Error()
	}
	return ErrTransportClose.Error() + ": " + e.Err.Error()
}

func (e *TransportCloseError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *TransportCloseError) Is(target error) bool {
	return target == ErrTransportClose
}

func (e *ResponseError) BackendErrorBody() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.Body)
}

func (e *ResponseError) BackendErrorMessage() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.Message)
}

type TurnFailedError struct {
	Status  string
	Message string
	Body    string
}

func (e *TurnFailedError) Error() string {
	status := strings.TrimSpace(e.Status)
	if status == "" {
		status = "failed"
	}
	out := ErrTurnFailed.Error() + ": status " + status
	if body := strings.TrimSpace(e.Body); body != "" {
		out += ": " + body
	} else if message := strings.TrimSpace(e.Message); message != "" {
		out += ": " + message
	}
	return out
}

func (e *TurnFailedError) Unwrap() error {
	return ErrTurnFailed
}

func (e *TurnFailedError) BackendErrorBody() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.Body)
}

func (e *TurnFailedError) BackendErrorMessage() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.Message)
}

func (e *TurnFailedError) BackendErrorStatus() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.Status)
}

type AppServer struct {
	transportFactory TransportFactory
	clientInfo       ClientInfo
	logger           *slog.Logger
	readTimeout      time.Duration
	turnTimeout      time.Duration
	now              func() time.Time
	timeoutContext   timeoutContextFactory
}

type AppServerOption func(*AppServer)

type timeoutContextFactory func(context.Context, time.Duration, error) (context.Context, context.CancelFunc)

type ClientInfo struct {
	Name    string `json:"name"`
	Title   string `json:"title,omitempty"`
	Version string `json:"version"`
}

type RunTurnRequest struct {
	Workspace               string
	Prompt                  string
	ResumeThreadID          string
	DeveloperInstructions   string
	ApprovalPolicy          any
	MCPElicitationPolicy    MCPElicitationPolicy
	ThreadSandbox           string
	TurnSandboxPolicy       any
	Model                   string
	ModelProvider           string
	ServiceTier             string
	ReasoningEffort         string
	TurnTimeout             time.Duration
	StallTimeout            time.Duration
	DynamicTools            []DynamicTool
	ToolHandler             DynamicToolHandler
	RequireSubscriptionAuth bool
}

type RunTurnResult struct {
	ThreadID           string
	TurnID             string
	SessionID          string
	AuthenticationMode string
}

type DynamicTool struct {
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

type DynamicToolCall struct {
	Name      string
	Arguments json.RawMessage
}

type DynamicToolResult struct {
	Content string
	Success bool
}

type MCPElicitationRule struct {
	Server     string
	Tool       string
	Repository string
}

type MCPElicitationPolicy struct {
	DeliverableKind string
	Repository      string
	IssueRepository string
	Allowlist       []MCPElicitationRule
}

type DynamicToolHandler func(context.Context, DynamicToolCall) (DynamicToolResult, error)

type Model struct {
	ID                        string
	Model                     string
	Default                   bool
	Upgrade                   string
	SupportedReasoningEfforts []string
}

type Account struct {
	Type               string
	PlanType           string
	RequiresOpenAIAuth bool
}

func (a Account) SubscriptionBased() bool {
	if !strings.EqualFold(strings.TrimSpace(a.Type), "chatgpt") {
		return false
	}
	plan := strings.ToLower(strings.TrimSpace(a.PlanType))
	return plan != "self_serve_business_usage_based" && plan != "enterprise_cbp_usage_based"
}

type UpdateHandler func(Update) error

type UpdateType string

const (
	UpdateProcessStarted    UpdateType = "process_started"
	UpdateProviderIdentity  UpdateType = "provider_identity"
	UpdateAgentMessageDelta UpdateType = "agent_message_delta"
	UpdateTokenUsage        UpdateType = "token_usage"
	UpdateRateLimits        UpdateType = "rate_limits"
	UpdateTurnStarted       UpdateType = "turn_started"
	UpdateTurnCompleted     UpdateType = "turn_completed"
	UpdateModelUpdated      UpdateType = "model_updated"
	UpdateRuntimeIdentity   UpdateType = "runtime_identity"
	UpdateToolStarted       UpdateType = "tool_started"
	UpdateToolOutput        UpdateType = "tool_output"
	UpdateToolCompleted     UpdateType = "tool_completed"
	UpdateMCPElicitation    UpdateType = "mcp_elicitation"
)

type Update struct {
	Type                UpdateType
	Method              string
	ProcessIdentity     string
	WorkerProcess       procgroup.Identity
	ThreadID            string
	TurnID              string
	AuxiliaryTurn       bool
	ItemID              string
	Tool                string
	Command             string
	Delta               string
	Status              string
	ExitCode            *int
	Model               string
	RuntimeIdentity     agentidentity.Identity
	BackendErrorBody    string
	BackendErrorMessage string
	Tokens              TokenUsage
	RateLimits          *RateLimitSnapshot
	Payload             json.RawMessage
}

type TokenUsage struct {
	InputTokens           int64
	CachedInputTokens     int64
	OutputTokens          int64
	ReasoningOutputTokens int64
	TotalTokens           int64
	Last                  *TokenUsageBreakdown
	ModelContextWindow    *int64
}

type TokenUsageBreakdown struct {
	InputTokens           int64 `json:"inputTokens"`
	CachedInputTokens     int64 `json:"cachedInputTokens"`
	OutputTokens          int64 `json:"outputTokens"`
	ReasoningOutputTokens int64 `json:"reasoningOutputTokens"`
	TotalTokens           int64 `json:"totalTokens"`
}

type RateLimitSnapshot struct {
	LimitID              string
	LimitName            string
	Primary              *RateLimitWindow
	Secondary            *RateLimitWindow
	Credits              *CreditsSnapshot
	RateLimitReachedType string
}

type RateLimitWindow struct {
	UsedPercent        float64
	WindowDurationMins *float64
	ResetsAt           *int64
}

type CreditsSnapshot struct {
	HasCredits bool
	Unlimited  bool
	Balance    string
}

func NewAppServer(factory TransportFactory, opts ...AppServerOption) (*AppServer, error) {
	if factory == nil {
		return nil, errors.New("transport factory is nil")
	}

	server := &AppServer{
		transportFactory: factory,
		clientInfo: ClientInfo{
			Name:    defaultClientName,
			Title:   defaultClientTitle,
			Version: defaultClientVersion,
		},
		logger:      slog.Default(),
		readTimeout: defaultReadTimeout,
		turnTimeout: defaultTurnTimeout,
		now:         time.Now,
		timeoutContext: func(ctx context.Context, timeout time.Duration, cause error) (context.Context, context.CancelFunc) {
			if cause != nil {
				return context.WithTimeoutCause(ctx, timeout, cause)
			}
			return context.WithTimeout(ctx, timeout)
		},
	}

	for _, opt := range opts {
		opt(server)
	}
	if server.clientInfo.Name == "" {
		server.clientInfo.Name = defaultClientName
	}
	if server.clientInfo.Version == "" {
		server.clientInfo.Version = defaultClientVersion
	}
	if server.logger == nil {
		server.logger = slog.Default()
	}
	if server.readTimeout <= 0 {
		server.readTimeout = defaultReadTimeout
	}
	if server.turnTimeout <= 0 {
		server.turnTimeout = defaultTurnTimeout
	}
	if server.now == nil {
		server.now = time.Now
	}
	if server.timeoutContext == nil {
		return nil, errors.New("timeout context factory is nil")
	}

	return server, nil
}

func WithClientInfo(info ClientInfo) AppServerOption {
	return func(server *AppServer) {
		server.clientInfo = info
	}
}

func WithLogger(logger *slog.Logger) AppServerOption {
	return func(server *AppServer) {
		server.logger = logger
	}
}

func WithReadTimeout(timeout time.Duration) AppServerOption {
	return func(server *AppServer) {
		server.readTimeout = timeout
	}
}

func WithTurnTimeout(timeout time.Duration) AppServerOption {
	return func(server *AppServer) {
		server.turnTimeout = timeout
	}
}

func (s *AppServer) RunTurn(ctx context.Context, req RunTurnRequest, onUpdate UpdateHandler) (result RunTurnResult, err error) {
	ctx = contextOrBackground(ctx)

	transport, err := s.transportFactory.NewTransport(ctx)
	if err != nil {
		now := s.now()
		return RunTurnResult{}, startupStageError(fmt.Errorf("start codex app-server transport: %w", err), "process/start", now, now, 0)
	}
	defer func() {
		closeErr := closeTransport(ctx, transport, s.readTimeout)
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		attachStartupProcessEvidence(err, transport)
	}()

	if identity := transportProcessIdentity(transport); identity != "" {
		if err := emitUpdate(Update{
			Type:            UpdateProcessStarted,
			Method:          "process/start",
			ProcessIdentity: identity,
			WorkerProcess:   transportWorkerProcess(transport),
		}, onUpdate); err != nil {
			return RunTurnResult{}, err
		}
	}

	if err := s.initialize(ctx, transport, onUpdate); err != nil {
		return RunTurnResult{}, err
	}
	authenticationMode := ""
	if req.RequireSubscriptionAuth {
		account, err := s.account(ctx, transport)
		if err != nil {
			return RunTurnResult{}, fmt.Errorf("verify codex subscription authentication: %w", err)
		}
		if !account.SubscriptionBased() {
			return RunTurnResult{}, fmt.Errorf("%w: account_type=%s plan_type=%s", ErrSubscriptionAuthRequired, account.Type, account.PlanType)
		}
		authenticationMode = AuthenticationModeChatGPTSubscription
	}

	threadID := strings.TrimSpace(req.ResumeThreadID)
	var runtimeIdentity agentidentity.Identity
	if threadID != "" {
		threadID, runtimeIdentity, err = s.resumeThread(ctx, transport, req, threadID, onUpdate)
		if err != nil {
			return RunTurnResult{}, err
		}
	} else {
		threadID, runtimeIdentity, err = s.startThread(ctx, transport, req, onUpdate)
		if err != nil {
			return RunTurnResult{}, err
		}
	}
	model := runtimeIdentity.Model()
	if model == "" && strings.TrimSpace(req.Model) == "" {
		model, err = s.resolveDefaultModel(ctx, transport, req.Workspace, onUpdate)
		if err != nil {
			return RunTurnResult{}, err
		}
		if model != "" {
			runtimeIdentity = runtimeIdentity.Merge(agentidentity.Identity{
				ResolvedModel: agentidentity.NewValue(model, agentidentity.ProvenanceConfigured),
			})
			if err := emitUpdate(Update{
				Type:     UpdateRuntimeIdentity,
				Method:   "config/read",
				ThreadID: threadID,
				Model:    model,
				RuntimeIdentity: agentidentity.Identity{
					ResolvedModel: agentidentity.NewValue(model, agentidentity.ProvenanceConfigured),
				},
			}, onUpdate); err != nil {
				return RunTurnResult{}, err
			}
		}
	}

	elicitationState := newMCPElicitationState(req.MCPElicitationPolicy, onUpdate)
	turn, err := s.startTurn(ctx, transport, req, threadID, elicitationState, onUpdate)
	if err != nil {
		return RunTurnResult{}, err
	}
	turnID := turn.ID

	result = RunTurnResult{
		ThreadID:           threadID,
		TurnID:             turnID,
		SessionID:          threadID + "-" + turnID,
		AuthenticationMode: authenticationMode,
	}
	if !turn.StartedEmitted {
		startedIdentity := runtimeIdentity.Merge(turn.RuntimeIdentity)
		if turn.Model != "" {
			startedIdentity = startedIdentity.Merge(agentidentity.RuntimeUpdate(
				turn.Model,
				"",
				"",
				"",
				time.Time{},
			))
		}
		if err := emitUpdate(Update{
			Type:            UpdateTurnStarted,
			Method:          "turn/start",
			ThreadID:        threadID,
			TurnID:          turnID,
			Status:          "started",
			Model:           startedIdentity.Model(),
			RuntimeIdentity: startedIdentity,
		}, onUpdate); err != nil {
			return RunTurnResult{}, err
		}
	}

	if err := s.streamTurn(ctx, transport, threadID, turnID, req.turnTimeout(s.turnTimeout), req.StallTimeout, req.ToolHandler, elicitationState, onUpdate); err != nil {
		return RunTurnResult{}, err
	}

	return result, nil
}

func (s *AppServer) CheckHealth(ctx context.Context) (err error) {
	ctx = contextOrBackground(ctx)
	transport, err := s.transportFactory.NewTransport(ctx)
	if err != nil {
		return fmt.Errorf("start codex app-server transport: %w", err)
	}
	defer func() {
		closeErr := closeTransport(ctx, transport, s.readTimeout)
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	return s.initialize(ctx, transport, nil)
}

func (s *AppServer) Account(ctx context.Context) (account Account, err error) {
	ctx = contextOrBackground(ctx)
	transport, err := s.transportFactory.NewTransport(ctx)
	if err != nil {
		return Account{}, fmt.Errorf("start codex app-server transport: %w", err)
	}
	defer func() {
		closeErr := closeTransport(ctx, transport, s.readTimeout)
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	if err := s.initialize(ctx, transport, nil); err != nil {
		return Account{}, err
	}
	return s.account(ctx, transport)
}

func (s *AppServer) account(ctx context.Context, transport Transport) (account Account, err error) {
	if err := sendRequest(ctx, transport, accountReadRequestID, "account/read", map[string]any{"refreshToken": false}); err != nil {
		return Account{}, err
	}
	result, err := s.awaitResponse(ctx, transport, accountReadRequestID, nil, nil, nil)
	if err != nil {
		return Account{}, err
	}
	var response struct {
		Account *struct {
			Type     string `json:"type"`
			PlanType string `json:"planType"`
		} `json:"account"`
		RequiresOpenAIAuth bool `json:"requiresOpenaiAuth"`
	}
	if err := json.Unmarshal(result, &response); err != nil {
		return Account{}, fmt.Errorf("%w: decode account/read result: %w", ErrInvalidResponse, err)
	}
	account.RequiresOpenAIAuth = response.RequiresOpenAIAuth
	if response.Account != nil {
		account.Type = strings.TrimSpace(response.Account.Type)
		account.PlanType = strings.TrimSpace(response.Account.PlanType)
	}
	return account, nil
}

func (s *AppServer) ListModels(ctx context.Context) (models []Model, err error) {
	ctx = contextOrBackground(ctx)
	transport, err := s.transportFactory.NewTransport(ctx)
	if err != nil {
		return nil, fmt.Errorf("start codex app-server transport: %w", err)
	}
	defer func() {
		closeErr := closeTransport(ctx, transport, s.readTimeout)
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	if err := s.initialize(ctx, transport, nil); err != nil {
		return nil, err
	}

	cursor := ""
	for {
		params := map[string]any{
			"includeHidden": true,
			"limit":         100,
		}
		if cursor != "" {
			params["cursor"] = cursor
		}
		if err := sendRequest(ctx, transport, modelListRequestID, "model/list", params); err != nil {
			return nil, err
		}
		result, err := s.awaitResponse(ctx, transport, modelListRequestID, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		var response struct {
			Data []struct {
				ID                        string `json:"id"`
				Model                     string `json:"model"`
				Default                   bool   `json:"isDefault"`
				Upgrade                   string `json:"upgrade"`
				SupportedReasoningEfforts []struct {
					Effort string `json:"reasoningEffort"`
				} `json:"supportedReasoningEfforts"`
			} `json:"data"`
			NextCursor string `json:"nextCursor"`
		}
		if err := json.Unmarshal(result, &response); err != nil {
			return nil, fmt.Errorf("%w: decode model/list result: %w", ErrInvalidResponse, err)
		}
		for _, entry := range response.Data {
			model := Model{
				ID:      strings.TrimSpace(entry.ID),
				Model:   strings.TrimSpace(entry.Model),
				Default: entry.Default,
				Upgrade: strings.TrimSpace(entry.Upgrade),
			}
			for _, option := range entry.SupportedReasoningEfforts {
				if effort := strings.TrimSpace(option.Effort); effort != "" {
					model.SupportedReasoningEfforts = append(model.SupportedReasoningEfforts, effort)
				}
			}
			models = append(models, model)
		}
		cursor = strings.TrimSpace(response.NextCursor)
		if cursor == "" {
			return models, nil
		}
	}
}

func (s *AppServer) DefaultModel(ctx context.Context, workspace string) (model string, err error) {
	ctx = contextOrBackground(ctx)
	transport, err := s.transportFactory.NewTransport(ctx)
	if err != nil {
		return "", fmt.Errorf("start codex app-server transport: %w", err)
	}
	defer func() {
		closeErr := closeTransport(ctx, transport, s.readTimeout)
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	if err := s.initialize(ctx, transport, nil); err != nil {
		return "", err
	}
	return s.resolveDefaultModel(ctx, transport, workspace, nil)
}

func (s *AppServer) VerifyThread(ctx context.Context, threadID string) (err error) {
	ctx = contextOrBackground(ctx)
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return errors.New("codex thread id is required")
	}
	transport, err := s.transportFactory.NewTransport(ctx)
	if err != nil {
		return fmt.Errorf("start codex app-server transport: %w", err)
	}
	defer func() {
		closeErr := closeTransport(ctx, transport, s.readTimeout)
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	if err := s.initialize(ctx, transport, nil); err != nil {
		return err
	}
	params := map[string]any{
		"threadId":     threadID,
		"includeTurns": true,
	}
	if err := sendRequest(ctx, transport, threadReadRequestID, "thread/read", params); err != nil {
		return err
	}
	result, err := s.awaitResponse(ctx, transport, threadReadRequestID, nil, nil, nil)
	if err != nil {
		return err
	}
	var response threadRuntimeResponse
	if err := json.Unmarshal(result, &response); err != nil {
		return fmt.Errorf("%w: decode thread/read result: %w", ErrInvalidResponse, err)
	}
	if strings.TrimSpace(response.Thread.ID) != threadID {
		return fmt.Errorf("%w: thread/read result missing requested thread id", ErrInvalidResponse)
	}
	return nil
}

func (s *AppServer) initialize(ctx context.Context, transport Transport, onUpdate UpdateHandler) (err error) {
	startedAt := s.now()
	defer func() {
		err = startupStageError(err, "initialize", startedAt, s.now(), s.readTimeout)
	}()
	params := map[string]any{
		"capabilities": map[string]any{
			"experimentalApi": true,
		},
		"clientInfo": s.clientInfo,
	}

	if err := sendRequest(ctx, transport, initializeRequestID, "initialize", params); err != nil {
		return err
	}
	if _, err := s.awaitResponse(ctx, transport, initializeRequestID, nil, nil, onUpdate); err != nil {
		return err
	}
	markTransportReady(transport, s.now())

	return transport.Send(ctx, Message{
		Method: "initialized",
		Params: json.RawMessage(`{}`),
	})
}

type threadRuntimeResponse struct {
	Thread struct {
		ID            string `json:"id"`
		Model         string `json:"model"`
		ModelID       string `json:"model_id"`
		ModelIDCamel  string `json:"modelId"`
		ResolvedModel string `json:"resolvedModel"`
		ResolvedSnake string `json:"resolved_model"`
		ModelProvider string `json:"modelProvider"`
		ProviderSnake string `json:"model_provider"`
	} `json:"thread"`
	Model            string `json:"model"`
	ModelID          string `json:"model_id"`
	ModelIDCamel     string `json:"modelId"`
	ResolvedModel    string `json:"resolvedModel"`
	ResolvedSnake    string `json:"resolved_model"`
	ModelProvider    string `json:"modelProvider"`
	ProviderSnake    string `json:"model_provider"`
	ReasoningEffort  string `json:"reasoningEffort"`
	EffortSnake      string `json:"reasoning_effort"`
	ServiceTier      string `json:"serviceTier"`
	ServiceTierSnake string `json:"service_tier"`
}

func (r threadRuntimeResponse) runtimeIdentity() agentidentity.Identity {
	return agentidentity.RuntimeUpdate(
		firstNonBlank(r.Model, r.ResolvedModel, r.ResolvedSnake, r.ModelID, r.ModelIDCamel, r.Thread.Model, r.Thread.ResolvedModel, r.Thread.ResolvedSnake, r.Thread.ModelID, r.Thread.ModelIDCamel),
		firstNonBlank(r.ModelProvider, r.ProviderSnake, r.Thread.ModelProvider, r.Thread.ProviderSnake),
		firstNonBlank(r.ReasoningEffort, r.EffortSnake),
		firstNonBlank(r.ServiceTier, r.ServiceTierSnake),
		time.Time{},
	)
}

func runtimeIdentityUpdate(method string, threadID string, identity agentidentity.Identity) Update {
	return Update{
		Type:            UpdateRuntimeIdentity,
		Method:          method,
		ThreadID:        threadID,
		Model:           identity.Model(),
		RuntimeIdentity: identity,
	}
}

func providerIdentityUpdate(method string, threadID string) Update {
	return Update{
		Type:     UpdateProviderIdentity,
		Method:   method,
		ThreadID: threadID,
	}
}

func (s *AppServer) startThread(
	ctx context.Context,
	transport Transport,
	req RunTurnRequest,
	onUpdate UpdateHandler,
) (threadID string, runtimeIdentity agentidentity.Identity, err error) {
	startedAt := s.now()
	startupComplete := false
	defer func() {
		if !startupComplete {
			err = startupStageError(err, "thread/start", startedAt, s.now(), s.readTimeout)
		}
	}()
	params := map[string]any{
		"cwd": req.Workspace,
	}
	setOptional(params, "approvalPolicy", req.ApprovalPolicy)
	if req.DeveloperInstructions != "" {
		params["developerInstructions"] = req.DeveloperInstructions
	}
	if len(req.DynamicTools) > 0 {
		params["dynamicTools"] = req.DynamicTools
	}
	if req.ThreadSandbox != "" {
		params["sandbox"] = req.ThreadSandbox
	}
	if req.Model != "" {
		params["model"] = req.Model
	}
	if req.ModelProvider != "" {
		params["modelProvider"] = req.ModelProvider
	}
	if req.ServiceTier != "" {
		params["serviceTier"] = req.ServiceTier
	}
	if err := sendRequest(ctx, transport, threadStartRequestID, "thread/start", params); err != nil {
		return "", agentidentity.Identity{}, err
	}

	result, err := s.awaitResponse(ctx, transport, threadStartRequestID, req.ToolHandler, nil, onUpdate)
	if err != nil {
		return "", agentidentity.Identity{}, err
	}

	var response threadRuntimeResponse
	if err := json.Unmarshal(result, &response); err != nil {
		return "", agentidentity.Identity{}, fmt.Errorf("%w: decode thread/start result: %w", ErrInvalidResponse, err)
	}
	if response.Thread.ID == "" {
		return "", agentidentity.Identity{}, fmt.Errorf("%w: thread/start result missing thread id", ErrInvalidResponse)
	}
	identity := response.runtimeIdentity()
	if identity.Model() == "" && req.Model != "" {
		identity.ResolvedModel = agentidentity.NewValue(req.Model, agentidentity.ProvenanceConfigured)
	}
	update := providerIdentityUpdate("thread/start", response.Thread.ID)
	if !identity.IsZero() {
		update = runtimeIdentityUpdate("thread/start", response.Thread.ID, identity)
	}
	startupComplete = true
	if err := emitUpdate(update, onUpdate); err != nil {
		return "", agentidentity.Identity{}, err
	}
	return response.Thread.ID, identity, nil
}

func (s *AppServer) resumeThread(
	ctx context.Context,
	transport Transport,
	req RunTurnRequest,
	threadID string,
	onUpdate UpdateHandler,
) (resumedThreadID string, runtimeIdentity agentidentity.Identity, err error) {
	startedAt := s.now()
	startupComplete := false
	defer func() {
		if !startupComplete {
			err = startupStageError(err, "thread/resume", startedAt, s.now(), s.readTimeout)
		}
	}()
	params := map[string]any{
		"threadId": threadID,
		"cwd":      req.Workspace,
	}
	setOptional(params, "approvalPolicy", req.ApprovalPolicy)
	if req.DeveloperInstructions != "" {
		params["developerInstructions"] = req.DeveloperInstructions
	}
	if req.ThreadSandbox != "" {
		params["sandbox"] = req.ThreadSandbox
	}
	if req.Model != "" {
		params["model"] = req.Model
	}
	if req.ModelProvider != "" {
		params["modelProvider"] = req.ModelProvider
	}
	if req.ServiceTier != "" {
		params["serviceTier"] = req.ServiceTier
	}

	if err := sendRequest(ctx, transport, threadResumeRequestID, "thread/resume", params); err != nil {
		return "", agentidentity.Identity{}, err
	}

	result, err := s.awaitResponse(ctx, transport, threadResumeRequestID, req.ToolHandler, nil, onUpdate)
	if err != nil {
		return "", agentidentity.Identity{}, err
	}

	var response threadRuntimeResponse
	if err := json.Unmarshal(result, &response); err != nil {
		return "", agentidentity.Identity{}, fmt.Errorf("%w: decode thread/resume result: %w", ErrInvalidResponse, err)
	}
	if response.Thread.ID == "" {
		return "", agentidentity.Identity{}, fmt.Errorf("%w: thread/resume result missing thread id", ErrInvalidResponse)
	}
	identity := response.runtimeIdentity()
	if identity.Model() == "" && req.Model != "" {
		identity.ResolvedModel = agentidentity.NewValue(req.Model, agentidentity.ProvenanceConfigured)
	}
	update := providerIdentityUpdate("thread/resume", response.Thread.ID)
	if !identity.IsZero() {
		update = runtimeIdentityUpdate("thread/resume", response.Thread.ID, identity)
	}
	startupComplete = true
	if err := emitUpdate(update, onUpdate); err != nil {
		return "", agentidentity.Identity{}, err
	}
	return response.Thread.ID, identity, nil
}

type startTurnResult struct {
	ID              string
	Model           string
	RuntimeIdentity agentidentity.Identity
	StartedEmitted  bool
}

func (s *AppServer) startTurn(
	ctx context.Context,
	transport Transport,
	req RunTurnRequest,
	threadID string,
	elicitationState *mcpElicitationState,
	onUpdate UpdateHandler,
) (turn startTurnResult, err error) {
	startedAt := s.now()
	defer func() {
		err = startupStageError(err, "turn/start", startedAt, s.now(), s.readTimeout)
	}()
	params := map[string]any{
		"threadId": threadID,
		"input": []map[string]any{
			{
				"type":          "text",
				"text":          req.Prompt,
				"text_elements": []any{},
			},
		},
		"cwd": req.Workspace,
	}
	setOptional(params, "approvalPolicy", req.ApprovalPolicy)
	setOptional(params, "sandboxPolicy", req.TurnSandboxPolicy)
	if req.Model != "" {
		params["model"] = req.Model
	}
	if req.ServiceTier != "" {
		params["serviceTier"] = req.ServiceTier
	}
	if req.ReasoningEffort != "" {
		params["effort"] = req.ReasoningEffort
	}

	if err := sendRequest(ctx, transport, turnStartRequestID, "turn/start", params); err != nil {
		return startTurnResult{}, err
	}

	trackTurnStarted := func(update Update) error {
		turn.RuntimeIdentity = turn.RuntimeIdentity.Merge(update.RuntimeIdentity)
		if update.Type == UpdateTurnStarted {
			turn.StartedEmitted = true
			if update.Model != "" {
				turn.Model = update.Model
			}
		}
		if onUpdate == nil {
			return nil
		}
		return onUpdate(update)
	}

	result, err := s.awaitResponse(ctx, transport, turnStartRequestID, req.ToolHandler, elicitationState, trackTurnStarted)
	if err != nil {
		return startTurnResult{}, err
	}

	var response struct {
		Turn struct {
			ID            string `json:"id"`
			Model         string `json:"model"`
			ModelID       string `json:"model_id"`
			ModelIDCamel  string `json:"modelId"`
			ResolvedModel string `json:"resolvedModel"`
			ResolvedSnake string `json:"resolved_model"`
		} `json:"turn"`
		Model         string `json:"model"`
		ModelID       string `json:"model_id"`
		ModelIDCamel  string `json:"modelId"`
		ResolvedModel string `json:"resolvedModel"`
		ResolvedSnake string `json:"resolved_model"`
	}
	if err := json.Unmarshal(result, &response); err != nil {
		return startTurnResult{}, fmt.Errorf("%w: decode turn/start result: %w", ErrInvalidResponse, err)
	}
	if response.Turn.ID == "" {
		return startTurnResult{}, fmt.Errorf("%w: turn/start result missing turn id", ErrInvalidResponse)
	}

	turn.ID = response.Turn.ID
	turn.Model = firstNonBlank(
		turn.Model,
		response.Turn.Model,
		response.Turn.ResolvedModel,
		response.Turn.ResolvedSnake,
		response.Turn.ModelID,
		response.Turn.ModelIDCamel,
		response.Model,
		response.ResolvedModel,
		response.ResolvedSnake,
		response.ModelID,
		response.ModelIDCamel,
	)
	return turn, nil
}

func (s *AppServer) resolveDefaultModel(
	ctx context.Context,
	transport Transport,
	workspace string,
	onUpdate UpdateHandler,
) (string, error) {
	params := map[string]any{
		"includeLayers": false,
	}
	if strings.TrimSpace(workspace) != "" {
		params["cwd"] = workspace
	}
	if err := sendRequest(ctx, transport, configReadRequestID, "config/read", params); err != nil {
		return "", err
	}
	result, err := s.awaitResponse(ctx, transport, configReadRequestID, nil, nil, onUpdate)
	if err != nil {
		var responseErr *ResponseError
		if errors.As(err, &responseErr) {
			return "", nil
		}
		return "", err
	}
	var response struct {
		Config struct {
			Model string `json:"model"`
		} `json:"config"`
		Model string `json:"model"`
	}
	if err := json.Unmarshal(result, &response); err != nil {
		return "", fmt.Errorf("%w: decode config/read result: %w", ErrInvalidResponse, err)
	}
	return firstNonBlank(response.Config.Model, response.Model), nil
}

func (s *AppServer) awaitResponse(
	ctx context.Context,
	transport Transport,
	requestID int,
	toolHandler DynamicToolHandler,
	elicitationState *mcpElicitationState,
	onUpdate UpdateHandler,
) (json.RawMessage, error) {
	for {
		msg, err := receiveWithTimeout(ctx, transport, s.readTimeout, s.timeoutContext)
		if err != nil {
			return nil, fmt.Errorf("wait for %s response: %w", requestName(requestID), err)
		}

		if requestIDMatches(msg.ID, requestID) {
			if msg.Error != nil {
				return nil, &ResponseError{
					Request: requestName(requestID),
					Code:    msg.Error.Code,
					Message: msg.Error.Message,
					Body:    string(rawPayload(msg)),
				}
			}
			if len(msg.Result) == 0 {
				return nil, fmt.Errorf("%w: %s response missing result", ErrInvalidResponse, requestName(requestID))
			}
			return msg.Result, nil
		}

		if err := elicitationState.observe(msg); err != nil {
			return nil, err
		}
		handled, err := handleServerRequest(ctx, transport, msg, toolHandler, elicitationState, s.logger)
		if err != nil {
			return nil, err
		}
		if handled {
			continue
		}
		if err := maybeEmitUpdate(msg, onUpdate); err != nil {
			return nil, err
		}
	}
}

func (r RunTurnRequest) turnTimeout(fallback time.Duration) time.Duration {
	if r.TurnTimeout > 0 {
		return r.TurnTimeout
	}
	return fallback
}

func (s *AppServer) streamTurn(
	ctx context.Context,
	transport Transport,
	threadID string,
	turnID string,
	turnTimeout time.Duration,
	stallTimeout time.Duration,
	toolHandler DynamicToolHandler,
	elicitationState *mcpElicitationState,
	onUpdate UpdateHandler,
) error {
	for {
		msg, err := receiveTurnMessage(ctx, transport, turnTimeout, stallTimeout, s.timeoutContext)
		if err != nil {
			return fmt.Errorf("stream turn: %w", err)
		}

		if err := elicitationState.observe(msg); err != nil {
			return err
		}
		handled, err := handleServerRequest(ctx, transport, msg, toolHandler, elicitationState, s.logger)
		if err != nil {
			return err
		}
		if handled {
			continue
		}

		update, ok, err := updateFromMessage(msg)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		update.AuxiliaryTurn = !turnUpdateMatches(update, threadID, turnID)
		if err := emitUpdate(update, onUpdate); err != nil {
			return err
		}
		if update.Type != UpdateTurnCompleted || update.AuxiliaryTurn {
			continue
		}
		if update.Status == "" || update.Status == "completed" {
			return nil
		}
		return &TurnFailedError{
			Status:  update.Status,
			Message: update.BackendErrorMessage,
			Body:    update.BackendErrorBody,
		}
	}
}

func turnUpdateMatches(update Update, threadID string, turnID string) bool {
	if updateThreadID := strings.TrimSpace(update.ThreadID); updateThreadID != "" && updateThreadID != strings.TrimSpace(threadID) {
		return false
	}
	if updateTurnID := strings.TrimSpace(update.TurnID); updateTurnID != "" && updateTurnID != strings.TrimSpace(turnID) {
		return false
	}
	return true
}

func receiveTurnMessage(
	ctx context.Context,
	transport Transport,
	turnTimeout time.Duration,
	stallTimeout time.Duration,
	timeoutContext timeoutContextFactory,
) (Message, error) {
	if stallTimeout <= 0 || turnTimeout > 0 && turnTimeout < stallTimeout {
		return receiveWithTimeout(ctx, transport, turnTimeout, timeoutContext)
	}

	stallErr := fmt.Errorf("%w after %s", ErrStreamStalled, stallTimeout)
	receiveCtx, cancel := timeoutContext(contextOrBackground(ctx), stallTimeout, stallErr)
	defer cancel()

	msg, err := transport.Receive(receiveCtx)
	if err != nil && errors.Is(context.Cause(receiveCtx), ErrStreamStalled) {
		return Message{}, context.Cause(receiveCtx)
	}
	return msg, err
}

func sendRequest(ctx context.Context, transport Transport, id int, method string, params any) error {
	data, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("marshal %s params: %w", method, err)
	}

	return transport.Send(ctx, Message{
		ID:     requestID(id),
		Method: method,
		Params: data,
	})
}

func handleServerRequest(
	ctx context.Context,
	transport Transport,
	msg Message,
	toolHandler DynamicToolHandler,
	elicitationState *mcpElicitationState,
	logger *slog.Logger,
) (bool, error) {
	if msg.Method == "" || len(msg.ID) == 0 {
		return false, nil
	}

	response := Message{
		ID: msg.ID,
	}
	result, ok, err := serverRequestResult(ctx, msg, toolHandler, elicitationState, logger)
	if err != nil {
		return true, err
	}
	if ok {
		response.Result = result
	} else {
		response.Error = &RPCError{
			Code:    methodNotFoundCode,
			Message: "unsupported server request: " + msg.Method,
		}
	}

	if err := transport.Send(ctx, response); err != nil {
		return true, fmt.Errorf("respond to codex server request %s: %w", msg.Method, err)
	}
	return true, nil
}

func serverRequestResult(
	ctx context.Context,
	msg Message,
	toolHandler DynamicToolHandler,
	elicitationState *mcpElicitationState,
	logger *slog.Logger,
) (json.RawMessage, bool, error) {
	var result any
	switch msg.Method {
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval":
		result = map[string]string{"decision": "decline"}
	case "item/permissions/requestApproval":
		result = map[string]any{"permissions": map[string]any{}}
	case "item/tool/requestUserInput":
		result = map[string]any{"answers": map[string]any{}}
	case "mcpServer/elicitation/request":
		response, decision := mcpElicitationResponse(msg.Params, elicitationState)
		logMCPElicitationDecision(ctx, logger, decision)
		if decision.Action != "accept" {
			if err := elicitationState.publishDecline(decision); err != nil {
				return nil, true, err
			}
		}
		result = response
	case "item/tool/call":
		if toolHandler == nil {
			result = map[string]any{
				"contentItems": []map[string]string{{"type": "inputText", "text": "dynamic tool handler unavailable"}},
				"success":      false,
			}
			break
		}
		var params struct {
			Tool      string          `json:"tool"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			result = dynamicToolResponse("decode dynamic tool call: "+err.Error(), false)
			break
		}
		toolResult, err := toolHandler(ctx, DynamicToolCall{Name: params.Tool, Arguments: params.Arguments})
		if err != nil {
			toolResult = DynamicToolResult{Content: err.Error(), Success: false}
		}
		result = dynamicToolResponse(toolResult.Content, toolResult.Success)
	default:
		return nil, false, nil
	}

	data, err := json.Marshal(result)
	if err != nil {
		return nil, true, fmt.Errorf("marshal %s server request response: %w", msg.Method, err)
	}
	return data, true, nil
}

type mcpElicitationDecision struct {
	Server       string
	Mode         string
	ApprovalKind string
	ThreadID     string
	TurnID       string
	ItemID       string
	Tool         string
	Repository   string
	Action       string
	Reason       string
}

type pendingMCPToolCall struct {
	ThreadID  string
	TurnID    string
	ItemID    string
	Server    string
	Tool      string
	Arguments json.RawMessage
}

type mcpElicitationState struct {
	policy   MCPElicitationPolicy
	pending  map[string]pendingMCPToolCall
	onUpdate UpdateHandler
}

func newMCPElicitationState(policy MCPElicitationPolicy, onUpdate UpdateHandler) *mcpElicitationState {
	return &mcpElicitationState{
		policy: MCPElicitationPolicy{
			DeliverableKind: strings.TrimSpace(policy.DeliverableKind),
			Repository:      strings.TrimSpace(policy.Repository),
			IssueRepository: strings.TrimSpace(policy.IssueRepository),
			Allowlist:       append([]MCPElicitationRule(nil), policy.Allowlist...),
		},
		pending:  map[string]pendingMCPToolCall{},
		onUpdate: onUpdate,
	}
}

func (s *mcpElicitationState) observe(msg Message) error {
	if s == nil || msg.Method != "item/started" && msg.Method != "item/completed" {
		return nil
	}
	var params struct {
		ThreadID string `json:"threadId"`
		TurnID   string `json:"turnId"`
		Item     struct {
			ID        string          `json:"id"`
			Type      string          `json:"type"`
			Server    string          `json:"server"`
			Tool      string          `json:"tool"`
			Arguments json.RawMessage `json:"arguments"`
		} `json:"item"`
	}
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return fmt.Errorf("%w: decode MCP tool lifecycle correlation: %w", ErrInvalidResponse, err)
	}
	if params.Item.Type != "mcpToolCall" {
		return nil
	}
	key := mcpToolCallKey(params.ThreadID, params.TurnID, params.Item.ID)
	if key == "" {
		return nil
	}
	if msg.Method == "item/completed" {
		delete(s.pending, key)
		return nil
	}
	s.pending[key] = pendingMCPToolCall{
		ThreadID:  strings.TrimSpace(params.ThreadID),
		TurnID:    strings.TrimSpace(params.TurnID),
		ItemID:    strings.TrimSpace(params.Item.ID),
		Server:    strings.TrimSpace(params.Item.Server),
		Tool:      strings.TrimSpace(params.Item.Tool),
		Arguments: append(json.RawMessage(nil), params.Item.Arguments...),
	}
	return nil
}

func mcpToolCallKey(threadID string, turnID string, itemID string) string {
	threadID = strings.TrimSpace(threadID)
	turnID = strings.TrimSpace(turnID)
	itemID = strings.TrimSpace(itemID)
	if threadID == "" || turnID == "" || itemID == "" {
		return ""
	}
	return threadID + "\x00" + turnID + "\x00" + itemID
}

func (s *mcpElicitationState) correlate(server string, threadID string, turnID string) []pendingMCPToolCall {
	if s == nil {
		return nil
	}
	server = strings.TrimSpace(server)
	threadID = strings.TrimSpace(threadID)
	turnID = strings.TrimSpace(turnID)
	if server == "" || threadID == "" || turnID == "" {
		return nil
	}
	var matches []pendingMCPToolCall
	for _, call := range s.pending {
		if call.Server == server && call.ThreadID == threadID && call.TurnID == turnID {
			matches = append(matches, call)
		}
	}
	return matches
}

func (s *mcpElicitationState) hasServerRule(server string) bool {
	if s == nil {
		return false
	}
	for _, rule := range s.policy.Allowlist {
		if strings.TrimSpace(rule.Server) == server {
			return true
		}
	}
	return false
}

func (s *mcpElicitationState) hasToolRule(server string, tool string) bool {
	if s == nil {
		return false
	}
	for _, rule := range s.policy.Allowlist {
		if strings.TrimSpace(rule.Server) == server && strings.TrimSpace(rule.Tool) == tool {
			return true
		}
	}
	return false
}

func (s *mcpElicitationState) hasRepositoryRule(server string, tool string, repository string) bool {
	if s == nil {
		return false
	}
	for _, rule := range s.policy.Allowlist {
		if strings.TrimSpace(rule.Server) == server && strings.TrimSpace(rule.Tool) == tool &&
			strings.EqualFold(strings.TrimSpace(rule.Repository), repository) {
			return true
		}
	}
	return false
}

func (s *mcpElicitationState) publishDecline(decision mcpElicitationDecision) error {
	if s == nil || s.onUpdate == nil {
		return nil
	}
	tool := strings.TrimSpace(decision.Tool)
	if decision.Server != "" && tool != "" {
		tool = decision.Server + "/" + tool
	}
	return emitUpdate(Update{
		Type:     UpdateMCPElicitation,
		Method:   "mcpServer/elicitation/request",
		ThreadID: decision.ThreadID,
		TurnID:   decision.TurnID,
		ItemID:   decision.ItemID,
		Tool:     tool,
		Delta:    mcpElicitationActivityContent(decision),
		Status:   decision.Action,
	}, s.onUpdate)
}

func mcpElicitationActivityContent(decision mcpElicitationDecision) string {
	server := strings.TrimSpace(decision.Server)
	if server == "" {
		server = "<unknown>"
	}
	parts := []string{"server=" + server}
	if tool := strings.TrimSpace(decision.Tool); tool != "" {
		parts = append(parts, "tool="+tool)
	}
	if repository := strings.TrimSpace(decision.Repository); repository != "" {
		parts = append(parts, "repository="+repository)
	}
	parts = append(parts, "reason="+decision.Reason)
	return strings.Join(parts, " ")
}

func mcpElicitationResponse(params json.RawMessage, state *mcpElicitationState) (map[string]any, mcpElicitationDecision) {
	decision := mcpElicitationDecision{
		Action: "decline",
		Reason: "invalid_request",
	}
	policy := MCPElicitationPolicy{}
	if state != nil {
		policy = state.policy
	}
	decline := func() (map[string]any, mcpElicitationDecision) {
		return map[string]any{"action": "decline", "content": nil}, decision
	}

	var request struct {
		ServerName      string          `json:"serverName"`
		Mode            string          `json:"mode"`
		ThreadID        string          `json:"threadId"`
		TurnID          string          `json:"turnId"`
		Meta            json.RawMessage `json:"_meta"`
		RequestedSchema json.RawMessage `json:"requestedSchema"`
	}
	if err := json.Unmarshal(params, &request); err != nil {
		return decline()
	}

	decision.Server = request.ServerName
	decision.Mode = request.Mode
	decision.ThreadID = request.ThreadID
	decision.TurnID = request.TurnID
	matches := state.correlate(request.ServerName, request.ThreadID, request.TurnID)
	if len(matches) == 1 {
		decision.ItemID = matches[0].ItemID
		decision.Tool = matches[0].Tool
	}
	browserRequest := request.ServerName == chromeDevToolsServer
	if !browserRequest && !state.hasServerRule(request.ServerName) {
		decision.Reason = "unsupported_server"
		return decline()
	}
	if request.Mode != "form" {
		decision.Reason = "unsupported_mode"
		return decline()
	}

	var meta struct {
		ApprovalKind string `json:"codex_approval_kind"`
	}
	if err := json.Unmarshal(request.Meta, &meta); err != nil {
		decision.Reason = "invalid_metadata"
		return decline()
	}
	decision.ApprovalKind = meta.ApprovalKind
	if meta.ApprovalKind != mcpToolApprovalKind {
		decision.Reason = "unsupported_approval_kind"
		return decline()
	}
	if !isEmptyObjectSchema(request.RequestedSchema) {
		decision.Reason = "unsupported_schema"
		return decline()
	}
	if !browserRequest {
		switch len(matches) {
		case 0:
			decision.Reason = "missing_correlation"
			return decline()
		case 1:
		default:
			decision.Reason = "ambiguous_correlation"
			return decline()
		}
		call := matches[0]
		if !state.hasToolRule(call.Server, call.Tool) {
			decision.Reason = "tool_not_allowlisted"
			return decline()
		}
		repositoryArgument, expectedRepository, deliverableTool := deliverableMCPRepository(policy, call.Server, call.Tool)
		if !deliverableTool || policy.DeliverableKind != "pull_request" {
			decision.Reason = "non_deliverable_mutation"
			return decline()
		}
		repository, ok := mcpToolRepository(call.Arguments, repositoryArgument)
		if !ok {
			decision.Reason = "invalid_tool_arguments"
			return decline()
		}
		decision.Repository = repository
		if expectedRepository == "" || !strings.EqualFold(repository, expectedRepository) ||
			!state.hasRepositoryRule(call.Server, call.Tool, expectedRepository) {
			decision.Reason = "repository_mismatch"
			return decline()
		}
		if issuePublicationTool(call.Server, call.Tool) &&
			!strings.EqualFold(repository, policy.Repository) {
			decision.Reason = "cross_project_issue_publication_unsupported"
			return decline()
		}
		decision.Action = "accept"
		decision.Reason = "allowlisted_deliverable_tool"
		return map[string]any{"action": "accept", "content": map[string]any{}}, decision
	}

	decision.Action = "accept"
	decision.Reason = "supported_browser_tool_approval"
	return map[string]any{"action": "accept", "content": map[string]any{}}, decision
}

func issuePublicationTool(server string, tool string) bool {
	if server != "codex_apps" {
		return false
	}
	switch tool {
	case "github.add_comment_to_issue", "github.update_issue_comment", "github.create_issue", "github.update_issue":
		return true
	default:
		return false
	}
}

func deliverableMCPRepository(policy MCPElicitationPolicy, server string, tool string) (string, string, bool) {
	if server != "codex_apps" {
		return "", "", false
	}
	switch tool {
	case "github.add_comment_to_issue", "github.update_issue_comment":
		return "repo_full_name", policy.IssueRepository, true
	case "github.create_issue", "github.update_issue":
		return "repository_full_name", policy.IssueRepository, true
	case "github.create_pull_request", "github.update_pull_request":
		return "repository_full_name", policy.Repository, true
	default:
		return "", "", false
	}
}

func mcpToolRepository(arguments json.RawMessage, key string) (string, bool) {
	var values map[string]json.RawMessage
	if err := json.Unmarshal(arguments, &values); err != nil || values == nil {
		return "", false
	}
	var repository string
	if err := json.Unmarshal(values[key], &repository); err != nil {
		return "", false
	}
	repository = strings.TrimSpace(repository)
	return repository, repository != ""
}

func isEmptyObjectSchema(data json.RawMessage) bool {
	var schema map[string]json.RawMessage
	if err := json.Unmarshal(data, &schema); err != nil || len(schema) != 2 {
		return false
	}

	var schemaType string
	if err := json.Unmarshal(schema["type"], &schemaType); err != nil || schemaType != "object" {
		return false
	}

	var properties map[string]json.RawMessage
	if err := json.Unmarshal(schema["properties"], &properties); err != nil {
		return false
	}
	return properties != nil && len(properties) == 0
}

func logMCPElicitationDecision(ctx context.Context, logger *slog.Logger, decision mcpElicitationDecision) {
	if logger == nil {
		logger = slog.Default()
	}
	level := slog.LevelInfo
	if decision.Action != "accept" {
		level = slog.LevelWarn
	}
	logger.LogAttrs(ctx, level, "codex MCP elicitation decision",
		slog.String("method", "mcpServer/elicitation/request"),
		slog.String("server", decision.Server),
		slog.String("tool", decision.Tool),
		slog.String("repository", decision.Repository),
		slog.String("mode", decision.Mode),
		slog.String("approval_kind", decision.ApprovalKind),
		slog.String("action", decision.Action),
		slog.String("reason", decision.Reason),
	)
}

func dynamicToolResponse(content string, success bool) map[string]any {
	return map[string]any{
		"contentItems": []map[string]string{{"type": "inputText", "text": content}},
		"success":      success,
	}
}

func maybeEmitUpdate(msg Message, onUpdate UpdateHandler) error {
	update, ok, err := updateFromMessage(msg)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return emitUpdate(update, onUpdate)
}

func emitUpdate(update Update, onUpdate UpdateHandler) error {
	if onUpdate == nil {
		return nil
	}
	if err := onUpdate(update); err != nil {
		return fmt.Errorf("handle codex update: %w", err)
	}
	return nil
}

func transportProcessIdentity(transport Transport) string {
	provider, ok := transport.(interface {
		ProcessIdentity() string
	})
	if !ok {
		return ""
	}
	return provider.ProcessIdentity()
}

func transportWorkerProcess(transport Transport) procgroup.Identity {
	provider, ok := transport.(interface {
		WorkerProcess() procgroup.Identity
	})
	if !ok {
		return procgroup.Identity{}
	}
	return provider.WorkerProcess()
}

func updateFromMessage(msg Message) (Update, bool, error) {
	switch msg.Method {
	case "item/agentMessage/delta":
		var params struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
			ItemID   string `json:"itemId"`
			Delta    string `json:"delta"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return Update{}, false, fmt.Errorf("%w: decode agent message delta: %w", ErrInvalidResponse, err)
		}
		return Update{
			Type:     UpdateAgentMessageDelta,
			Method:   msg.Method,
			ThreadID: params.ThreadID,
			TurnID:   params.TurnID,
			ItemID:   params.ItemID,
			Delta:    params.Delta,
			Payload:  rawPayload(msg),
		}, true, nil
	case "item/commandExecution/outputDelta", "item/fileChange/outputDelta":
		var params struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
			ItemID   string `json:"itemId"`
			Delta    string `json:"delta"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return Update{}, false, fmt.Errorf("%w: decode tool output delta: %w", ErrInvalidResponse, err)
		}
		tool := "command"
		if msg.Method == "item/fileChange/outputDelta" {
			tool = "apply_patch"
		}
		return Update{
			Type:     UpdateToolOutput,
			Method:   msg.Method,
			ThreadID: params.ThreadID,
			TurnID:   params.TurnID,
			ItemID:   params.ItemID,
			Tool:     tool,
			Delta:    params.Delta,
			Payload:  rawPayload(msg),
		}, true, nil
	case "item/mcpToolCall/progress":
		var params struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
			ItemID   string `json:"itemId"`
			Message  string `json:"message"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return Update{}, false, fmt.Errorf("%w: decode tool progress: %w", ErrInvalidResponse, err)
		}
		return Update{
			Type:     UpdateToolOutput,
			Method:   msg.Method,
			ThreadID: params.ThreadID,
			TurnID:   params.TurnID,
			ItemID:   params.ItemID,
			Tool:     "mcp",
			Delta:    params.Message,
			Payload:  rawPayload(msg),
		}, true, nil
	case "item/started", "item/completed":
		return toolLifecycleUpdate(msg)
	case "thread/tokenUsage/updated":
		var params struct {
			ThreadID    string `json:"threadId"`
			TurnID      string `json:"turnId"`
			Model       string `json:"model"`
			ModelID     string `json:"model_id"`
			ModelIDAlt  string `json:"modelId"`
			Resolved    string `json:"resolvedModel"`
			ResolvedAlt string `json:"resolved_model"`
			TokenUsage  struct {
				Total              TokenUsageBreakdown  `json:"total"`
				Last               *TokenUsageBreakdown `json:"last"`
				ModelContextWindow *int64               `json:"modelContextWindow"`
			} `json:"tokenUsage"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return Update{}, false, fmt.Errorf("%w: decode token usage: %w", ErrInvalidResponse, err)
		}
		return Update{
			Type:     UpdateTokenUsage,
			Method:   msg.Method,
			ThreadID: params.ThreadID,
			TurnID:   params.TurnID,
			Model:    firstNonBlank(params.Model, params.Resolved, params.ResolvedAlt, params.ModelID, params.ModelIDAlt),
			Tokens: TokenUsage{
				InputTokens:           params.TokenUsage.Total.InputTokens,
				CachedInputTokens:     params.TokenUsage.Total.CachedInputTokens,
				OutputTokens:          params.TokenUsage.Total.OutputTokens,
				ReasoningOutputTokens: params.TokenUsage.Total.ReasoningOutputTokens,
				TotalTokens:           params.TokenUsage.Total.TotalTokens,
				Last:                  params.TokenUsage.Last,
				ModelContextWindow:    params.TokenUsage.ModelContextWindow,
			},
			Payload: rawPayload(msg),
		}, true, nil
	case "account/rateLimits/updated":
		var params struct {
			RateLimits rateLimitSnapshotPayload `json:"rateLimits"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return Update{}, false, fmt.Errorf("%w: decode rate limits: %w", ErrInvalidResponse, err)
		}
		return Update{
			Type:       UpdateRateLimits,
			Method:     msg.Method,
			RateLimits: params.RateLimits.snapshot(),
			Payload:    rawPayload(msg),
		}, true, nil
	case "turn/started":
		var params struct {
			ThreadID    string `json:"threadId"`
			Model       string `json:"model"`
			ModelID     string `json:"model_id"`
			ModelIDAlt  string `json:"modelId"`
			Resolved    string `json:"resolvedModel"`
			ResolvedAlt string `json:"resolved_model"`
			Turn        struct {
				ID          string `json:"id"`
				Model       string `json:"model"`
				ModelID     string `json:"model_id"`
				ModelIDAlt  string `json:"modelId"`
				Resolved    string `json:"resolvedModel"`
				ResolvedAlt string `json:"resolved_model"`
			} `json:"turn"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return Update{}, false, fmt.Errorf("%w: decode turn started: %w", ErrInvalidResponse, err)
		}
		model := firstNonBlank(
			params.Turn.Model,
			params.Turn.Resolved,
			params.Turn.ResolvedAlt,
			params.Turn.ModelID,
			params.Turn.ModelIDAlt,
			params.Model,
			params.Resolved,
			params.ResolvedAlt,
			params.ModelID,
			params.ModelIDAlt,
		)
		return Update{
			Type:            UpdateTurnStarted,
			Method:          msg.Method,
			ThreadID:        params.ThreadID,
			TurnID:          params.Turn.ID,
			Status:          "started",
			Model:           model,
			RuntimeIdentity: agentidentity.RuntimeUpdate(model, "", "", "", time.Time{}),
			Payload:         rawPayload(msg),
		}, true, nil
	case "thread/settings/updated":
		var params struct {
			ThreadID       string `json:"threadId"`
			ThreadSettings struct {
				Model         string `json:"model"`
				ModelProvider string `json:"modelProvider"`
				ServiceTier   string `json:"serviceTier"`
				Effort        string `json:"effort"`
			} `json:"threadSettings"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return Update{}, false, fmt.Errorf("%w: decode thread settings update: %w", ErrInvalidResponse, err)
		}
		identity := agentidentity.RuntimeUpdate(
			params.ThreadSettings.Model,
			params.ThreadSettings.ModelProvider,
			params.ThreadSettings.Effort,
			params.ThreadSettings.ServiceTier,
			time.Time{},
		)
		if strings.TrimSpace(params.ThreadSettings.Effort) == "" {
			identity.ReasoningEffort = agentidentity.UnknownValue()
		}
		if strings.TrimSpace(params.ThreadSettings.ServiceTier) == "" {
			identity.ServiceTier = agentidentity.UnknownValue()
		}
		return Update{
			Type:            UpdateRuntimeIdentity,
			Method:          msg.Method,
			ThreadID:        params.ThreadID,
			Model:           identity.Model(),
			RuntimeIdentity: identity,
			Payload:         rawPayload(msg),
		}, true, nil
	case "model/rerouted":
		var params struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
			ToModel  string `json:"toModel"`
			Model    string `json:"model"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return Update{}, false, fmt.Errorf("%w: decode model rerouted: %w", ErrInvalidResponse, err)
		}
		model := firstNonBlank(params.ToModel, params.Model)
		return Update{
			Type:            UpdateModelUpdated,
			Method:          msg.Method,
			ThreadID:        params.ThreadID,
			TurnID:          params.TurnID,
			Model:           model,
			RuntimeIdentity: agentidentity.RuntimeUpdate(model, "", "", "", time.Time{}),
			Payload:         rawPayload(msg),
		}, true, nil
	case "model/safetyBuffering/updated":
		var params struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
			Model    string `json:"model"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return Update{}, false, fmt.Errorf("%w: decode model safety buffering: %w", ErrInvalidResponse, err)
		}
		return Update{
			Type:            UpdateModelUpdated,
			Method:          msg.Method,
			ThreadID:        params.ThreadID,
			TurnID:          params.TurnID,
			Model:           params.Model,
			RuntimeIdentity: agentidentity.RuntimeUpdate(params.Model, "", "", "", time.Time{}),
			Payload:         rawPayload(msg),
		}, true, nil
	case "turn/completed":
		var params struct {
			ThreadID string `json:"threadId"`
			Turn     struct {
				ID      string          `json:"id"`
				Status  string          `json:"status"`
				Error   json.RawMessage `json:"error"`
				Message string          `json:"message"`
				Model   string          `json:"model"`
			} `json:"turn"`
			Type    string          `json:"type"`
			Status  any             `json:"status"`
			Error   json.RawMessage `json:"error"`
			Message string          `json:"message"`
			Model   string          `json:"model"`
		}
		if len(msg.Params) > 0 {
			if err := json.Unmarshal(msg.Params, &params); err != nil {
				return Update{}, false, fmt.Errorf("%w: decode turn completed: %w", ErrInvalidResponse, err)
			}
		}
		status := strings.TrimSpace(params.Turn.Status)
		if status == "" {
			status = turnCompletedTopLevelStatus(params.Status, params.Error, params.Type)
		}
		errorBody, errorMessage := turnCompletedBackendError(params.Error, params.Turn.Error, params.Message, params.Turn.Message, msg.Params)
		if status != "" && !strings.EqualFold(status, "completed") && errorBody == "" && errorMessage == "" {
			errorBody = turnCompletedMissingErrorBody(status)
			if strings.EqualFold(status, "interrupted") {
				errorMessage = "codex reported an interrupted turn without error detail"
			} else {
				errorMessage = "codex reported turn status " + strconv.Quote(status) + " without error detail"
			}
		}
		return Update{
			Type:                UpdateTurnCompleted,
			Method:              msg.Method,
			ThreadID:            params.ThreadID,
			TurnID:              params.Turn.ID,
			Status:              status,
			Model:               firstNonBlank(params.Turn.Model, params.Model),
			BackendErrorBody:    errorBody,
			BackendErrorMessage: errorMessage,
			Payload:             rawPayload(msg),
		}, true, nil
	default:
		return Update{}, false, nil
	}
}

func toolLifecycleUpdate(msg Message) (Update, bool, error) {
	var params struct {
		ThreadID string `json:"threadId"`
		TurnID   string `json:"turnId"`
		Item     struct {
			ID               string          `json:"id"`
			Type             string          `json:"type"`
			Command          string          `json:"command"`
			Arguments        json.RawMessage `json:"arguments"`
			Status           string          `json:"status"`
			ExitCode         *int            `json:"exitCode"`
			AggregatedOutput string          `json:"aggregatedOutput"`
			Server           string          `json:"server"`
			Tool             string          `json:"tool"`
			Result           json.RawMessage `json:"result"`
			Error            json.RawMessage `json:"error"`
			Changes          json.RawMessage `json:"changes"`
			ContentItems     json.RawMessage `json:"contentItems"`
		} `json:"item"`
	}
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return Update{}, false, fmt.Errorf("%w: decode tool lifecycle: %w", ErrInvalidResponse, err)
	}
	if !toolItemType(params.Item.Type) {
		return Update{}, false, nil
	}
	tool := firstNonBlank(params.Item.Tool, params.Item.Type)
	if params.Item.Server != "" && params.Item.Tool != "" {
		tool = params.Item.Server + "/" + params.Item.Tool
	}
	content := firstNonBlank(
		params.Item.Command,
		compactNonNullJSON(params.Item.Arguments),
	)
	errorBody := ""
	errorMessage := ""
	updateType := UpdateToolStarted
	if msg.Method == "item/completed" {
		updateType = UpdateToolCompleted
		errorBody = compactNonNullJSON(params.Item.Error)
		errorMessage = errorMessageFromJSON(params.Item.Error)
		if failedToolStatus(params.Item.Status) && errorBody == "" {
			errorBody = firstNonBlank(
				compactNonNullJSON(params.Item.Result),
				compactNonNullJSON(params.Item.ContentItems),
			)
		}
		if failedToolStatus(params.Item.Status) && errorMessage == "" {
			errorMessage = firstNonBlank(
				errorMessageFromJSON(params.Item.Result),
				errorMessageFromJSON(params.Item.ContentItems),
			)
		}
		if failedToolStatus(params.Item.Status) && errorMessage == "" {
			errorMessage = toolFailureDiagnosticMessage(msg.Params)
		}
		content = firstNonBlank(
			params.Item.AggregatedOutput,
			errorMessage,
			compactNonNullJSON(params.Item.Result),
			compactNonNullJSON(params.Item.ContentItems),
			compactNonNullJSON(params.Item.Changes),
			params.Item.Command,
			params.Item.Status,
		)
	}
	return Update{
		Type:                updateType,
		Method:              msg.Method,
		ThreadID:            params.ThreadID,
		TurnID:              params.TurnID,
		ItemID:              params.Item.ID,
		Tool:                tool,
		Command:             strings.TrimSpace(params.Item.Command),
		Delta:               content,
		Status:              params.Item.Status,
		ExitCode:            params.Item.ExitCode,
		BackendErrorBody:    errorBody,
		BackendErrorMessage: errorMessage,
		Payload:             rawPayload(msg),
	}, true, nil
}

func failedToolStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "failed", "error", "cancelled", "canceled", "timed_out":
		return true
	default:
		return false
	}
}

func toolItemType(value string) bool {
	switch strings.TrimSpace(value) {
	case "commandExecution", "fileChange", "mcpToolCall", "dynamicToolCall", "collabAgentToolCall", "webSearch", "imageGeneration":
		return true
	default:
		return false
	}
}

func turnCompletedTopLevelStatus(status any, errorBody json.RawMessage, eventType string) string {
	switch value := status.(type) {
	case string:
		return strings.TrimSpace(value)
	case float64:
		if value >= 400 || len(bytes.TrimSpace(errorBody)) > 0 {
			return "failed"
		}
	}
	if len(bytes.TrimSpace(errorBody)) > 0 || strings.EqualFold(strings.TrimSpace(eventType), "error") {
		return "failed"
	}
	return ""
}

func turnCompletedBackendError(
	topLevelError json.RawMessage,
	turnError json.RawMessage,
	topLevelMessage string,
	turnMessage string,
	params json.RawMessage,
) (string, string) {
	if looksLikeErrorEvent(params) {
		body := compactJSON(params)
		message := firstNonBlank(errorMessageFromJSON(params), errorMessageFromJSON(topLevelError), errorMessageFromJSON(turnError), topLevelMessage, turnMessage)
		return body, message
	}
	body := compactNonNullJSON(firstRawJSON(topLevelError, turnError))
	message := firstNonBlank(errorMessageFromJSON(topLevelError), errorMessageFromJSON(turnError), topLevelMessage, turnMessage)
	if body != "" {
		if message != "" {
			return body, message
		}
		return body, body
	}
	if strings.TrimSpace(message) != "" {
		return compactJSON(params), message
	}
	return "", ""
}

func turnCompletedMissingErrorBody(status string) string {
	body, err := json.Marshal(struct {
		Status string `json:"status"`
		Detail string `json:"detail"`
	}{
		Status: strings.TrimSpace(status),
		Detail: "backend supplied no error",
	})
	if err != nil {
		return ""
	}
	return string(body)
}

func firstRawJSON(values ...json.RawMessage) json.RawMessage {
	for _, value := range values {
		if len(bytes.TrimSpace(value)) > 0 {
			return value
		}
	}
	return nil
}

func compactJSON(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return ""
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return string(raw)
	}
	return buf.String()
}

func compactNonNullJSON(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	return compactJSON(raw)
}

func errorMessageFromJSON(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return ""
	}
	var decoded struct {
		Message string `json:"message"`
		Error   struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return ""
	}
	return firstNonBlank(decoded.Message, decoded.Error.Message)
}

type toolFailureDiagnostic struct {
	Code       string `json:"code,omitempty"`
	Message    string `json:"message,omitempty"`
	HTTPStatus int    `json:"http_status,omitempty"`
}

func toolFailureDiagnosticMessage(raw json.RawMessage) string {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return ""
	}
	diagnostic := toolFailureDiagnostic{}
	collectToolFailureDiagnostic(value, &diagnostic)
	if diagnostic == (toolFailureDiagnostic{}) {
		return ""
	}
	return marshalToolFailureDiagnostic(diagnostic)
}

func collectToolFailureDiagnostic(value any, diagnostic *toolFailureDiagnostic) {
	if diagnostic == nil {
		return
	}
	switch value := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			leftRank := toolFailureDiagnosticKeyRank(keys[i])
			rightRank := toolFailureDiagnosticKeyRank(keys[j])
			if leftRank != rightRank {
				return leftRank < rightRank
			}
			return keys[i] < keys[j]
		})
		for _, key := range keys {
			collectToolFailureDiagnosticField(key, value[key], diagnostic)
		}
		for _, key := range keys {
			if toolFailureDiagnosticSensitiveKey(key) {
				continue
			}
			collectToolFailureDiagnostic(value[key], diagnostic)
		}
	case []any:
		for _, item := range value {
			collectToolFailureDiagnostic(item, diagnostic)
		}
	}
}

func collectToolFailureDiagnosticField(key string, value any, diagnostic *toolFailureDiagnostic) {
	switch toolFailureDiagnosticKey(key) {
	case "code":
		if diagnostic.Code == "" {
			diagnostic.Code = toolFailureDiagnosticText(value)
		}
	case "message":
		if diagnostic.Message == "" {
			message, ok := value.(string)
			if ok {
				diagnostic.Message = strings.TrimSpace(message)
			}
		}
	case "httpstatus":
		if diagnostic.HTTPStatus == 0 {
			diagnostic.HTTPStatus = toolFailureHTTPStatus(value)
		}
	}
}

func toolFailureDiagnosticKey(key string) string {
	key = strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(key), "_", ""), "-", ""))
	switch key {
	case "code", "errorcode":
		return "code"
	case "message", "errormessage":
		return "message"
	case "httpstatus", "statuscode", "status":
		return "httpstatus"
	default:
		return ""
	}
}

func toolFailureDiagnosticSensitiveKey(key string) bool {
	key = strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(key), "_", ""), "-", ""))
	for _, fragment := range []string{
		"argument", "auth", "body", "command", "cookie", "credential", "header", "input", "password", "payload", "request", "secret", "token",
	} {
		if strings.Contains(key, fragment) {
			return true
		}
	}
	return key == "aggregatedoutput" || key == "changes"
}

func toolFailureDiagnosticKeyRank(key string) int {
	switch strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(key), "_", ""), "-", "")) {
	case "error":
		return 0
	case "failure":
		return 1
	case "diagnostic", "diagnostics":
		return 2
	case "detail", "details":
		return 3
	case "result":
		return 4
	case "contentitems":
		return 5
	default:
		return 6
	}
}

func toolFailureDiagnosticText(value any) string {
	switch value := value.(type) {
	case string:
		return strings.TrimSpace(value)
	case json.Number:
		return strings.TrimSpace(value.String())
	default:
		return ""
	}
}

func toolFailureHTTPStatus(value any) int {
	text := toolFailureDiagnosticText(value)
	status, err := strconv.Atoi(text)
	if err != nil || status < 100 || status > 599 {
		return 0
	}
	return status
}

func marshalToolFailureDiagnostic(diagnostic toolFailureDiagnostic) string {
	encoded, err := json.Marshal(diagnostic)
	if err != nil {
		return ""
	}
	if len(encoded) <= maxToolFailureDiagnosticBytes {
		return string(encoded)
	}
	fitToolFailureDiagnosticField(&diagnostic, diagnostic.Message, func(value string) {
		diagnostic.Message = value
	})
	encoded, err = json.Marshal(diagnostic)
	if err != nil {
		return ""
	}
	if len(encoded) <= maxToolFailureDiagnosticBytes {
		return string(encoded)
	}
	fitToolFailureDiagnosticField(&diagnostic, diagnostic.Code, func(value string) {
		diagnostic.Code = value
	})
	encoded, err = json.Marshal(diagnostic)
	if err != nil || len(encoded) > maxToolFailureDiagnosticBytes {
		return ""
	}
	return string(encoded)
}

func fitToolFailureDiagnosticField(diagnostic *toolFailureDiagnostic, value string, set func(string)) {
	runes := []rune(value)
	best := ""
	for low, high := 0, len(runes); low <= high; {
		middle := low + (high-low)/2
		candidate := string(runes[:middle])
		set(candidate)
		encoded, err := json.Marshal(diagnostic)
		if err == nil && len(encoded) <= maxToolFailureDiagnosticBytes {
			best = candidate
			low = middle + 1
		} else {
			high = middle - 1
		}
	}
	set(best)
}

func looksLikeErrorEvent(raw json.RawMessage) bool {
	var decoded struct {
		Type   string          `json:"type"`
		Status any             `json:"status"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(decoded.Type), "error") || len(bytes.TrimSpace(decoded.Error)) > 0
}

type rateLimitSnapshotPayload struct {
	LimitID              string                  `json:"limitId"`
	LimitName            string                  `json:"limitName"`
	Primary              *rateLimitWindowPayload `json:"primary"`
	Secondary            *rateLimitWindowPayload `json:"secondary"`
	Credits              *creditsSnapshotPayload `json:"credits"`
	RateLimitReachedType string                  `json:"rateLimitReachedType"`
}

type rateLimitWindowPayload struct {
	UsedPercent        float64  `json:"usedPercent"`
	WindowDurationMins *float64 `json:"windowDurationMins"`
	ResetsAt           *int64   `json:"resetsAt"`
}

type creditsSnapshotPayload struct {
	HasCredits bool   `json:"hasCredits"`
	Unlimited  bool   `json:"unlimited"`
	Balance    string `json:"balance"`
}

func (p rateLimitSnapshotPayload) snapshot() *RateLimitSnapshot {
	return &RateLimitSnapshot{
		LimitID:              p.LimitID,
		LimitName:            p.LimitName,
		Primary:              p.Primary.window(),
		Secondary:            p.Secondary.window(),
		Credits:              p.Credits.credits(),
		RateLimitReachedType: p.RateLimitReachedType,
	}
}

func (p *rateLimitWindowPayload) window() *RateLimitWindow {
	if p == nil {
		return nil
	}
	return &RateLimitWindow{
		UsedPercent:        p.UsedPercent,
		WindowDurationMins: p.WindowDurationMins,
		ResetsAt:           p.ResetsAt,
	}
}

func (p *creditsSnapshotPayload) credits() *CreditsSnapshot {
	if p == nil {
		return nil
	}
	return &CreditsSnapshot{
		HasCredits: p.HasCredits,
		Unlimited:  p.Unlimited,
		Balance:    p.Balance,
	}
}

func receiveWithTimeout(ctx context.Context, transport Transport, timeout time.Duration, timeoutContext timeoutContextFactory) (Message, error) {
	ctx = contextOrBackground(ctx)
	if timeout <= 0 {
		return transport.Receive(ctx)
	}

	receiveCtx, cancel := timeoutContext(ctx, timeout, nil)
	defer cancel()
	return transport.Receive(receiveCtx)
}

func setOptional(params map[string]any, key string, value any) {
	if value == nil {
		return
	}
	if raw, ok := value.(json.RawMessage); ok && len(raw) == 0 {
		return
	}
	params[key] = value
}

func requestID(id int) json.RawMessage {
	return json.RawMessage(strconv.Itoa(id))
}

func requestIDMatches(raw json.RawMessage, id int) bool {
	return bytes.Equal(bytes.TrimSpace(raw), requestID(id))
}

func requestName(id int) string {
	switch id {
	case initializeRequestID:
		return "initialize"
	case threadStartRequestID:
		return "thread/start"
	case turnStartRequestID:
		return "turn/start"
	case threadResumeRequestID:
		return "thread/resume"
	case configReadRequestID:
		return "config/read"
	case modelListRequestID:
		return "model/list"
	case threadReadRequestID:
		return "thread/read"
	case accountReadRequestID:
		return "account/read"
	default:
		return strconv.Itoa(id)
	}
}

func rawPayload(msg Message) json.RawMessage {
	data, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	return data
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func closeTransport(ctx context.Context, transport Transport, timeout time.Duration) error {
	ctx = contextOrBackground(ctx)
	ctx = context.WithoutCancel(ctx)
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if err := transport.Close(ctx); err != nil {
		return &TransportCloseError{Err: err}
	}
	return nil
}
