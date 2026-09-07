package tracker

import (
	"context"
	"time"

	"github.com/digitaldrywood/detent/internal/policy"
)

type ChangeRequest struct {
	ID             string             `json:"change_id"`
	OrganizationID OrganizationID     `json:"organization_id"`
	ProjectID      ProjectID          `json:"project_id"`
	WorkItemID     NativeWorkItemID   `json:"work_item_id"`
	LinkedIssues   []NativeWorkItemID `json:"linked_issues"`
	Title          string             `json:"title"`
	Body           string             `json:"body"`
	CurrentVersion string             `json:"current_version_id"`
	Revision       Revision           `json:"revision,string"`
	CreatedAt      time.Time          `json:"created_at"`
	UpdatedAt      time.Time          `json:"updated_at"`
}

type ChangeArtifact struct {
	Kind         string `json:"kind"`
	URI          string `json:"uri"`
	SHA256       string `json:"sha256"`
	Availability string `json:"availability"`
}

type ChangeExternalReference struct {
	Provider string `json:"provider"`
	ID       string `json:"id"`
	URL      string `json:"url"`
}

type ChangeCheckSpec struct {
	Name           string `json:"name"`
	PrincipalID    string `json:"principal_id"`
	WorkflowID     string `json:"workflow_id"`
	WorkflowSHA256 string `json:"workflow_sha256"`
	Source         string `json:"source"`
	MaxAgeSeconds  int64  `json:"max_age_seconds"`
}

type ChangeReviewPolicy struct {
	ID             string            `json:"review_policy_id"`
	PolicyID       string            `json:"policy_id"`
	RequireReview  bool              `json:"require_review"`
	RequiredChecks []ChangeCheckSpec `json:"required_checks"`
}

type ApproveChangeReviewPolicy struct {
	Mutation
	ExpectedID string             `json:"expected_review_policy_id"`
	Policy     ChangeReviewPolicy `json:"policy"`
}

type ChangeVersionInput struct {
	BaseSHA      string                   `json:"base_sha"`
	HeadSHA      string                   `json:"head_sha"`
	MergeBaseSHA string                   `json:"merge_base_sha"`
	Repository   string                   `json:"repository"`
	Code         ChangeArtifact           `json:"code"`
	Artifacts    []ChangeArtifact         `json:"artifacts"`
	RunID        string                   `json:"run_id,omitempty"`
	AttemptID    string                   `json:"attempt_id,omitempty"`
	PolicyID     string                   `json:"policy_id"`
	External     *ChangeExternalReference `json:"external,omitempty"`
}

type ChangeCheckExpectation struct {
	ChangeCheckSpec
	CheckRunID string `json:"check_run_id"`
}

type ChangeVersion struct {
	ChangeVersionInput
	ID           string                   `json:"version_id"`
	ChangeID     string                   `json:"change_id"`
	Number       int64                    `json:"number,string"`
	Policy       policy.Descriptor        `json:"policy"`
	ReviewPolicy ChangeReviewPolicy       `json:"review_policy"`
	Checks       []ChangeCheckExpectation `json:"checks"`
	Actor        Actor                    `json:"actor"`
	CreatedAt    time.Time                `json:"created_at"`
}

type CreateChange struct {
	Mutation
	Title        string             `json:"title"`
	Body         string             `json:"body"`
	LinkedIssues []NativeWorkItemID `json:"linked_issues"`
}

type PublishChangeVersion struct {
	Mutation
	ExpectedVersionID string `json:"expected_version_id"`
	ChangeVersionInput
}

type ChangeReview struct {
	ID        string    `json:"review_id"`
	VersionID string    `json:"version_id"`
	Decision  string    `json:"decision"`
	Body      string    `json:"body"`
	Actor     Actor     `json:"actor"`
	CreatedAt time.Time `json:"created_at"`
}

type ReviewChange struct {
	Mutation
	Decision          string              `json:"decision"`
	Body              string              `json:"body"`
	ExpectedVersionID string              `json:"expected_version_id,omitempty"`
	Bundle            *ChangeReviewBundle `json:"bundle,omitempty"`
}

type ChangeReviewBundle struct {
	ArtifactID string `json:"artifact_id"`
	Revision   int64  `json:"revision"`
	SHA256     string `json:"sha256"`
	HeadSHA    string `json:"head_sha"`
}

type ViewChangeFile struct {
	Mutation
	Bundle     ChangeReviewBundle `json:"bundle"`
	FileSHA256 string             `json:"file_sha256"`
	Viewed     bool               `json:"viewed"`
}

type ChangeViewedFile struct {
	ManifestSHA256 string `json:"manifest_sha256"`
	FileSHA256     string `json:"file_sha256"`
	Viewed         bool   `json:"viewed"`
}

type ChangeCheckResult struct {
	CheckRunID     string           `json:"check_run_id"`
	HeadSHA        string           `json:"head_sha"`
	RunID          string           `json:"run_id"`
	PolicyID       string           `json:"policy_id"`
	ConfigDigest   string           `json:"config_digest"`
	WorkflowID     string           `json:"workflow_id"`
	WorkflowSHA256 string           `json:"workflow_sha256"`
	Source         string           `json:"source"`
	Conclusion     string           `json:"conclusion"`
	CompletedAt    time.Time        `json:"completed_at"`
	Evidence       []ChangeArtifact `json:"evidence"`
}

type SubmitChangeCheck struct {
	Mutation
	ChangeCheckResult
}

type ChangeCheck struct {
	ChangeCheckResult
	VersionID  string    `json:"version_id"`
	Actor      Actor     `json:"actor"`
	ReceivedAt time.Time `json:"received_at"`
}

type ChangeDiscussion struct {
	ID         string      `json:"comment_id"`
	VersionID  string      `json:"version_id,omitempty"`
	Body       string      `json:"body"`
	Actor      Actor       `json:"actor"`
	Provenance *Provenance `json:"provenance,omitempty"`
	CreatedAt  time.Time   `json:"created_at"`
}

type DiscussChange struct {
	Mutation
	VersionID  string      `json:"version_id,omitempty"`
	Body       string      `json:"body"`
	Provenance *Provenance `json:"provenance,omitempty"`
}

type ChangeDetail struct {
	External   *PullRequestSummary `json:"external_snapshot,omitempty"`
	Change     ChangeRequest       `json:"change"`
	Versions   []ChangeVersion     `json:"versions"`
	Reviews    []ChangeReview      `json:"reviews"`
	Checks     []ChangeCheck       `json:"checks"`
	Discussion []ChangeDiscussion  `json:"discussion"`
	Summary    ChangeSummary       `json:"summary"`
}

type ChangeSummary struct {
	NativeReview   string   `json:"native_review"`
	ExternalReview string   `json:"external_review"`
	Checks         string   `json:"checks"`
	Status         string   `json:"status"`
	Messages       []string `json:"messages"`
}

type ChangeReader interface {
	FetchChanges(context.Context, NativeWorkItemID) ([]ChangeRequest, error)
	FetchChange(context.Context, NativeWorkItemID, string) (ChangeDetail, error)
	FetchNativeAttempts(context.Context, NativeWorkItemID) ([]NativeAttempt, error)
}
