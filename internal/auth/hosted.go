package auth

import (
	"context"
	"errors"
	"time"
)

var ErrHostedIdentity = errors.New("hosted identity is unavailable or inactive")

type HostedIdentity struct {
	Subject        string    `json:"subject"`
	OrganizationID string    `json:"organization_id"`
	SessionID      string    `json:"session_id"`
	CreatedAt      time.Time `json:"created_at"`
	ExpiresAt      time.Time `json:"expires_at"`
	SupportActor   string    `json:"support_actor,omitempty"`
	SupportReason  string    `json:"support_reason,omitempty"`
}

type Organization struct {
	ID         string `json:"id"`
	ExternalID string `json:"external_id"`
	Name       string `json:"name"`
}

type Membership struct {
	ID             string `json:"id"`
	UserID         string `json:"user_id"`
	OrganizationID string `json:"organization_id"`
	Status         string `json:"status"`
	Role           struct {
		Slug string `json:"slug"`
	} `json:"role"`
}

type Invitation struct {
	ID             string    `json:"id"`
	Email          string    `json:"email"`
	OrganizationID string    `json:"organization_id"`
	State          string    `json:"state"`
	ExpiresAt      time.Time `json:"expires_at"`
	AcceptedUserID string    `json:"accepted_user_id"`
}

type HostedProvider interface {
	IdentityProvider
	CurrentSession(context.Context, HostedIdentity) (HostedIdentity, error)
	Memberships(context.Context, string, string) ([]Membership, error)
	Organization(context.Context, string) (Organization, error)
	CreateOrganization(context.Context, string, string) (Organization, error)
	CreateMembership(context.Context, string, string, string) (Membership, error)
	SetMembershipRole(context.Context, string, string) error
	RevokeMembership(context.Context, string) error
	Invite(context.Context, string, string, string, string) (Invitation, error)
	Invitation(context.Context, string) (Invitation, error)
	AcceptInvitation(context.Context, string, string) error
	RevokeSession(context.Context, string) error
}

func ValidOrganizationRole(role string) bool {
	switch role {
	case "owner", "admin", "member", "viewer":
		return true
	default:
		return false
	}
}

func ValidSupportReason(reason string) bool {
	switch reason {
	case "customer-request", "account-recovery", "troubleshooting":
		return true
	default:
		return false
	}
}
