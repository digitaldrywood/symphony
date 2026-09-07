package hubserver

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"strings"

	"github.com/digitaldrywood/detent/internal/auth"
)

type HostedDestination struct {
	OrganizationID       string `yaml:"organization_id"`
	WorkOSOrganizationID string `yaml:"workos_organization_id"`
	PublicURL            string `yaml:"public_url"`
}

type HostedConfig struct {
	OrganizationID       string
	WorkOSOrganizationID string
	BootstrapSubject     string
	PublicURL            string
	StaffEmails          []string
	SupportActors        []string
	Directory            []HostedDestination
	Provider             auth.HostedProvider
	PlanID               string
	StorageQuotaBytes    int64
	EventQuota           int64
}

func (c *HostedConfig) validate() error {
	if c == nil {
		return nil
	}
	if !strings.HasPrefix(c.OrganizationID, "org_") || len(c.OrganizationID) > 64 || !hostedSafeID(c.OrganizationID) || c.Provider == nil {
		return errors.New("hosted organization identity and identity provider are required")
	}
	if c.WorkOSOrganizationID == "" && c.BootstrapSubject == "" {
		return errors.New("unallocated hosted Hub requires a bootstrap user ID")
	}
	if !hostedPublicURL(c.PublicURL) {
		return errors.New("hosted public URL must use HTTPS or loopback HTTP without a path, query or fragment")
	}
	if c.PlanID != "" && !hostedSafeID(c.PlanID) || c.StorageQuotaBytes < 0 || c.EventQuota < 0 {
		return errors.New("hosted plan metadata is invalid")
	}
	seen := make(map[string]bool)
	for _, destination := range c.Directory {
		if !hostedSafeID(destination.OrganizationID) || !hostedSafeID(destination.WorkOSOrganizationID) || !hostedPublicURL(destination.PublicURL) || seen[destination.OrganizationID] {
			return errors.New("hosted organization directory is invalid")
		}
		seen[destination.OrganizationID] = true
	}
	for _, actor := range c.SupportActors {
		if !hostedEmailListed(c.StaffEmails, actor) {
			return errors.New("hosted support actors must also be declared staff identities")
		}
	}
	return nil
}

func hostedPublicURL(value string) bool {
	u, err := url.Parse(value)
	return err == nil && u != nil && u.Host != "" && u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == "" && (u.Scheme == "https" || u.Scheme == "http" && listenerAddressLoopback(u.Host))
}

func hostedSafeID(value string) bool {
	return value != "" && len(value) <= 128 && strings.IndexFunc(value, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' && r != '-'
	}) == -1
}

func hostedEmailListed(emails []string, value string) bool {
	return slices.ContainsFunc(emails, func(email string) bool { return strings.EqualFold(strings.TrimSpace(email), strings.TrimSpace(value)) })
}

func (s *Service) hostedProviderOrganization(ctx context.Context) (string, error) {
	var id string
	err := s.database.db.QueryRowContext(ctx, "SELECT provider_id FROM hosted_tenant WHERE singleton = 1").Scan(&id)
	return id, err
}
