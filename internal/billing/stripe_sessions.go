package billing

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type stripePrice struct {
	ID            string
	Livemode      *bool
	Active        bool
	Type          string
	BillingScheme string `json:"billing_scheme"`
	Recurring     struct {
		UsageType string `json:"usage_type"`
	}
}

func (p stripePrice) supported() bool {
	return validID(p.ID, "price_") && testMode(p.Livemode) && p.Type == "recurring" && p.BillingScheme == "per_unit" && p.Recurring.UsageType == "licensed"
}

func (s *stripeProvider) Checkout(ctx context.Context, request CheckoutRequest) (Session, error) {
	if err := s.verifyBinding(ctx, request.Binding); err != nil {
		return Session{}, err
	}
	if !validID(request.PriceID, "price_") || request.IdempotencyKey == "" {
		return Session{}, errors.New("stripe checkout configuration is invalid")
	}
	var price stripePrice
	if err := s.request(ctx, http.MethodGet, "prices/"+request.PriceID, "", nil, &price); err != nil {
		return Session{}, err
	}
	if !price.supported() || !price.Active || price.ID != request.PriceID {
		return Session{}, errors.New("stripe checkout requires an active test recurring price")
	}
	form := url.Values{
		"mode": {"subscription"}, "customer": {request.CustomerID}, "client_reference_id": {request.OrganizationID},
		"line_items[0][price]": {request.PriceID}, "line_items[0][quantity]": {"1"},
		"success_url": {request.ReturnURL + "?checkout=returned"}, "cancel_url": {request.ReturnURL},
		"expires_at": {strconv.FormatInt(request.ExpiresAt.Unix(), 10)},
		"subscription_data[metadata][detent_organization_id]": {request.OrganizationID},
		"metadata[detent_organization_id]":                    {request.OrganizationID},
	}
	var response struct {
		ID        string
		URL       string
		Customer  string
		Livemode  *bool
		ExpiresAt int64 `json:"expires_at"`
	}
	if err := s.request(ctx, http.MethodPost, "checkout/sessions", request.IdempotencyKey, form, &response); err != nil {
		return Session{}, err
	}
	if !validID(response.ID, "cs_test_") || !testMode(response.Livemode) || response.Customer != request.CustomerID || !sessionURL(response.URL, "checkout.stripe.com") || response.ExpiresAt != request.ExpiresAt.Unix() {
		return Session{}, errors.New("stripe returned an invalid test checkout session")
	}
	return Session{ID: response.ID, URL: response.URL, ExpiresAt: time.Unix(response.ExpiresAt, 0).UTC()}, nil
}

func (s *stripeProvider) Portal(ctx context.Context, binding Binding, configuration, returnURL string) (Session, error) {
	if err := s.verifyBinding(ctx, binding); err != nil {
		return Session{}, err
	}
	if !validID(configuration, "bpc_") {
		return Session{}, errors.New("stripe portal configuration is required")
	}
	var config struct {
		ID       string
		Livemode *bool
		Active   bool
	}
	if err := s.request(ctx, http.MethodGet, "billing_portal/configurations/"+configuration, "", nil, &config); err != nil {
		return Session{}, err
	}
	if config.ID != configuration || !testMode(config.Livemode) || !config.Active {
		return Session{}, errors.New("stripe portal requires an active test configuration")
	}
	var response struct {
		ID       string
		URL      string
		Livemode *bool
		Customer string
	}
	form := url.Values{"customer": {binding.CustomerID}, "configuration": {configuration}, "return_url": {returnURL}}
	if err := s.request(ctx, http.MethodPost, "billing_portal/sessions", "", form, &response); err != nil {
		return Session{}, err
	}
	if !validID(response.ID, "bps_") || !testMode(response.Livemode) || response.Customer != binding.CustomerID || !sessionURL(response.URL, "billing.stripe.com") {
		return Session{}, errors.New("stripe returned an invalid test portal session")
	}
	return Session{ID: response.ID, URL: response.URL}, nil
}
