package billing

import (
	"context"
	"time"
)

type Binding struct {
	AccountID      string
	CustomerID     string
	OrganizationID string
}

type CheckoutRequest struct {
	Binding
	PriceID        string
	IdempotencyKey string
	ReturnURL      string
	ExpiresAt      time.Time
}

type Session struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	ExpiresAt time.Time `json:"expires_at"`
}

type Snapshot struct {
	SubscriptionID    string    `json:"subscription_id"`
	PriceID           string    `json:"price_id"`
	Status            string    `json:"status"`
	InvoiceID         string    `json:"invoice_id"`
	InvoiceStatus     string    `json:"invoice_status"`
	InvoiceCreatedAt  time.Time `json:"invoice_created_at"`
	PeriodEnd         time.Time `json:"period_end"`
	TrialEnd          time.Time `json:"trial_end"`
	CancelAt          time.Time `json:"cancel_at"`
	CancelAtPeriodEnd bool      `json:"cancel_at_period_end"`
	PaymentHold       string    `json:"payment_hold"`
}

type Provider interface {
	Reconcile(context.Context, Binding) (Snapshot, error)
	Checkout(context.Context, CheckoutRequest) (Session, error)
	Portal(context.Context, Binding, string, string) (Session, error)
}
