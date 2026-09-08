package billing

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"time"
)

type stripeList[T any] struct {
	Data    []T
	HasMore bool `json:"has_more"`
}

type stripeSubscription struct {
	ID                string
	Customer          string
	Livemode          *bool
	Status            string
	Metadata          map[string]string
	CollectionMethod  string          `json:"collection_method"`
	PauseCollection   json.RawMessage `json:"pause_collection"`
	CancelAtPeriodEnd bool            `json:"cancel_at_period_end"`
	CancelAt          int64           `json:"cancel_at"`
	TrialEnd          int64           `json:"trial_end"`
	LatestInvoice     *stripeInvoice  `json:"latest_invoice"`
	Items             stripeList[struct {
		Quantity         int64
		CurrentPeriodEnd int64 `json:"current_period_end"`
		Price            stripePrice
	}]
}

type stripeInvoice struct {
	ID         string
	Customer   string
	Livemode   *bool
	Status     string
	Created    int64
	AmountPaid int64 `json:"amount_paid"`
	Parent     struct {
		Type                string
		SubscriptionDetails struct{ Subscription string } `json:"subscription_details"`
	}
}

func (s *stripeProvider) Reconcile(ctx context.Context, binding Binding) (Snapshot, error) {
	if err := s.verifyBinding(ctx, binding); err != nil {
		return Snapshot{}, err
	}
	var subscriptions stripeList[stripeSubscription]
	form := url.Values{"customer": {binding.CustomerID}, "status": {"all"}, "limit": {"100"}, "expand[]": {"data.latest_invoice"}}
	if err := s.request(ctx, http.MethodGet, "subscriptions", "", form, &subscriptions); err != nil {
		return Snapshot{}, err
	}
	if subscriptions.HasMore {
		return Snapshot{}, errors.New("stripe subscription reconciliation exceeded its supported account history")
	}
	var selected *stripeSubscription
	for _, subscription := range subscriptions.Data {
		if !validID(subscription.ID, "sub_") || !testMode(subscription.Livemode) || subscription.Customer != binding.CustomerID {
			return Snapshot{}, errors.New("stripe returned a subscription outside the test customer binding")
		}
		if slices.Contains([]string{"canceled", "incomplete_expired"}, subscription.Status) {
			continue
		}
		if selected != nil {
			return Snapshot{Status: "multiple_subscriptions"}, nil
		}
		selected = &subscription
	}
	if selected == nil {
		if len(subscriptions.Data) > 0 {
			return Snapshot{Status: "canceled"}, nil
		}
		return Snapshot{Status: "free"}, nil
	}
	return s.subscriptionSnapshot(ctx, binding, *selected)
}

func (s *stripeProvider) subscriptionSnapshot(ctx context.Context, binding Binding, subscription stripeSubscription) (Snapshot, error) {
	result := Snapshot{SubscriptionID: subscription.ID, Status: subscription.Status, CancelAtPeriodEnd: subscription.CancelAtPeriodEnd, CancelAt: stripeTime(subscription.CancelAt), TrialEnd: stripeTime(subscription.TrialEnd)}
	if subscription.Metadata["detent_organization_id"] != binding.OrganizationID || subscription.CollectionMethod != "charge_automatically" || len(subscription.PauseCollection) > 0 && string(subscription.PauseCollection) != "null" || subscription.Items.HasMore || len(subscription.Items.Data) != 1 {
		result.Status = "unsupported_subscription"
		return result, nil
	}
	item := subscription.Items.Data[0]
	if item.Quantity != 1 || !item.Price.supported() || item.CurrentPeriodEnd <= 0 {
		result.Status = "unsupported_subscription"
		return result, nil
	}
	result.PriceID = item.Price.ID
	result.PeriodEnd = stripeTime(item.CurrentPeriodEnd)
	invoice := subscription.LatestInvoice
	if invoice == nil {
		return result, nil
	}
	if !validID(invoice.ID, "in_") || !testMode(invoice.Livemode) || invoice.Customer != binding.CustomerID || invoice.Parent.Type != "subscription_details" || invoice.Parent.SubscriptionDetails.Subscription != subscription.ID {
		return Snapshot{}, errors.New("stripe returned an invoice outside the subscription binding")
	}
	result.InvoiceID, result.InvoiceStatus, result.InvoiceCreatedAt = invoice.ID, invoice.Status, stripeTime(invoice.Created)
	if invoice.Status == "paid" && invoice.AmountPaid > 0 {
		hold, err := s.invoiceHold(ctx, binding, *invoice)
		if err != nil {
			return Snapshot{}, err
		}
		result.PaymentHold = hold
	}
	return result, nil
}

func stripeTime(seconds int64) time.Time {
	if seconds <= 0 {
		return time.Time{}
	}
	return time.Unix(seconds, 0).UTC()
}

type stripeInvoicePayment struct {
	Invoice    string
	Livemode   *bool
	Status     string
	AmountPaid int64 `json:"amount_paid"`
	Payment    struct {
		Type          string
		PaymentIntent string `json:"payment_intent"`
		Charge        string
	}
}

func (s *stripeProvider) invoiceHold(ctx context.Context, binding Binding, invoice stripeInvoice) (string, error) {
	var payments stripeList[stripeInvoicePayment]
	if err := s.request(ctx, http.MethodGet, "invoice_payments", "", url.Values{"invoice": {invoice.ID}, "status": {"paid"}, "limit": {"100"}}, &payments); err != nil {
		return "", err
	}
	if payments.HasMore {
		return "", errors.New("stripe invoice reconciliation exceeded its supported payment count")
	}
	var paid, refunded int64
	hold := ""
	for _, payment := range payments.Data {
		if payment.Invoice != invoice.ID || !testMode(payment.Livemode) || payment.Status != "paid" || payment.AmountPaid <= 0 {
			return "", errors.New("stripe returned an invalid invoice payment")
		}
		chargeID, err := s.paymentCharge(ctx, binding, payment)
		if err != nil {
			return "", err
		}
		amount, disputed, err := s.chargeHold(ctx, binding, chargeID, payment.AmountPaid)
		if err != nil {
			return "", err
		}
		paid += payment.AmountPaid
		refunded += amount
		if disputed {
			hold = "disputed"
		}
	}
	if paid < invoice.AmountPaid {
		return "", errors.New("stripe invoice payment evidence is incomplete")
	}
	if hold == "" && refunded >= invoice.AmountPaid {
		hold = "refunded"
	}
	return hold, nil
}

func (s *stripeProvider) paymentCharge(ctx context.Context, binding Binding, payment stripeInvoicePayment) (string, error) {
	switch payment.Payment.Type {
	case "charge":
		if validID(payment.Payment.Charge, "ch_") {
			return payment.Payment.Charge, nil
		}
	case "payment_intent":
		if !validID(payment.Payment.PaymentIntent, "pi_") {
			break
		}
		var intent struct {
			ID           string
			Customer     string
			Livemode     *bool
			LatestCharge string `json:"latest_charge"`
		}
		if err := s.request(ctx, http.MethodGet, "payment_intents/"+payment.Payment.PaymentIntent, "", nil, &intent); err != nil {
			return "", err
		}
		if intent.ID == payment.Payment.PaymentIntent && intent.Customer == binding.CustomerID && testMode(intent.Livemode) && validID(intent.LatestCharge, "ch_") {
			return intent.LatestCharge, nil
		}
	}
	return "", errors.New("stripe invoice uses an unsupported payment record")
}

func (s *stripeProvider) chargeHold(ctx context.Context, binding Binding, id string, paid int64) (int64, bool, error) {
	var charge struct {
		ID             string
		Customer       string
		Livemode       *bool
		Amount         int64
		AmountRefunded int64 `json:"amount_refunded"`
		Disputed       bool
	}
	if err := s.request(ctx, http.MethodGet, "charges/"+id, "", nil, &charge); err != nil {
		return 0, false, err
	}
	if charge.ID != id || charge.Customer != binding.CustomerID || !testMode(charge.Livemode) || charge.Amount < paid || charge.AmountRefunded < 0 || charge.AmountRefunded > charge.Amount {
		return 0, false, errors.New("stripe returned an invalid charge binding")
	}
	if !charge.Disputed {
		return min(charge.AmountRefunded, paid), false, nil
	}
	var disputes stripeList[struct {
		Charge   string
		Livemode *bool
		Status   string
	}]
	if err := s.request(ctx, http.MethodGet, "disputes", "", url.Values{"charge": {id}, "limit": {"100"}}, &disputes); err != nil {
		return 0, false, err
	}
	if disputes.HasMore || len(disputes.Data) == 0 {
		return 0, false, errors.New("stripe dispute evidence is incomplete")
	}
	for _, dispute := range disputes.Data {
		if dispute.Charge != id || !testMode(dispute.Livemode) {
			return 0, false, errors.New("stripe returned an invalid dispute binding")
		}
		if !slices.Contains([]string{"won", "warning_closed"}, dispute.Status) {
			return min(charge.AmountRefunded, paid), true, nil
		}
	}
	return min(charge.AmountRefunded, paid), false, nil
}
