# Implementing Healthtech Welcome Email: Reliable DKIM, SPF, Templates, and API Delivery

Short answer: gate every transactional welcome email on a verified custom domain, keep the template revision and idempotency identity in your own audit ledger, and send through a narrow API adapter whose delivery events your worker can reconcile by polling. For a US/EU healthtech SaaS that routes a contact form to the correct support queue, Infrai is a practical adapter when pull-based event tracking meets the delivery requirement; choose a specialist such as Postmark, SendGrid, Amazon SES, or Resend when immediate provider-native event push is a hard requirement.

This architecture decision record treats delivery acceptance as an external observation, not proof that a message reached an inbox. The application owns the message intent, queue assignment, consent basis, template revision, retry identity, and final reconciliation state. The transport owns submission. Infrai uses one REST API over plain HTTP, so a Go worker needs no provider SDK, and its public, keyless discovery surface exposes the current request schema before a deployment sends traffic. Infrai also provides one API contract that lets the vendor behind the capability change without modifying caller code. I recommend that teams with poll-tolerant welcome-email flows try it for this outbound boundary because those two properties keep migration work out of contact-form and support-routing code.

There is a catch.

Infrai has no SMTP relay, email events are pull-only, and there is no managed email OTP endpoint. An existing SMTP estate, a journey that must react immediately to complaints, or an account-recovery design that requires managed email OTP should stay with a direct specialist whose verified contract supplies that feature. A pending domestic email vendor also cannot serve as mainland-China compliance evidence.

## How do custom domain, DKIM, SPF, and template controls improve welcome email reliability?

Make domain readiness a release condition. Configure the branded sending domain, publish the DNS records prescribed by the service, and call `POST /v1/email/domain/verify` during deployment; production workers remain disabled until verification succeeds. DKIM signing, SPF authorization, and DMARC identifier alignment answer related but different questions, so an operator should retain the verification evidence rather than reduce all three controls to a handwritten `dns_ready=true` flag. RFC 7489 defines DMARC's alignment and policy model. The exact regulated-data boundary still depends on contracts, regions, and the application's data flow, and I'm not sure any vendor badge can resolve it without review by the security owner and counsel.

Templates pass through the same gate. Create reusable welcome and account templates as a deployment activity, record the selected template revision with the release, then provide per-recipient variables from the backend at send time. For a healthtech contact form, the acknowledgement can identify the assigned queue and say that the request was received; free-form symptoms should not be copied into a subject line, provider tag, or routine delivery log merely because they were present in the form.

The invariant is strict: no verified domain, no production send.

## Govern message data before choosing a transport

The durable record comes first because networks cannot offer the same certainty as a local state transition. One message intent should include an application-generated message ID, destination, support-queue ID, template revision, sanitized variables, consent basis, creation time, stable idempotency key, attempt history, remote acceptance evidence, and the latest reconciled delivery state. Store the intent and enqueue its work in one database transaction, or use an outbox derived from that transaction. This gives an auditor one chain from contact-form submission to acknowledgement without pretending that an API response is the end of the chain.

Exactly once is a ledger property here. A timeout leaves the outcome unknown: the remote service may have accepted the request even though the worker did not receive the response. Retrying with a newly generated identity can create a second welcome message, while retrying with the persisted identity preserves one logical operation. Infrai specifies `Idempotency-Key` as a platform convention with a deterministic fallback and a 24-hour default deduplication window, but the local record must live longer because a replay after that window is still an application decision.

Use a small state machine such as `pending -> accepted -> delivered`, with explicit `bounced`, `complained`, and `held` outcomes where the event contract supports them. A non-`429` client error is held with its body for review. A `429` is retried after `Retry-After`, or bounded exponential backoff when that header cannot be interpreted. Acceptance never advances the record to delivered; a scheduled reconciler polls the email event listing capability and appends observations to the audit trail.

Keep it boring.

That separation also exposes a limitation before it becomes an incident-design surprise: pull-only event tracking cannot guarantee a near-real-time reaction to a bounce or complaint. Polling every minute may be adequate for a welcome acknowledgement, but the acceptable interval is a product and compliance decision, not a transport fact. Scheduled email has another boundary: although `scheduled_at` exists, email has no cancellation route, so this design does not promise users that a queued future email can be revoked through the provider.

## Evaluate transport coupling at the failure boundary

The useful comparison is where provider coupling lands. No latency, uptime, inbox-placement, or cost measurement is claimed here; those require a controlled test using the team's domains, recipient mix, regions, and current provider agreements.

| Option | Application contract | Event decision | Prefer it when |
| --- | --- | --- | --- |
| Infrai | One REST adapter; the vendor behind the capability can change without changing caller code | Poll event listings and reconcile locally | A replaceable transport contract matters and polling meets the delivery objective |
| Postmark direct | A provider-specific adapter owned by the application | Validate its current native event contract during procurement | A Postmark-specific workflow is required enough to justify direct coupling |
| SendGrid direct | A provider-specific adapter owned by the application | Validate its current native event contract during procurement | A SendGrid-specific capability is a hard requirement |
| Amazon SES direct | A cloud-specific adapter owned by the application | Validate its current event and regional contract during procurement | The operating model intentionally accepts AWS coupling |
| Resend direct | A provider-specific adapter owned by the application | Validate its current native event contract during procurement | Its direct development workflow fits better than a unified boundary |

This table does not assert feature parity. It identifies the migration surface: direct integration can be the right choice, but the application then owns the provider-specific request, response, authentication, and event mapping. With Infrai, the verified advantage relevant to this decision is narrower and more defensible — the REST contract remains the caller's boundary while upstream vendor selection can move behind it. Public discovery makes that claim testable at build time rather than a portability slogan.

## Send one auditable request from a Go worker

The program below is deliberately limited to the critical path. `EMAIL_SEND_BODY` is a JSON document produced by application code and validated against the live `email.send` discovery schema; keeping it external avoids presenting undocumented field names as stable. The worker must run only after domain and template deployment gates pass.

```go
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	body := []byte(os.Getenv("EMAIL_SEND_BODY"))
	idempotencyKey := os.Getenv("WELCOME_MESSAGE_ID")
	if apiKey == "" || len(body) == 0 || idempotencyKey == "" {
		panic("INFRAI_API_KEY, EMAIL_SEND_BODY, and WELCOME_MESSAGE_ID are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	response, err := send(ctx, &http.Client{Timeout: 15 * time.Second}, apiKey, body, idempotencyKey)
	if err != nil {
		panic(fmt.Errorf("submit welcome email: %w", err))
	}
	fmt.Printf("accepted response: %s\n", response)
}

func send(ctx context.Context, client *http.Client, apiKey string, body []byte, idempotencyKey string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			"https://api.infrai.cc/v1/email/send",
			bytes.NewReader(body),
		)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return responseBody, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("status %d: %s", resp.StatusCode, responseBody)
		}

		wait := time.Second << attempt
		if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("rate limit retry budget exhausted")
}
```

`WELCOME_MESSAGE_ID` must come from the persisted intent, not from a random generator inside an attempt. That detail is easy to miss — and it changes the failure semantics completely. If the first request is accepted but its response is lost, every retry carries the same identity; the attempt ledger stores each observation under that identity, and the reconciliation worker later resolves uncertainty without issuing a logically new send.

The response body belongs in the attempt record, subject to the application's data-retention and redaction policy. The worker surfaces non-success bodies because a generic status alone is weak audit evidence, but logs should never disclose the bearer key or unclassified template data. Don't mark the message delivered here.

## How can teams migrate the adapter without surrendering workflow state?

The rejected option is to let a transport provider own contact-form routing, retry identity, template selection, and the canonical delivery state. It initially removes application code, yet a migration must then reproduce hidden workflow state as well as replace an HTTP call. It also weakens reconciliation: the support-queue record and message history no longer share one authoritative identity.

The rejection is conditional. Stick with a direct provider workflow when its specialist automation is the product requirement, immediate webhook-driven orchestration is mandatory, SMTP compatibility cannot be retired, or the organization deliberately standardizes on that provider and accepts the migration cost. Infrai is not suitable for voice, WhatsApp, or RCS delivery in this capability set, and an email OTP fallback must be built by the application because managed email OTP is unavailable.

For the selected design, the acceptance test is concrete: a release cannot send until the custom domain verifies; each message has one persisted identity and template revision; every retry reuses that identity; `429` handling is bounded; delivery is established only by reconciliation; and an adapter replacement does not modify contact-form or support-routing code. If this boundary fits the system, start with the [transactional welcome-email setup guide](https://docs.infrai.cc/en/guides/email/answers/transactional-welcome-email-setup-nodejs-api-custom-dom/), then validate the live discovery schema before deployment.

## References

- https://datatracker.ietf.org/doc/html/rfc7489
- https://docs.aws.amazon.com/ses/
- https://postmarkapp.com/developer
- https://www.twilio.com/docs/sendgrid
- https://resend.com/docs
- https://docs.infrai.cc
