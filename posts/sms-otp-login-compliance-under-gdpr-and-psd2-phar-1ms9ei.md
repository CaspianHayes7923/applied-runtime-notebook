# SMS OTP Login Compliance under GDPR and PSD2 (Pharmacy Refill Alerts)

Short answer: SMS OTP is not a universal compliance answer for pharmacy refill alerts. It can be one possession factor in a proportionate login design, but GDPR, PSD2, and NIST ask different questions, and SMS remains exposed to phishing, number reassignment, and SIM-swap risk. Use a phishing-resistant option for higher-risk access, keep the alert itself sparse, and preserve evidence of each authentication decision.

For a B2B SaaS operator, the deliverable is larger than a six-digit code. The system has to decide which event is a low-risk reminder, which action exposes health data or initiates a regulated payment, and which evidence an auditor can later reconcile. A generated compliance report sent as an email attachment may help reviewers, but the attachment should be an export of an authoritative audit trail, not the audit trail itself.

That distinction matters.

## EU and US compliance is a governance boundary

No single rule says that every pharmacy refill login must use SMS OTP, or that SMS OTP makes the workflow compliant. GDPR Article 32 requires security appropriate to risk and names measures such as encryption, resilience, restoration, and regular testing; it does not certify a particular authentication channel. In the United States, the HIPAA Security Rule requires access controls for electronic protected health information, while the exact safeguard depends on the covered workflow and risk analysis. I'm not sure a particular reminder contains protected health information until the message template, recipient relationship, logs, and downstream links are mapped; privacy and legal review resolve that uncertainty.

PSD2 is narrower than many architecture diagrams imply. Its strong customer authentication rules concern payment-service access and regulated payment actions, not every pharmacy notification. Where PSD2 SCA applies, authentication generally needs at least two independent elements drawn from knowledge, possession, and inherence. An SMS code alone is possession, not two-factor authentication. A password plus an SMS code can supply two categories, but remote electronic payment initiation can also invoke dynamic-linking requirements that bind the authentication code to the amount and payee. A generic refill-login OTP doesn't do that job.

NIST SP 800-63B treats use of the public switched telephone network for out-of-band authentication as restricted and calls for risk indicators such as SIM change, number porting, and other abnormal behavior. It also distinguishes phishing-resistant cryptographic authentication from codes a user can relay to an impostor. NIST guidance is not, by itself, an EU or US pharmacy statute; it is a useful engineering baseline and a precise warning against equating code delivery with phishing resistance.

So the decision boundary is concrete: SMS can remain available for a low-risk reminder or as one factor in a stepped-up flow, provided the risk assessment accepts the channel. It is not suitable as the only control for administrator access, recovery of a high-value account, disclosure of sensitive refill details, or a payment action requiring stronger evidence. For those paths, prefer a phishing-resistant cryptographic authenticator; retain an accessible alternative and a carefully governed recovery process.

## Privacy exposure survives reliable SMS delivery

A refill alert crosses several trust boundaries before anyone enters an OTP: the dispensing system emits an event, the SaaS tenant selects a template, a messaging service addresses a telephone number, a carrier handles the message, and a browser or app receives the click. Authentication protects only part of that chain. A code does nothing to minimize sensitive text on a locked screen, correct a stale telephone number, or prevent a tenant operator from exporting excessive data.

Keep the notification deliberately uninteresting: identify the service only to the degree the recipient expects, avoid medication names and diagnosis clues, and direct the user to an authenticated session for details. GDPR's data-minimization principle supports this design, while CTIA messaging guidance makes consent, opt-out handling, and predictable sender behavior part of responsible application-to-person messaging in the US ecosystem. Consent to receive a refill reminder and proof of account control are separate records. Don't collapse them into one boolean.

A useful threat model separates at least four events:

1. Sending a reminder to a verified destination with no sensitive detail.
2. Opening refill details after a normal login.
3. Changing the telephone number or recovering the account.
4. Approving a payment or another high-impact action.

The catch is operational complexity. Passkeys or hardware-backed credentials reduce phishing exposure, yet recovery, shared devices, accessibility, and enrollment support still need design work. TOTP avoids carrier delivery but remains phishable. SMS has broad reach, but number lifecycle and carrier dependence make it a poor root of trust. There is no honest one-control answer.

## What should an SMS OTP login audit trail prove under GDPR and PSD2?

Exactly-once delivery is not a promise an external messaging network can give an application. The safer model is at-least-once processing with an idempotent state transition: retries may resend a request, but only one challenge record can consume the successful verification, and only one protected action can commit against that challenge. The audit trail must distinguish `challenge_created`, `delivery_requested`, `verification_failed`, `challenge_consumed`, and `action_committed`; otherwise a later report cannot tell a carrier retry from a second user action.

Do not store the OTP itself in logs or reports. Store a keyed digest or provider-neutral challenge identifier, timestamps, tenant and account identifiers, the assessed risk tier, factor type, policy version, attempt count, and terminal outcome. Set an explicit expiry and cap attempts according to the threat model. A value such as 10 minutes is a policy choice, not a compliance constant, so record the policy version that produced it.

The following Go shape keeps verification and the protected action behind a single transactional boundary. The interfaces are intentionally generic: the important contract is conditional consumption, not an SDK.

```go
package auth

import (
    "context"
    "errors"
    "time"
)

var ErrChallengeRejected = errors.New("challenge rejected")

type VerifyCommand struct {
    TenantID     string
    AccountID    string
    ChallengeID  string
    PresentedOTP string
    ActionID     string
    Now          time.Time
}

type Store interface {
    // ConsumeAndCommit succeeds once for a challenge and action pair.
    ConsumeAndCommit(ctx context.Context, cmd VerifyCommand) (bool, error)
    AppendAudit(ctx context.Context, event AuditEvent) error
}

type AuditEvent struct {
    TenantID    string
    AccountID   string
    ChallengeID string
    ActionID    string
    Outcome     string
    Policy      string
    OccurredAt  time.Time
}

func Verify(ctx context.Context, store Store, cmd VerifyCommand) error {
    committed, err := store.ConsumeAndCommit(ctx, cmd)
    if err != nil {
        return err
    }
    if !committed {
        return ErrChallengeRejected
    }

    return store.AppendAudit(ctx, AuditEvent{
        TenantID: cmd.TenantID, AccountID: cmd.AccountID,
        ChallengeID: cmd.ChallengeID, ActionID: cmd.ActionID,
        Outcome: "challenge_consumed", Policy: "refill-login-v3",
        OccurredAt: cmd.Now,
    })
}
```

In a production ledger, the state change and durable audit event belong in one database transaction or transactional outbox. The later email attachment is generated from that ledger, includes a report ID, covered time window, policy versions, event counts, and a content hash, and is sent only to an approved compliance mailbox. If mail is retried, use the report ID as the idempotency key. Keep the canonical report in controlled storage under the retention policy; ordinary email forwarding and mailbox deletion make email a weak system of record.

The join is the evidence.

Consider a hypothetical refill request with action ID `refill-7f2`, challenge ID `otp-a91`, and policy `refill-login-v3`. Two delivery requests may exist because the first carrier acknowledgment arrived late, but there must still be one challenge creation, one successful consumption, and one committed business action. The report generator should retain both delivery attempts, link them to the same challenge, prove that consumption occurred before expiry, and show that the tenant and account on the action match those on the challenge. It should also show the applicable consent record without pretending that messaging consent authorized disclosure of refill details. If the attachment merely reports "one OTP sent, one refill approved," it erases the retry, the policy decision, and the identity joins an auditor needs. This example is intentionally provider-neutral — swapping a messaging route must not rewrite the evidence model or change the meaning of a successful authentication event.

## Test replay, reassignment, and concurrency

A green delivery receipt proves little. Test number reassignment, a recent SIM change signal when available, repeated requests, delayed messages arriving after expiry, replay after successful consumption, tenant crossover, clock skew, and concurrent verification. Also test the uncomfortable path: the user receives a convincing phishing prompt and voluntarily relays the current code. SMS OTP cannot distinguish that relay from the intended browser, which is why sensitive actions need transaction context or phishing-resistant authentication.

The compliance evidence should reconcile three ledgers: authentication challenges, messaging attempts, and protected business actions. Counts will not always match one-to-one. One challenge may have multiple delivery attempts; an expired challenge has no committed action; a committed refill request must point to exactly one consumed challenge when policy required step-up. Alert on unexplained joins rather than forcing the totals to look tidy.

Use synthetic accounts in deployment tests, never real patient destinations. Exercise opt-out suppression separately from authentication, verify that logs redact message bodies and presented codes, and confirm that report generation respects tenant boundaries. A `401` after a wrong or expired code is an expected client-visible rejection, while the internal event should identify the policy reason without leaking it to an attacker. Short and plain.

## Migrate one risk tier at a time

Start with an inventory, not a forced migration. Classify reminder delivery, detail access, account recovery, staff administration, and payment approval; attach a required assurance level and governing rationale to each. Then deploy immutable audit events and reconciliation before changing factors, because otherwise the team cannot compare rejection, recovery, and abandonment patterns across policy versions.

For low-risk reminders, preserve SMS where its reach is necessary while minimizing message content. For detail access, offer stronger authentication and step up when risk changes. For recovery, don't let the same telephone number both trigger and approve its own replacement. For administrator and high-impact actions, require phishing-resistant authentication and maintain a separately controlled recovery route.

Review the policy after material changes in regulation, threat signals, messaging routes, or the exposed data. GDPR risk analysis, PSD2 payment scope, HIPAA applicability, and NIST assurance guidance are related inputs, not interchangeable badges. The defensible outcome is a traceable decision: what the system knew, which policy it applied, which independent factor or factors succeeded, which action committed, and where the evidence was retained.

## References

- https://eur-lex.europa.eu/eli/reg/2016/679/oj
- https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32018R0389
- https://pages.nist.gov/800-63-4/sp800-63b.html
- https://www.hhs.gov/hipaa/for-professionals/security/laws-regulations/index.html
- https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms
