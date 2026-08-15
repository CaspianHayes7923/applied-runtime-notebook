# Regional Runtime Decision for Node.js SaaS: Chat API Article Summaries

Data residency, replay semantics, and context limits change this decision before model quality or token price enters the discussion. Short answer: a Node.js SaaS that summarizes long articles for US and EU tenants should put a small, region-aware operation contract in front of a chat completions API, start with one backend that passes a corpus test, and add a gateway only when measured routing or compliance requirements justify it.

The unit of correctness is not the HTTP request. It is the committed summary of one immutable source version under one policy version. A timeout can leave the request outcome unknown; a retry can repeat computation; a deployment can alter a prompt without changing the article. Treating those events as ordinary request plumbing creates summaries that cannot be reconciled later.

This note records an architecture decision, not a provider ranking.

## Decision, invariants, and failure boundaries

The decision is to expose an internal `Summarize` operation whose inputs include tenant, source identity, source version, policy version, required processing region, and an idempotency key. The first implementation may call a single managed chat API. The application still owns the operation record, while provider-specific request fields remain inside one adapter. That boundary is small enough to test and substantial enough to prevent model names, retry rules, and truncation behavior from leaking into route handlers and queue consumers.

Four invariants define success. First, the same tenant, source version, and policy version identify one logical summary operation. Second, one operation may have several attempts but only one committed result. Third, the resolved region and runtime configuration are recorded before content leaves the service boundary. Fourth, an input that exceeds the proven context policy is rejected or routed through an explicit chunking workflow; it is never silently truncated.

Exactly once is an outcome here, not a network claim.

The audit record should contain an input digest, output digest, policy version, adapter configuration identifier, resolved region, attempt timestamps, and terminal disposition. It should not contain credentials, and it should retain source text or generated text only when the service's data policy requires that evidence. This resembles a ledger because the same questions apply: what was authorized, what happened, which attempt won, and can the result be explained without reconstructing state from application logs? Compliance obligations differ by jurisdiction, contract, and data class, so a regional endpoint label alone is insufficient evidence; counsel and the organization's retention policy must determine what may be stored and for how long.

Failure boundaries follow the operation. A connection loss after the remote runtime accepts work leaves the attempt unresolved, not failed. The reconciler must inspect the local operation record before another worker commits anything. A `401` is different: it is a terminal configuration or authorization signal for that attempt and should not enter a rapid retry loop. A client-side `429` policy may permit delayed retry, but only under the same operation key and with the retry decision recorded. Don't let an HTTP library decide business semantics from status codes alone.

## How should a Node.js SaaS choose a chat API for long article summarization in the US and EU?

Begin with disqualifiers. For each tenant class, document acceptable processing regions, retention and training terms, maximum source size, required completion time, and whether human review is available. Then obtain written evidence for the candidate runtime's processing and retention boundaries. I'm not sure a generic "EU available" label resolves every subprocessor question; the contract, data-processing terms, and an effective-region check in each deployment are what would resolve that uncertainty.

Next, evaluate the real corpus rather than a convenient sample. Freeze a set that includes the shortest accepted item, the longest accepted item, repeated sections, tables, malformed markup, contradictory passages, and documents close to the service's size ceiling. Version the evaluation set and scoring policy. Review factual coverage, unsupported assertions, omission of required sections, and format validity, then measure p50 and p95 completion time, retry count, input expansion from chunk overlap, and manual-review rate. Those numbers belong to the service's own workload; publishing invented universal benchmarks would be less useful than showing the method.

The comparison should remain compact:

| Runtime shape | Appropriate when | Principal limitation | Evidence required |
|---|---|---|---|
| Direct managed API behind one adapter | One backend satisfies region, context, quality, and continuity requirements | Provider-specific behavior still exists inside the adapter | Corpus results, contract terms, region verification, replay tests |
| Internal router with several adapters | Tenant or continuity rules require distinct backends | Routing policy, evaluation, and reconciliation become owned software | Per-route corpus results, deterministic policy versions, failover drills |
| Self-hosted gateway | The team can own deployment, upgrades, capacity, and on-call response | Control comes with operational responsibility | Load tests, upgrade procedure, security review, recovery objectives |

Price is a constraint, but advertised token rates are not total cost. Chunk overlap, repeated attempts, evaluation traffic, audit storage, network transfer, manual review, and gateway operations all belong in the denominator. A simple direct adapter can therefore be the sound choice for a narrow workload even when a routing layer appears more flexible, while a multi-region regulated workload may justify the router despite its larger control surface.

Streaming deserves a separate decision. Server-Sent Events provide a one-way event stream from server to client, which can improve perceived latency for an interactive UI, but partial tokens do not make a batch summary more correct. If the product streams, specify cancellation, partial-display, reconnect, and final-commit semantics; otherwise, keep the durable result path independent of the browser connection.

## Critical path: one committed summary, many possible attempts

The critical path derives a stable operation key, checks for an existing commit, invokes a narrow runtime interface, and commits through a uniqueness boundary. This Go example models the server-side component behind a Node.js product; the wire format is deliberately absent because correctness should not depend on one commercial schema. A real store must implement `Commit` and `GetCommitted` with a unique constraint and transactional behavior.

```go
package summary

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

var ErrAlreadyCommitted = errors.New("summary operation already committed")

type Request struct {
	TenantID     string
	SourceID     string
	SourceVersion string
	PolicyVersion string
	Region       string
	Text         string
}

type Result struct {
	Text       string
	RuntimeRef string
}

type Record struct {
	OperationKey string
	InputDigest  string
	OutputDigest string
	PolicyVersion string
	Region       string
	Result       Result
}

type Runtime interface {
	Summarize(context.Context, Request) (Result, error)
}

type Store interface {
	GetCommitted(context.Context, string) (Record, bool, error)
	Commit(context.Context, Record) error
}

func Execute(ctx context.Context, req Request, runtime Runtime, store Store) (Result, error) {
	key := digest(req.TenantID + "\x00" + req.SourceID + "\x00" +
		req.SourceVersion + "\x00" + req.PolicyVersion)

	if prior, ok, err := store.GetCommitted(ctx, key); err != nil {
		return Result{}, err
	} else if ok {
		return prior.Result, nil
	}

	result, err := runtime.Summarize(ctx, req)
	if err != nil {
		return Result{}, err
	}

	record := Record{
		OperationKey: key,
		InputDigest: digest(req.Text),
		OutputDigest: digest(result.Text),
		PolicyVersion: req.PolicyVersion,
		Region: req.Region,
		Result: result,
	}
	if err := store.Commit(ctx, record); err == nil {
		return result, nil
	} else if !errors.Is(err, ErrAlreadyCommitted) {
		return Result{}, err
	}

	winner, ok, err := store.GetCommitted(ctx, key)
	if err != nil || !ok {
		return Result{}, ErrAlreadyCommitted
	}
	return winner.Result, nil
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
```

The remote computation can occur twice during a race, yet the store accepts one business result. Tests should run two workers against the same key, cancel after invocation but before commit, replay after a lost response, change only the policy version, reject a region mismatch before invocation, and verify that an oversized article follows the declared whole-document or chunking policy. This is also where auditability earns its keep: an operator can distinguish duplicate computation from duplicate commitment without reading raw article content.

Chunking requires its own deterministic policy version. Record normalization rules, chunk boundaries, overlap, per-chunk prompt identity, and final synthesis identity. A partial set of chunk outputs is intermediate state, not a summary. If synthesis fails, replay under the same operation and policy; if the policy changes, create a new logical operation so the old output remains explainable.

## Rejected option, and when it is still correct

The rejected design lets every Node.js route handler build chat completions payloads, select runtime identifiers, truncate input, and retry independently. It reduces the first integration to a few lines, but it distributes policy across endpoints and workers; one path may retry a timeout, another may accept a partial result, and neither can prove which prompt version produced the committed summary. Reconciliation becomes guesswork.

The catch is that an internal router is not automatically better. It is not suitable when one direct adapter already meets the region and continuity requirements, the team has no measured need for dynamic routing, or gateway operations would exceed the risk being controlled. Stick with the direct adapter in that case, keep its fields in one package, and retain the operation ledger outside it. A self-hosted gateway is also the wrong choice when the team cannot own upgrades, capacity planning, security response, and regional deployment; open source makes inspection and modification possible, but it does not transfer operational accountability away from the operator.

Revisit the decision when a new tenant jurisdiction appears, the maximum article size changes, the retention contract changes, evaluation quality crosses a recorded threshold, or measured retry and chunking overhead alters the cost model. The durable conclusion is intentionally vendor-neutral: choose the smallest runtime boundary that preserves regional policy, deterministic evaluation, idempotent commitment, and an audit trail that can reconcile every accepted summary.

## References

- https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
- https://github.com/BerriAI/litellm
