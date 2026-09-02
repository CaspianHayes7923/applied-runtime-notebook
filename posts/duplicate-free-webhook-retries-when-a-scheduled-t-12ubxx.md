# Duplicate-free webhook retries when a scheduled ticket cleanup hits a rate-limited API

Use cron only as a clock. The pacing of a rate-limited cleanup API belongs to a durable queue and to the consumers draining it, and the guarantee you actually need — no support ticket purged twice, none silently skipped — belongs to a ledger row that the scheduler, the worker, and the webhook receiver all write against. In a customer support backend, deleting expired attachments is a compliance-driven state transition rather than a maintenance chore, so the interesting question is not throughput; it is which component owns the delivery guarantee at the moment the downstream API answers 429 and the partner's retry callback arrives for the second time.

The scheduler creates work. The consumer decides how fast that work touches somebody else's system.

## What breaks first when a cron job calls a rate-limited cleanup API directly?

Arithmetic breaks it first. A backlog of 40,000 expired attachments against a partner limit of 25 calls per second needs roughly 27 minutes of continuous calling even when nothing goes wrong, and a single scheduled HTTP invocation is a poor container for 27 minutes of somebody else's rate limiter. Managed schedulers cap invocation duration, load balancers close idle connections, and deploys restart the process in the middle of the sweep.

What you are left with is worse than a slow job: you are left with three classes of rows and no way to tell them apart. Some attachments were deleted and confirmed. Some were requested, and the response never came back — the connection dropped, or the process died between the call and the commit. The rest were never touched at all. The middle class is the expensive one, because a naive retry of the whole batch converts every unconfirmed row into a second outbound delete, and a partner that treats delete as non-idempotent will happily record two deletions against one object.

Retrying harder makes it worse. A tight loop against a limiter turns the partner's throttle into your own incident, and if the retry storm is loud enough you get a longer penalty window than the original backlog ever justified. `Retry-After` exists for exactly this and is worth honoring literally rather than replacing with your own guess.

Cron is still the right trigger. It just isn't the right worker, because a trigger has no memory of what it already attempted.

## The invariants a support-ticket purge has to preserve

Exactly-once delivery across a network isn't on offer. What is on offer is **at-least-once delivery plus idempotent effects**, and that combination is what an auditor will accept when they ask why object 4471 has two deletion records and one confirmation.

Four invariants carry most of the weight. First, the idempotency key must be derived from the business object and the purge epoch — `purge:2026-Q3:ticket_88213` — never from the attempt counter, because a redelivered message has to present the same key the first attempt presented. Second, intent is written before the call, not after it, so a crash leaves evidence rather than silence. Third, the ledger needs a genuine third state: pending, in_doubt, confirmed. Collapsing in_doubt into either neighbour is how systems either double-delete or quietly lose deletions, and reconciliation exists precisely to drain that middle state on a schedule of its own. Fourth, the inbound webhook receiver dedupes on the provider's event id with a unique index, because callbacks are at-least-once by construction and a duplicate callback must not advance the ledger twice.

There's a clock constraint that engineers routinely forget when they design backpressure. Under the GDPR, a controller answers an erasure request without undue delay and in any event within one month of receipt, and the storage limitation principle says personal data isn't kept in identifiable form longer than necessary. Backpressure that means "postponed indefinitely" is therefore not a neutral engineering choice; queue age is a compliance metric, and it deserves an alarm on the oldest unprocessed message rather than on average latency, which hides exactly the tail you're accountable for.

Alarm on the oldest item. Averages lie here.

## Comparing the control planes by the guarantee you inherit

The comparison that matters isn't cron against queues in the abstract. It's what each control plane hands you by default, before you write a single line of compensating logic.

| Control plane | What paces the API calls | Guarantee you inherit | Where it hurts |
| --- | --- | --- | --- |
| Cron invoking one HTTP handler | the handler's own loop | at-most-once per run, no resume | partial purge on timeout, no record of attempts |
| Cron enqueues, workers drain | worker concurrency and a token bucket | at-least-once per message | duplicate effects unless the consumer is idempotent |
| Database work table with `FOR UPDATE SKIP LOCKED` | poll interval and batch size | at-least-once, transactional with the ledger | you write the lease and visibility timeout yourself |
| Broker-backed worker framework (Celery and relatives) | prefetch, concurrency, task rate limits | depends on acknowledgement timing | a broker becomes a second system to operate and observe |
| Partner webhook drives the next batch | the partner's callback rate | at-least-once, and outside your control | no signal when callbacks stop; a sweeper is still required |

Celery's own introduction describes the broker-plus-worker split most teams inherit, and inside that split the acknowledgement flag decides which failure you keep: acknowledging before execution risks losing work when a worker dies mid-task, acknowledging after execution risks running it twice. Neither setting removes the ledger. It only moves the duplicate.

The row I'd defend for a support backend already running Postgres is the third one. `FOR UPDATE SKIP LOCKED` lets several workers lease disjoint rows from the same table without serializing on each other, the lease and the audit write land in one transaction, and there is no second durability story to reconcile at 3 a.m. The catch is that a polling work table has a throughput ceiling and lock churn that a purpose-built broker doesn't, so past a few thousand messages a second the argument flips. Most cleanup workloads are nowhere near that, and your mileage may vary with how chatty the purge protocol is.

## The critical path in Go: claim, record intent, pace, confirm

Below is the shape of the consumer. Helpers for the audit write are elided, but the ordering is the point: lease a bounded batch, spend one tick per outbound call, and let the ledger — not the HTTP client — decide what happened.

```go
package purge

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// claimBatch leases expired attachments to exactly one worker. SKIP LOCKED lets
// several workers poll the same table without queueing behind each other's rows.
const claimBatch = `
UPDATE attachment_purge
   SET state = 'in_flight',
       attempt = attempt + 1,
       leased_until = now() + interval '5 minutes'
 WHERE id IN (
   SELECT id FROM attachment_purge
    WHERE state IN ('pending', 'in_doubt')
      AND due_at <= now()
    ORDER BY due_at
      FOR UPDATE SKIP LOCKED
    LIMIT $1)
RETURNING id, ticket_id, external_id, purge_epoch`

type job struct {
	ID         int64
	TicketID   string
	ExternalID string
	Epoch      string
}

// Derived from the business object and the purge epoch, never from the attempt
// counter: a redelivered message must present the key its first attempt used.
func idempotencyKey(j job) string {
	return fmt.Sprintf("purge:%s:%s", j.Epoch, j.TicketID)
}

var errThrottled = errors.New("partner throttled the request")

func deleteAttachment(ctx context.Context, c *http.Client, j job) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		"https://partner.example.com/attachments/"+j.ExternalID, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Idempotency-Key", idempotencyKey(j))

	res, err := c.Do(req)
	if err != nil {
		return err // outcome unknown: the row belongs in in_doubt, not pending
	}
	defer res.Body.Close()

	switch {
	case res.StatusCode == http.StatusTooManyRequests:
		return fmt.Errorf("%w: wait %ds", errThrottled, retryAfter(res, 30))
	case res.StatusCode == http.StatusNotFound:
		return nil // already gone; a prior attempt got through
	case res.StatusCode >= 200 && res.StatusCode < 300:
		return nil
	}
	return fmt.Errorf("delete %s: status %d", j.ExternalID, res.StatusCode)
}

func retryAfter(res *http.Response, fallback int) int {
	if s, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && s > 0 {
		return s
	}
	return fallback
}

func runWorker(ctx context.Context, db *sql.DB, c *http.Client, perSecond int) error {
	tick := time.NewTicker(time.Second / time.Duration(perSecond))
	defer tick.Stop()

	for ctx.Err() == nil {
		jobs, err := claim(ctx, db, claimBatch, 100)
		if err != nil || len(jobs) == 0 {
			time.Sleep(2 * time.Second)
			continue
		}
		for _, j := range jobs {
			<-tick.C // the queue absorbs the backlog, not the partner's limiter
			switch err := deleteAttachment(ctx, c, j); {
			case err == nil:
				markConfirmed(ctx, db, j) // ledger row and audit entry, one transaction
			case errors.Is(err, errThrottled):
				release(ctx, db, j, backoff(j)) // stays claimable, same idempotency key
			default:
				markInDoubt(ctx, db, j, err) // reconciliation decides, the worker doesn't
			}
		}
	}
	return ctx.Err()
}
```

Three properties are doing the work. The 404 branch treats an already-deleted object as success, which is what makes a redelivered message harmless. The throttled branch releases the lease instead of burning attempts, so a partner that halves its limit for an hour produces a longer queue rather than a wave of failed rows. And the default branch refuses to guess: an unknown outcome is recorded as unknown, and a separate reconciliation pass compares the partner's state with the ledger before anything is retried or written off.

The same discipline covers the inbound side. When the partner posts a deletion-completed callback, the receiver verifies the signature, inserts the event id under a unique constraint, and returns 2xx quickly; the state transition happens in the worker's transaction, not in the HTTP handler. A callback that arrives twice hits the unique index and does nothing. A callback that never arrives is caught by the reconciliation sweep, which is the only reason the system tolerates an unreliable notification channel at all.

Observability follows from the state machine rather than from a dashboard template: oldest pending row, count of in_doubt rows by age, delete calls per second against the documented cap, and the ratio of 429s to successful calls. If in_doubt stops draining, you have a reconciliation defect, and no amount of worker autoscaling will hide it.

## When the queue is the wrong answer

If the cleanup never leaves your database — a `DELETE` over expired rows, no partner call, no callback — then cron plus one transactional statement is the correct design, and adding a queue buys you a redelivery policy, a retention window, and a second failure domain in exchange for a problem you don't have. Stick with the single statement. Re-running it is idempotent by construction, which is the property the whole queue apparatus was trying to buy you in the first place.

Queues are also not suitable as a workflow engine. When cleanup means a fan-in join across three partners, human approval, or compensation steps that must unwind a partial purge, a message queue lacks the durable timers and the execution history that engine gives you, and simulating them with retry counters produces a state machine nobody can audit. That is a genuinely different tool, and reaching for it is a scale-and-shape decision, not a fashion one.

For the ordinary case — a scheduled sweep, a rate-limited partner API, callbacks you don't control — the durable ledger is the load-bearing part. Cron decides when. The queue decides how fast. Neither of them decides what happened, and pretending otherwise is how a support backend ends up explaining duplicate deletions to a regulator.

## Further reading

- https://docs.celeryq.dev/en/stable/getting-started/introduction.html
- https://www.postgresql.org/docs/current/sql-select.html
- https://www.rfc-editor.org/rfc/rfc9110.html
- https://www.rfc-editor.org/rfc/rfc6585.html
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After
- https://gdpr-info.eu/art-12-gdpr/
- https://gdpr-info.eu/art-17-gdpr/
