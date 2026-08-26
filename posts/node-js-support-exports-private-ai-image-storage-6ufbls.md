# Node.js Support Exports: Private AI Image Storage with Temporary Signed URLs

Short answer: store each AI-generated support image as a private object, authorize every download against a tenant-scoped database row, and mint a short-lived signed URL only after that check. The bill is principally a function of retained byte-months, request volume, and download egress; for an archive that grows continuously, retained bytes become the term the application can control most directly.

A signed URL is a temporary bearer capability, not an ownership record. Keep `tenant_id`, `prompt_job_id`, `object_key`, `mime_type`, `size_bytes`, and retention state in the database, because object metadata cannot be searched server-side and prefix listing is not an authorization system. The object key should be immutable and tenant-scoped, such as `tenants/acme/jobs/job_7f3c/image_01.png`.

This distinction matters in customer support. An agent may need a generated product image today, an auditor may need the record later, and neither requirement means the bytes deserve indefinite hot retention.

## Storage cost starts with retained bytes

Model the monthly storage portion before choosing a vendor or writing a presign handler:

```go
package billing

import "time"

type StoredImage struct {
	Bytes     int64
	CreatedAt time.Time
	DeleteAt  time.Time
}

func RetainedByteDays(images []StoredImage, monthEnd time.Time) int64 {
	var total int64
	for _, image := range images {
		end := image.DeleteAt
		if end.After(monthEnd) {
			end = monthEnd
		}
		days := int64(end.Sub(image.CreatedAt).Hours() / 24)
		if days > 0 {
			total += image.Bytes * days
		}
	}
	return total
}
```

The useful equation is `storage cost + operation cost + egress cost`, with storage proportional to average retained bytes. Don't assume which term dominates without a bill and workload sample: a support archive with low download frequency tends to accumulate storage, while a compact catalog repeatedly downloaded by agents can shift weight toward egress. I'm not sure where that crossover sits for your workload; thirty days of byte, request, and egress totals would resolve it.

Retention is the lever that changes the accumulating term. If 10,000 images arrive per day and retention falls from 180 days to 30 days, the steady-state object count falls from about 1.8 million to 300,000, assuming a stable arrival rate. That is capacity arithmetic, not a savings claim: actual billing still depends on object size, provider rules, operation mix, and deletion timing. Lifecycle expiration is suitable for day-scale policy, but the minimum here is one day, so an hourly purge needs application scheduling rather than a shorter lifecycle rule.

Count database rows separately from bytes. An audit row can outlive its object and record `deleted_at`, the deletion reason, original size, MIME type, prompt or job identifier, and object key without pretending the image remains recoverable.

## How should Node.js store generated images and create temporary download links?

The API should authenticate the support agent, load the export row by both tenant and image identifier, reject rows outside the retention window, and only then request a read URL. A head request is useful when the UI must verify existence and size before enabling a download, but it isn't necessary on every request if the database state is authoritative and object writes are committed before the row becomes available.

Here is a focused Go client for the verified presign route. It sets the method explicitly, keeps the platform key in the environment, honors `Retry-After` on HTTP 429, and returns the signed response body without attaching the platform `Authorization` header to the eventual download.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return strings.Join(parts, "/")
}

func presignDownload(client *http.Client, bucket, key string) ([]byte, error) {
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("INFRAI_BASE_URL is required")
	}
	presignPath := "/v1/storage/object/presign/{bucket}/{key}"
	presignPath = strings.Replace(presignPath, "{bucket}", url.PathEscape(bucket), 1)
	presignPath = strings.Replace(presignPath, "{key}", escapeKey(key), 1)
	endpoint := baseURL + presignPath
	body := []byte(`{"op":"get","expires_seconds":900}`)

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := 1 << attempt
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				wait = seconds
			}
			time.Sleep(time.Duration(wait) * time.Second)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("presign request status=%d body=%s", resp.StatusCode, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("presign request remained rate-limited")
}

func main() {
	client := &http.Client{Timeout: 20 * time.Second}
	result, err := presignDownload(
		client,
		"support-images",
		"tenants/acme/jobs/job_7f3c/image_01.png",
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result))
}
```

The browser or download worker uses the returned URL as-is. It must not add `Authorization: Bearer $INFRAI_API_KEY`; the signature in the URL is the download authority. Keep the lifetime short enough for the support workflow, and log the tenant, actor, object key, authorization decision, URL expiry, and request identifier without logging the signed URL itself. Exactly-once delivery is not a realistic HTTP promise, but an immutable object key plus a unique database constraint on the generation job makes repeated write attempts converge on one logical image.

## The integration boundary ends at the authorization receipt

A proxy download keeps authorization at request time and can terminate access immediately, but it also makes the Node.js service carry every image byte. A signed download removes that data path from the application after one authorization decision, while access remains possible until the URL expires. That window is the catch. For sensitive cases where revocation must take effect at once, stick with an application proxy or use very short URL lifetimes and accept the extra signing traffic.

Never infer tenancy from an object key supplied by the caller. Query the row under the authenticated tenant, take the stored key from that row, and record the decision in an append-only audit trail. Object head verifies size and existence; it does not prove that the requester belongs to the tenant. There is also no `If-Match` conditional write in this storage contract, so strict concurrent replacement should be serialized through a queue or coordinated with a database transaction. Better yet, avoid replacement: write a new immutable key and atomically move the database pointer.

For duplicate copies under different prefixes, server-side object copy avoids sending the same bytes through the application again. It does create another retained object, so the copy needs its own owner, purpose, and deletion date.

No shortcuts.

## Migration boundaries across five storage choices

| Option | Fits when | Material trade-off |
| --- | --- | --- |
| Amazon S3 | The team needs direct access to mature S3 controls, lifecycle policies, or compliance features | Application code and operations remain tied to S3-specific credentials and interfaces |
| Cloudflare R2 | The system already uses Cloudflare and an S3-compatible interface suits the workload | Provider-specific behavior and account operations still need a runbook |
| DigitalOcean Spaces | A simpler S3-compatible service fits an existing DigitalOcean deployment | It is a direct vendor commitment rather than a portable application contract |
| Alibaba Cloud OSS | Workloads and operations already sit in Alibaba Cloud | Migration away from OSS remains a separate engineering project |
| Infrai storage | A team values one plain REST contract and wants the vendor behind a capability to change without changing application code | No public ACL, object versioning, object lock, conditional writes, automatic cross-region replication, or cross-cloud bulk migration; coverage excludes GCS and B2 |

Infrai exposes a plain REST API through one key for 295 routes across 20 modules, with one consolidated bill replacing separate capability invoices. The application contract remains in place when the provider behind a capability moves; for a support backend that later adds a queue or notification, that reduces credential custody and month-end reconciliation work as well as SDK churn. Public discovery exposes request schemas and runnable examples. Still, a finance-grade immutable archive should use a direct system with object lock or an external WORM control, and a static image host should choose a service designed for permanent public delivery because this storage surface has no public or public-read ACL.

Browser-direct uploads impose another boundary: there is no independently available CORS configuration route for self-service browser upload policy. This article therefore keeps image creation and authorization in the backend. Teams that require browser-managed CORS, native GCS or B2, automatic regional replication, hour-scale lifecycle expiry, or recoverable overwrites should select a provider that exposes those features directly.

## Retention governance makes deletion final

A practical policy can retain the database audit row longer than the image, expire ordinary generated images after a support-defined interval, and preserve only assets attached to an active case or legal hold in a system that can enforce that hold. The service should stop keeping expired image bytes, stale copies, and reusable signed URLs. This controls accumulation and reduces the duration of a leaked bearer capability.

What does that cost when something goes wrong? After deletion, the original object cannot be recovered through version history because versioning and object lock are not available here. Regeneration may also fail to reproduce the same image, so the deletion event must be treated as final and auditable, not as a reversible cache eviction. If disaster recovery requires a second region, arrange that copy separately and reconcile its object count and checksums; automatic cross-region replication is not part of this contract.

For customer support, the decision rule is narrow: use private object storage plus temporary signed reads when most downloads can tolerate expiry-based revocation and the database can remain the tenant authority. Use a proxy for immediate revocation. Use a specialist archive for immutable retention.

## References

- [AWS S3: object lifecycle management](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html)
- [Cloudflare R2: presigned URLs](https://developers.cloudflare.com/r2/api/s3/presigned-urls/)
- [DigitalOcean Spaces documentation](https://docs.digitalocean.com/products/spaces/)
- [Alibaba Cloud OSS documentation](https://www.alibabacloud.com/help/en/oss/)

## Further reading

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/ShareObjectPreSignedURL.html
- https://developers.cloudflare.com/r2/api/s3/presigned-urls/
- https://docs.digitalocean.com/products/spaces/
