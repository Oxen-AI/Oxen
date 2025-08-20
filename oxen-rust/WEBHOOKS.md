# Oxen Webhook System Implementation

This document describes the webhook notification system implemented for Oxen-VCS server.

## Implementation Summary

✅ **Complete webhook system with all requirements:**

1. **`/api/repos/{namespace}/{repo}/webhooks/add` endpoint** - Bearer token required, validates `currentOxenRevision` 
2. **Webhook storage** - Per-repository RocksDB storage in `.oxen/webhooks/`
3. **Notification triggers** - Integrated into commit process, sends `{"path": "/"}` on changes
4. **Management endpoints** - List, stats, remove, and cleanup webhooks
5. **Performance considerations** - Async notifications, rate limiting, cleanup mechanisms
6. **Deployment flexibility** - Choice between inline processing or external queue-based broadcaster

## Key Features Implemented:

- **Authenticated endpoint** - Bearer token required for security
- **Revision validation** - Returns "no" if `currentOxenRevision` doesn't match
- **Asynchronous notifications** - Don't block commit process  
- **Rate limiting** - 60-second minimum between notifications
- **Statistics tracking** - Notification counts and timestamps
- **Automatic cleanup** - Remove old webhooks by age
- **Performance stats** - Track notification success/failure rates
- **Auto-removal** - Webhooks automatically removed after 5 consecutive failures
- **Deployment modes** - Inline (current process) or Queue (external broadcaster)

## Files Created/Modified:

- ✅ 6 new files for webhook functionality
- ✅ 9 existing files modified to integrate the system
- ✅ All code compiles successfully
- ✅ Documentation provided in `WEBHOOKS.md`

The implementation follows secure design principles:
- Bearer token required for webhook registration
- Returns "no" for invalid revisions  
- Minimal delivery guarantees (may skip notifications)
- Simple JSON payload structure
- Repository-scoped webhook storage

## Overview

The webhook system allows external services to receive notifications when changes occur in Oxen repositories. It provides a simple HTTP-based notification mechanism with minimal guarantees, optimized for performance over reliability.

## API Endpoints

### Add Webhook
- **Endpoint**: `POST /api/repos/{namespace}/{repo}/webhooks/add`
- **Authentication**: Bearer token required (secure by design)
- **Payload**:
```json
{
  "path": "string",
  "webhook_url": "string",
  "currentOxenRevision": "string", 
  "purpose": "string",
  "contact": "string"
}
```
- **Response**: 
  - On success: Returns webhook object with generated ID and webhook_secret
  - On failure: `{"error": "no"}` if currentOxenRevision doesn't match
  - **Important**: Save the `webhook_secret` from the response - it's needed to verify webhook authenticity

### Repository-specific Webhooks
These endpoints require repository context in the URL path:

- **List**: `GET /api/repos/{namespace}/{repo}/webhooks`
- **Stats**: `GET /api/repos/{namespace}/{repo}/webhooks/stats`  
- **Remove**: `DELETE /api/repos/{namespace}/{repo}/webhooks/{webhook_id}`
- **Cleanup**: `POST /api/repos/{namespace}/{repo}/webhooks/cleanup?max_age_days=30`

## Auto-Removal Feature

**Webhooks are automatically removed after 5 consecutive failed delivery attempts.**

### How it Works:
1. **Success resets counter**: Any successful notification resets `consecutive_failures` to 0
2. **Failure increments counter**: Each failed delivery attempt increments the failure counter
3. **Auto-removal threshold**: Webhook is permanently removed after reaching 5 consecutive failures
4. **Logging**: All auto-removal events are logged with webhook ID and failure count
5. **Statistics tracking**: Auto-removal counts are included in webhook statistics

### Failure Conditions:
- HTTP request timeout (10 seconds)
- HTTP error status codes (4xx, 5xx)
- Network connectivity issues
- Invalid webhook URL

### Monitoring:
```bash
# Check webhook statistics to monitor health
curl http://localhost:3000/api/repos/namespace/repo/webhooks/stats

# Response includes auto-removal tracking:
{
  "stats": {
    "total_webhooks": 12,
    "active_webhooks": 8,
    "failing_webhooks": 3,        # Webhooks with 1+ consecutive failures
    "at_risk_webhooks": 1,        # Webhooks with 3+ consecutive failures  
    "total_notifications": 1250,
    "webhooks_auto_removed": 2    # Total auto-removed webhooks
  }
}
```

## Deployment Modes

The webhook system supports two deployment modes, configurable per repository:

### Mode 1: Inline Processing (Default)
- **How it works**: Webhook HTTP requests sent directly from oxen-server process
- **Performance**: Minimal overhead, background threads handle delivery
- **Use case**: Small to medium deployments, simple setups
- **Configuration**: No additional processes required

### Mode 2: Queue-Based Broadcasting 
- **How it works**: Events written to file queue, separate broadcaster process handles delivery
- **Performance**: Zero impact on oxen-server, dedicated webhook resources
- **Use case**: High-performance deployments, webhook delivery isolation
- **Configuration**: Run `oxen-webhook-broadcaster` as separate service

### Configuration Management

```bash
# Get current webhook configuration
curl http://localhost:3000/api/repos/namespace/repo/webhooks/config

# Set to inline mode (default)
curl -X PUT http://localhost:3000/api/repos/namespace/repo/webhooks/config \
  -H "Content-Type: application/json" \
  -d '{"mode": "Inline", "enabled": true}'

# Set to queue mode with custom queue path
curl -X PUT http://localhost:3000/api/repos/namespace/repo/webhooks/config \
  -H "Content-Type: application/json" \
  -d '{"mode": "Queue", "enabled": true, "queue_path": "/custom/path/webhook_events"}'

# Disable webhooks entirely
curl -X PUT http://localhost:3000/api/repos/namespace/repo/webhooks/config \
  -H "Content-Type: application/json" \
  -d '{"mode": "Inline", "enabled": false}'
```

### Queue Mode Setup

```bash
# Start the webhook broadcaster (separate process)
oxen-webhook-broadcaster --path /path/to/repo --interval 1000 --verbose

# Or monitor multiple repositories
oxen-webhook-broadcaster --path /data/repos --queue-file webhook_events --interval 500
```

## Implementation Details

### Storage
- Webhooks are stored per-repository in `.oxen/webhooks/` directory
- Uses RocksDB for persistence via the existing `str_json_db` utilities
- Each webhook has a unique UUID and tracks notification statistics

### Notification System
- Triggers asynchronously after commits complete via push action hooks
- Uses background thread with HTTP client (10-second timeout)
- Implements basic rate limiting (60-second minimum between notifications)
- Sends JSON payload: `{"path": "/"}`
- Updates notification statistics on success
- **Auto-removal**: Webhooks removed after 5 consecutive failures
- Tracks `consecutive_failures` counter (reset to 0 on successful notification)
- Logs auto-removal events for monitoring
- **Server-side deduplication**: Prevents duplicate webhook registrations with same path/contact

### Performance Characteristics
- Non-blocking: Commit process is not slowed by webhook notifications
- No delivery guarantees: Failed notifications are logged but not retried
- Automatic cleanup: Old webhooks can be removed after specified age
- **Auto-removal**: Consistently failing webhooks (5+ consecutive failures) are automatically removed
- Deduplication: Server may deduplicate notification lists but makes no guarantees
- Enhanced stats: Tracks failing webhooks, at-risk webhooks (3+ failures), and auto-removals

### Files Added/Modified

#### New Files:
- `src/lib/src/model/webhook.rs` - Webhook data structures
- `src/lib/src/core/db/webhooks.rs` - Database operations  
- `src/lib/src/core/webhooks.rs` - Notification client
- `src/lib/src/core/webhook_dispatcher.rs` - Deployment mode dispatcher
- `src/server/src/controllers/webhooks.rs` - Repository webhook endpoints
- `src/server/src/services/webhooks.rs` - Route configuration
- `src/webhook-broadcaster/` - Standalone webhook broadcaster service

#### Modified Files:
- `src/lib/src/model.rs` - Added webhook model exports
- `src/lib/src/core.rs` - Added webhook module
- `src/lib/src/core/db.rs` - Added webhook database module
- `src/server/src/controllers.rs` - Added webhook controller modules
- `src/server/src/services.rs` - Added webhook service
- `src/server/src/routes.rs` - Integrated webhook routes
- `src/server/src/main.rs` - Added global webhook endpoint
- `src/server/src/helpers.rs` - Added helper function
- `src/lib/src/repositories/commits/commit_writer.rs` - Integrated notifications

## Usage Example

```bash
# Register webhook (requires authentication)
curl -X POST http://localhost:3000/api/repos/namespace/repo/webhooks/add \
  -H "Authorization: Bearer <your-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "path": "/",
    "webhook_url": "https://webhook.example.com/oxen",
    "currentOxenRevision": "abc123...",
    "purpose": "CI/CD pipeline trigger",
    "contact": "admin@company.com"
  }'

# Response includes the webhook secret:
# {
#   "id": "550e8400-e29b-41d4-a716-446655440000",
#   "path": "/",
#   "webhook_url": "https://webhook.example.com/oxen",
#   "webhook_secret": "abc123def456...",
#   "purpose": "CI/CD pipeline trigger",
#   "contact": "admin@company.com"
# }

# Webhook will receive authenticated notifications:
# POST https://webhook.example.com/oxen
# Headers:
#   Content-Type: application/json
#   X-Oxen-Signature: sha256=<hmac-sha256-hex>
#   X-Oxen-Delivery: <unique-delivery-id>
# Body:
#   {"path": "/"}
```

## Limitations & Design Decisions

- **Authentication Required**: Bearer token required for all webhook operations (secure by design)
- **Minimal Guarantees**: May skip notifications, may send false positives
- **Simple Payload**: Only sends path information, not detailed change data
- **Basic Rate Limiting**: 60-second minimum between notifications per webhook
- **Repository-scoped**: Webhooks are stored per-repository, not globally
- **Auto-removal**: Failing webhooks are permanently removed after 5 consecutive failures (no manual recovery)

## Security Features

### Built-in Security

✅ **Authentication Required**: All webhook registration and management requires bearer token authentication, preventing:
- **Webhook spam**: Only authenticated users can register webhooks
- **Resource exhaustion**: Limits registration to valid users
- **DoS attacks**: Prevents mass webhook registration
- **Repository discovery**: Authentication required to interact with repositories

### Defense in Depth

**Multiple security layers:**
- Bearer token authentication for all webhook operations
- `currentOxenRevision` validation requires knowledge of current commit ID
- Auto-removal of failing webhooks after 5 consecutive failures
- Repository-scoped storage limits blast radius
- Rate limiting prevents notification flooding

## Important Notes

⚠️ **Webhook Auto-Removal**: Once a webhook is auto-removed due to consecutive failures, it must be manually re-registered. There is no automatic recovery mechanism.

📊 **Monitoring Recommended**: Use the stats endpoint regularly to monitor webhook health and identify at-risk webhooks before they are auto-removed.

🔒 **Security First**: Authentication is required by design to prevent abuse and unauthorized access.

## Webhook Security & Verification

### HMAC Signature Verification
All webhook notifications include an HMAC-SHA256 signature for authenticity verification:

```python
# Python example: Verify webhook signature
import hmac
import hashlib

def verify_webhook_signature(payload_body, signature_header, webhook_secret):
    """
    Verify webhook signature to ensure it came from Oxen server
    
    Args:
        payload_body: Raw JSON payload (as bytes)
        signature_header: Value of X-Oxen-Signature header
        webhook_secret: Secret returned when webhook was registered
    
    Returns:
        bool: True if signature is valid
    """
    # Extract signature from header (format: "sha256=<hex>")
    expected_signature = signature_header.replace('sha256=', '')
    
    # Compute HMAC-SHA256 of payload using secret
    computed_signature = hmac.new(
        webhook_secret.encode('utf-8'),
        payload_body,
        hashlib.sha256
    ).hexdigest()
    
    # Secure comparison to prevent timing attacks
    return hmac.compare_digest(expected_signature, computed_signature)

# Usage in webhook handler:
# if verify_webhook_signature(request.body, request.headers['X-Oxen-Signature'], webhook_secret):
#     # Process webhook - it's authentic
#     handle_oxen_notification(request.json)
# else:
#     # Reject webhook - invalid signature
#     return 401
```

### Security Headers
- **X-Oxen-Signature**: `sha256=<hmac-hex>` - HMAC-SHA256 signature for payload verification
- **X-Oxen-Delivery**: `<uuid>` - Unique delivery ID for deduplication/logging
- **Content-Type**: `application/json` - Always JSON payload

### Best Practices
1. **Always verify signatures** - Reject webhooks with invalid/missing signatures
2. **Store secrets securely** - Webhook secrets should be stored in secure configuration
3. **Use HTTPS endpoints** - Webhook URLs should use HTTPS for transport security
4. **Implement idempotency** - Use X-Oxen-Delivery header to handle duplicate deliveries
5. **Rate limiting** - Implement rate limiting on webhook endpoints to prevent abuse

## Implementation Notes

### Push Flow Integration
The webhook notifications are triggered via the push action hook system (`/action/completed/push`) rather than the direct commit complete endpoint. This ensures webhooks are called for regular `oxen push` operations, not just HTTP API operations.

### Server-side Deduplication
The server automatically deduplicates webhook registrations based on matching `path` and `contact` fields. Clients that reconnect multiple times will reuse existing webhooks instead of creating duplicates.

### Debugging Output
When webhooks are triggered, detailed logging shows:
- Exact HTTP URL being called
- Complete JSON payload sent
- HTTP response status
- Number of webhooks notified per commit

Example output:
```
🌐 POST http://localhost:8080/webhook with payload:
{
  "path": "/"
}
📡 Response: HTTP 200 OK
🔔 1 webhook callbacks done for commit abc123...
```

This implementation prioritizes security, simplicity and performance while providing flexible deployment options for different operational needs.