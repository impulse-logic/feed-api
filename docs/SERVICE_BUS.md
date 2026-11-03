# Azure Service Bus guide

How feed-api reads vendor data from Azure Service Bus and saves it to the database.

Two endpoints do everything:

| Step | Call | What it does |
|---|---|---|
| 1. Look | `GET /queue/vendor/peek` | Shows the messages. Nothing is locked, changed or removed. |
| 2. Process | `POST /queue/vendor/process` | Picks messages (PeekLock) → saves them to the DB → **then** deletes them from the queue. |

---

## 1. The basics in plain words

| Word | Meaning |
|---|---|
| **Namespace** | Your Service Bus "server": `sb-sliq-skymallstg-724.servicebus.windows.net`. |
| **Queue** | A list of messages waiting to be processed, e.g. `vendor`. Oldest first. |
| **Message** | One item in the queue. Its **body** is the data. For vendor it is **one vendor or an array of many** (see "Message format" below). |
| **Sequence number** | A message's unique, increasing number. Used for paging in peek. |
| **Peek** | *Look* at messages. Safe to call any time. |
| **PeekLock** | *Take* a message: it is hidden from everyone else for a short time (the lock). The app then decides: **complete** (delete for good), **abandon** (put it back) or **dead-letter** (move it aside). |
| **Delivery count** | How many times a message was taken. After the queue's max (default 10) Azure moves it to the dead-letter queue by itself. |
| **Dead-letter queue (DLQ)** | A side queue for messages that can't be processed. They are kept there with a reason, not lost. In the portal: queue `vendor` → **Service Bus Explorer** → **Dead-letter**. |

---

## 2. How the app connects (authentication)

You don't call anything to connect. On startup:

1. `.env` holds `SERVICE_BUS_CONNECTION_STRING` (Endpoint + key name + key).
2. `queue.NewClient` (`internal/queue/client.go`) gives it to the official Azure SDK.
3. The SDK opens **one AMQP connection** and creates and renews the SAS tokens by itself.
4. Every queue and endpoint shares that one connection.

The log shows `Service Bus: using connection string (SAS key)`.

Other option: if `SERVICE_BUS_CONNECTION_STRING` is empty and `SERVICE_BUS_NAMESPACE` is set, the app logs
in with your Azure account instead (`az login` on your PC, or a managed identity on an Azure VM).
If both are empty the app still runs and `/queue` returns 503.

### Compared with the Laravel `ServiceBusController`

The Laravel code uses the same key, but talks to Azure with plain HTTP. The Go app uses the SDK:

| Laravel (HTTP REST) | feed-api (Go SDK, AMQP) |
|---|---|
| `generateSasToken()` signs a token by hand, valid for 1 hour | Done inside the SDK and renewed automatically |
| `peekLock()`: 1 HTTP call locks **1** message | One receive locks up to **100** messages |
| `completeMessage(lock_location)`: 1 more HTTP call per message | `CompleteMessage`, sent over the open connection, many in parallel |
| `receiveAndDelete()`: deletes **before** saving, so a crash loses data | Not used: a message is deleted only **after** the DB commit |
| No real "look only" call (its "peek" actually locks) | `GET /queue/vendor/peek`: true peek, no lock |
| Key hard-coded in the PHP class | Key only in `.env` (git-ignored) |

### 2.1 IMPORTANT: replace the root key (do this once)

The key in `.env` (and hard-coded in the Laravel class) is **RootManageSharedAccessKey**. It can
create and delete queues and change everything in the namespace, and it has been shared in chat. Replace it:

1. Open <https://portal.azure.com>, search **Service Bus**, and open **sb-sliq-skymallstg-724**.
2. **Rotate the root key.** In the left menu, open **Settings → Shared access policies**. Click
   **RootManageSharedAccessKey**, then **Regenerate primary key** (and secondary). The old key stops working.
3. **Create a key just for this app.** On the same page click **+ Add**. Name it `feed-api-listen`,
   tick **Listen** and **Send**, and click **Create**. *Listen* is needed for peek and process;
   *Send* is needed to move only the invalid items to the dead-letter queue (without it, the whole
   message is dead-lettered instead).
4. Click `feed-api-listen` and copy the **Primary Connection String**.
5. Paste it in `.env` (and on the server in `/opt/feed-api/.env`):
   ```ini
   SERVICE_BUS_CONNECTION_STRING="Endpoint=sb://sb-sliq-skymallstg-724.servicebus.windows.net/;SharedAccessKeyName=feed-api-listen;SharedAccessKey=..."
   ```
6. Restart the app. Update the Laravel app too: it will stop working once the root key is rotated.

> Never commit `.env`, and never paste keys in code, chat, tickets or screenshots.

---

## 3. Settings (`.env`)

| Variable | Default | Meaning |
|---|---|---|
| `SERVICE_BUS_CONNECTION_STRING` | *(none)* | Login for Service Bus. Keep the quotes. |
| `SERVICE_BUS_NAMESPACE` | *(none)* | Only used when the connection string is empty. |
| `SERVICE_BUS_ENTITY` | `vendor` | Queues the API may use, comma-separated (`vendor,product`). |
| `QUEUE_ADMIN_TOKEN` | *(none)* | Not used right now (the `/queue` endpoints need no header). See `internal/routes/routes.go` to turn the check back on. |
| `QUEUE_WORKERS` | `8` | Parallel readers in one process run (1–32). |
| `QUEUE_BATCH_SIZE` | `100` | Messages per batch, which is also one DB transaction (1–1000). |

The `/queue` endpoints need **no header and no token**: just call the URL.

---

## 4. Step 1: look at the messages (peek)

```
GET /queue/vendor/peek?count=10&fromSequence=0
```

| Query | Default | Allowed | Meaning |
|---|---|---|---|
| `count` | `10` | 1–100 | How many messages to show. |
| `fromSequence` | `0` | 0 or more | Start at this sequence number (`0` = oldest). |

```sh
curl "http://localhost:8080/queue/vendor/peek?count=5"
```
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/queue/vendor/peek?count=5" | ConvertTo-Json -Depth 10
```

Response (example):
```json
{
  "message": "Peeked 1 messages (not removed)",
  "data": {
    "queue": "vendor",
    "count": 1,
    "nextSequence": 23,
    "messages": [
      {
        "sequenceNumber": 22,
        "messageId": "237b0fb7-1744-45ea-b675-c349dcb44b3b",
        "enqueuedTime": "2026-10-31T19:00:05.394+06:00",
        "deliveryCount": 1,
        "contentType": null,
        "applicationProperties": { "source": "nodejs-service-bus-tester" },
        "bodyFormat": "json",
        "body": { "external_id": "1011", "name": "vendor1" }
      }
    ]
  }
}
```

| Field | Meaning |
|---|---|
| `nextSequence` | Send it as `fromSequence` to get the next page. Fewer messages than `count` means you reached the end. |
| `deliveryCount` | How many times the message was *taken* (peek does not count). |
| `applicationProperties` | Extra key/values the sender attached. |
| `bodyFormat` | `json`, `text` or `binary` (base64). |

### Message format (vendor queue)

A message body can be **one vendor** or an **array of vendors** (the real feed sends ~2,500 per message):

```json
{ "external_id": "1011", "name": "vendor1" }
```
```json
[
  { "external_id": "H0166", "name": "KENNETH HAREWOOD" },
  { "external_id": "H0167", "name": "DAWN HUNTE" }
]
```

Each item needs `external_id` and `name`: required, at most 255 characters, trimmed. Numbers are
accepted and turned into text.

---

## 5. Step 2: process the messages (pick → insert → delete)

```
POST /queue/vendor/process?max=1000
```

| Query | Default | Meaning |
|---|---|---|
| `max` | `0` | Process at most this many messages. `0` or missing = until the queue is empty. |

```sh
curl -X POST "http://localhost:8080/queue/vendor/process?max=5"
```

Response (example: one message with 2,553 vendors, 3 of them invalid). `received` is 2: the original
message plus the new message holding only the 3 invalid items, which was dead-lettered in the same run:
```json
{
  "message": "Processed 2 messages",
  "data": {
    "queue": "vendor",
    "received": 2,
    "completed": 1,
    "deadLettered": 1,
    "abandoned": 0,
    "counts": { "inserted": 2550, "updated": 0, "unchanged": 0, "skipped": 3, "storeLinksAdded": 63750 },
    "durationMs": 4100,
    "perSecond": 0.2,
    "errors": [
      "seq 11681 item 1820 (J0112): name \"JOYCE JOSEPH\" is also used by external_id J0098 in the same batch",
      "seq 11681 item 2401 (V003): name is required",
      "seq 11690: moved to the dead-letter queue: 3 invalid vendor items from message seq 11681 (2550 valid items were saved)"
    ]
  }
}
```

| Field | Meaning |
|---|---|
| `received` | Messages taken from the queue in this run. |
| `completed` | Message **deleted** from the queue: all its valid items were saved (invalid items, if any, were moved to a new message). |
| `deadLettered` | Messages moved to the dead-letter queue: invalid-items-only messages, or a message with an unreadable body. |
| `abandoned` | Put back because of a temporary error. They are retried in the next run. |
| `counts.inserted / updated / unchanged` | What happened to the vendors (duplicates in the queue count once). |
| `counts.skipped` | Invalid vendor items that were **not** saved (each one is listed in `errors`). |
| `counts.storeLinksAdded` | New `store_vendors` rows (every vendor is linked to every store). |
| `errors` | The first 100 problems: `seq <message> item <position> (<external_id>): <reason>`. |
| `stoppedEarly` | Only present when the run stopped because of a temporary error. |

### What happens inside, step by step

1. **One run at a time.** A second call while a run is busy gets **409**, so runs never overlap.
2. `QUEUE_WORKERS` workers start. Each takes up to `QUEUE_BATCH_SIZE` messages in **PeekLock**
   mode. Azure never gives the same locked message to two workers.
3. Each body is split into items (one object, or every element of an array), and **every item** is checked.
4. Duplicates are merged by `external_id` (the newest wins).
5. **Name clashes are found before saving.** `vendors.name` is unique, so these items are not saved:
   - the name already belongs to a vendor with another `external_id` in the DB;
   - the same name is used by two `external_id`s in the batch (the first one wins).
6. The valid vendors are saved in chunks of **1,000 per transaction** (2 SQL statements each),
   using the same rules as `POST /vendors`:
   - `vendors`: insert a new `external_id`, update `name` if it changed, otherwise leave it alone.
   - `store_vendors`: link every vendor to every store (`picklist_creation = false`). Existing links are never changed.
7. Only **after the commit** is each message settled:
   - all items saved → the message is **completed** (deleted);
   - some items invalid → a **new message with only the invalid items** is sent to the queue, then
     the original is completed. When the new message is received (usually in the same run) it goes
     straight to the **dead-letter queue** with the reasons. So the DLQ holds only the bad items.
8. While a big batch is being saved, the message locks are renewed every 20 s, so they never expire mid-save.

### What happens to each kind of message

| Situation | Result |
|---|---|
| All items valid | Saved, then the message is **deleted** from the queue. |
| Some items invalid (empty `name` / `external_id`, longer than 255, not an object, name already used by another `external_id`) | The **valid items are saved**. **Only the invalid items** go to the dead-letter queue, as a new message with the reasons (see below). E.g. 100 items, 80 valid → 80 saved, a 20-item message in the DLQ. |
| Same, but sending the new message fails (key without **Send** right, Azure error) | Fallback: the valid items are still saved, and the **whole original message** is dead-lettered with the reasons. Nothing is lost. |
| Body is not JSON at all | **Dead-lettered** ("invalid message body"). |
| Database down or other temporary error | **Abandoned** (stays in the queue), and the run stops (`stoppedEarly`). The next run retries it. |
| Same message delivered twice (e.g. the app crashed after saving, before deleting) | Saved again as "unchanged". **No duplicate rows**, ever. |
| App stopped in the middle | Messages not yet deleted come back when their lock expires. Nothing is lost. |

### Reading the dead-letter reasons

The dead-lettered message's **body is only the invalid items, exactly as they were sent**
(e.g. `[{"external_id":"V003","name":""}, ...]`), plus:

| Field | Example |
|---|---|
| `DeadLetterReason` | `3 invalid vendor items from message seq 11681 (2550 valid items were saved)` |
| `DeadLetterErrorDescription` | `item 1820 (J0112): name "JOYCE JOSEPH" is also used by external_id J0098 in the same batch; item 2401 (V003): name is required; ...` |
| property `invalidItems` | `[{"item":2401,"external_id":"V003","name":"","reason":"name is required"}, ...]` (item = position in the original message) |
| property `invalidItemsTotal` | `3` |
| property `originalSequence` / `originalMessageId` | the message the items came from |
| property `invalidItemsOnly` | `true` (marks the split message; it is dead-lettered as soon as it is received) |

In the fallback case (send failed) the DLQ holds the whole original message with the same reason,
description and `invalidItems`.

Read them with the API (below) or in the Azure portal: **Service Bus** → **sb-sliq-skymallstg-724** →
queue **vendor** → **Service Bus Explorer** → **Dead-letter** → **Peek from start** → open the message → *Properties*.

### View the dead-letter queue (API)

```
GET /queue/deadletter?queue=vendor&count=10&fromSequence=0
```

| Query | Default | Allowed | Meaning |
|---|---|---|---|
| `queue` | *(required)* | a name in `SERVICE_BUS_ENTITY` | Which queue's dead-letter queue to read. |
| `count` | `10` | 1–100 | How many messages to show. |
| `fromSequence` | `0` | 0 or more | Start here; use `nextSequence` from the previous page. |

Read-only like peek: nothing is removed or locked. Each message has the same fields as peek plus
`deadLetterReason`, `deadLetterErrorDescription` and `deadLetterSource`.

```json
{
  "message": "Peeked 1 dead-letter messages (not removed)",
  "data": {
    "queue": "vendor",
    "subQueue": "deadletter",
    "count": 1,
    "nextSequence": 41,
    "messages": [
      {
        "sequenceNumber": 40,
        "enqueuedTime": "2026-11-02T16:20:11+06:00",
        "deliveryCount": 1,
        "deadLetterReason": "2 invalid vendor items from message seq 11690 (80 valid items were saved)",
        "deadLetterErrorDescription": "item 3 (V003): name is required; item 9 (J0112): name \"JOYCE JOSEPH\" is already used by vendor external_id J0098",
        "applicationProperties": { "invalidItemsOnly": true, "originalSequence": 11690, "invalidItemsTotal": 2, "invalidItems": "[...]" },
        "bodyFormat": "json",
        "body": [ { "external_id": "V003", "name": "" }, { "external_id": "J0112", "name": "JOYCE JOSEPH" } ]
      }
    ]
  }
}
```

| Error | Cause |
|---|---|
| `422 The queue query parameter is required` | No `?queue=`. |
| `404 Unknown queue` | The queue is not in `SERVICE_BUS_ENTITY`. |

Fix the bad items at the source and send them again. Re-sending the whole message is also safe:
items that were already saved come back as "unchanged", so no duplicates are created.

### Speed

Measured from a PC in Bangladesh, with about 250 ms network latency to Azure:

| Run | Result |
|---|---|
| First run after the app starts (opens the connections) | 2,000 messages in ~27 s |
| Every later run | **2,000 messages in 6 s** (~650/s; 3 s of that is the final "queue empty" check) |
| Database part | ~40 ms per 100 vendors |

- **Network latency is the limit.** On a server in the **same Azure region** as the namespace (latency about
  1 ms), the same code is many times faster.
- Raise `QUEUE_WORKERS` (up to 32) to go faster.
- Service Bus itself has a ceiling: a Standard-tier queue handles roughly a few thousand messages per
  second; Premium handles more.
- The app reuses its connections between runs. Only the first run after a restart pays the connection
  cost. The first peek after a restart takes ~3 s; after that, peek takes well under a second.
- An empty-queue run returns after ~3 s, or up to ~30 s right after a restart.

### Running it automatically

Call the same endpoint on a schedule, e.g. a systemd timer on the VPS every minute:

```sh
curl -s -X POST http://localhost:8080/queue/vendor/process
```

Overlapping calls are safe: a call that arrives while a run is busy just gets 409.

---

## 6. Postman

1. Import `postman/feed-api.json`.
2. Open the **Queue (Service Bus)** folder:
   - **Q1 / Q2:** peek (first page, next page)
   - **Q4 / Q5:** error checks (404, 422). Q3 (401) only applies when the token check is turned back on.
   - **Q6:** process 5 messages
   - **Q7:** process the whole queue
   - **Q8:** bad `max` (422)

Q6 and Q7 change real data: they save vendors and delete messages.

---

## 7. Add another queue later (e.g. `product`)

1. `.env`: `SERVICE_BUS_ENTITY=vendor,product`, then restart. **Peek works now.** Look at the body format.
2. Write `internal/queue/product_processor.go`. Copy `vendor_processor.go`: parse and validate the
   body, then call a repository batch method that saves everything in one transaction.
3. Register it with a single line in `internal/routes/routes.go`, next to the vendor one:
   ```go
   consumers["product"] = queue.NewConsumer(sbClient, "product", queue.NewProductProcessor(productRepo), cfg.QueueWorkers, cfg.QueueBatchSize)
   ```
4. `POST /queue/product/process` works. Login, workers, batching, settling, the 409 guard and the
   dead-letter handling all come from `internal/queue/consumer.go`.

---

## 8. Errors and fixes

| Response | Cause | Fix |
|---|---|---|
| `503 Service Bus is not configured.` | No connection string or namespace. | Set `SERVICE_BUS_CONNECTION_STRING` and restart. |
| `404 Unknown queue` / `cannot be processed` | Queue not in `SERVICE_BUS_ENTITY`, or no processor for it yet. | Add it to `.env` (and write a processor for process). |
| `409 This queue is already being processed.` | A run is busy. | Wait for it to finish. |
| `422 count must be ...` / `max must be ...` | Bad query value. | See the tables above. |
| `502 Could not read the queue.` | Azure refused or is unreachable: wrong or rotated key, no *Listen* right, queue doesn't exist, no internet. | The server log line `Service Bus ... error` has the real reason. |
| App stops at start: `invalid SERVICE_BUS_CONNECTION_STRING` | Malformed string. | Paste the full Primary Connection String again, inside quotes. |
| `deadLettered` > 0 | Bad messages. | Read `errors`, or open the DLQ in the portal (Service Bus Explorer → Dead-letter). Fix the sender. |
| `abandoned` > 0, `stoppedEarly` set | DB problem during the run. | Check the server log (`Database error ...`), fix it, call process again. |
