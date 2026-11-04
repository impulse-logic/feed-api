# Azure Service Bus guide

How feed-api reads data from Azure Service Bus queues and saves it to the database.

Queues: **`vendor`**, **`product-hierarchies`**, **`products`**, **`product-upcs`**, **`boh`**, **`deliveries`**,
**`stocktake`**, **`product-variants`** and **`pos`**. Every queue uses the **same routes**; only the queue
name in the URL changes (dead-letter takes it as `?queue=`). No header or token is needed.

| Route | What it does |
|---|---|
| `GET /queue/:name/peek?count=10` | Shows the messages. Nothing is locked, changed or removed. |
| `POST /queue/:name/send` | Pushes the JSON body (object or array) into the queue. |
| `POST /queue/:name/process?max=100` | Picks messages (PeekLock) → saves them to the DB → **then** deletes them from the queue. |
| `GET /queue/deadletter?queue=:name` | Shows the dead-letter messages with the reasons. |

| Queue | Peek | Send | Process | Dead-letter | Saves like |
|---|---|---|---|---|---|
| `vendor` | `/queue/vendor/peek` | `/queue/vendor/send` | `/queue/vendor/process` | `/queue/deadletter?queue=vendor` | `POST /vendors` |
| `product-hierarchies` | `/queue/product-hierarchies/peek` | `/queue/product-hierarchies/send` | `/queue/product-hierarchies/process` | `/queue/deadletter?queue=product-hierarchies` | `POST /product-hierarchies` |
| `products` | `/queue/products/peek` | `/queue/products/send` | `/queue/products/process` | `/queue/deadletter?queue=products` | `POST /products` |
| `product-upcs` | `/queue/product-upcs/peek` | `/queue/product-upcs/send` | `/queue/product-upcs/process` | `/queue/deadletter?queue=product-upcs` | `POST /product-upcs` |
| `boh` | `/queue/boh/peek` | `/queue/boh/send` | `/queue/boh/process` | `/queue/deadletter?queue=boh` | `POST /boh` |
| `deliveries` | `/queue/deliveries/peek` | `/queue/deliveries/send` | `/queue/deliveries/process` | `/queue/deadletter?queue=deliveries` | `POST /deliveries` |
| `stocktake` | `/queue/stocktake/peek` | `/queue/stocktake/send` | `/queue/stocktake/process` | `/queue/deadletter?queue=stocktake` | `POST /stocktake` |
| `product-variants` | `/queue/product-variants/peek` | `/queue/product-variants/send` | `/queue/product-variants/process` | `/queue/deadletter?queue=product-variants` | `POST /product-variants` |
| `pos` | `/queue/pos/peek` | `/queue/pos/send` | `/queue/pos/process` | `/queue/deadletter?queue=pos` | `POST /pos` |

Sections 4–5 explain the routes with the `vendor` queue. Sections 5b–5i cover what is different for
each of the other queues.

**Order:** process `product-hierarchies` → `products` → then the rest (`product-upcs`,
`product-variants`, `boh`, `stocktake`, `deliveries`, `pos`). Every one of them needs the products
to exist (and active), so items processed too early go to the DLQ as "Product not found".

> **POS:** a sale line is counted every time it is processed (`pos.qty` and the stock deduction add up).
> Never send the same sale line twice. See 5i.

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
| `SERVICE_BUS_ENTITY` | `vendor,product-hierarchies,products,product-upcs,boh,deliveries,stocktake,product-variants,pos` | Queues the API may use, comma-separated. A queue that is not listed gets 404. |
| `QUEUE_ADMIN_TOKEN` | *(none)* | Not used right now (the `/queue` endpoints need no header). See `internal/routes/routes.go` to turn the check back on. |
| `QUEUE_WORKERS` | `8` | Parallel readers in one process run (1–32). |
| `QUEUE_BATCH_SIZE` | `100` | Messages per batch, which is also one DB transaction (1–1000). |
| `QUEUE_EMPTY_WAIT_MS` | `1000` | Only used when the message count can't be read (key without the Manage right): a receive that gets nothing this long means the queue is empty (200–10000). |

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

### Push data to a queue (send)

```
POST /queue/vendor/send
Content-Type: application/json
```

The body is sent to the queue **as-is**: one vendor or an array of vendors, the same formats the
processor reads.

```json
[
  { "external_id": "H0166", "name": "KENNETH HAREWOOD" },
  { "external_id": "H0167", "name": "DAWN HUNTE" }
]
```

```sh
curl -X POST "http://localhost:8080/queue/vendor/send" -H "Content-Type: application/json" -d '[{"external_id":"H0166","name":"KENNETH HAREWOOD"}]'
```

Response:
```json
{ "message": "Sent 2 items in 1 messages to queue vendor",
  "data": { "queue": "vendor", "messages": 1, "items": 2, "bytes": 115 } }
```

- The content is **not** checked here. `process` checks it later, and bad items go to the dead-letter queue as usual.
- **Size:** one Service Bus message holds at most 256 KB on the Standard tier. A bigger **array** is
  split into several messages automatically (tested: 6,000 items / 800 KB → 4 messages of 1,500).
- Every message gets `contentType: application/json` and the property `source: "feed-api"`, so peek
  shows where it came from.
- The key in `.env` needs the **Send** right.

| Error | Cause |
|---|---|
| `422 The body must be a JSON object or a JSON array.` | Empty body, invalid JSON, or something else (e.g. a number). |
| `422 The array is empty: nothing to send.` | `[]`. |
| `404 Unknown queue` | The queue is not in `SERVICE_BUS_ENTITY`. |
| `413 One item is larger than the queue's maximum message size.` | A single object (or one array item) bigger than 256 KB. |
| `502 Could not send to the queue.` | Azure error, or the key has no Send right. The server log has the reason. |

> The `/queue` routes have **no authentication**. On the live server, anyone who can reach the URL can
> push data into the queue (and run `process`). Put them behind Nginx rules or turn the token check
> back on (`internal/routes/routes.go`) before going live.

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
    "counts": { "inserted": 2550, "updated": 0, "unchanged": 0, "skipped": 3, "duplicates": 0, "storeLinksAdded": 63750 },
    "durationMs": 4100,
    "perSecond": 0.2,
    "errors": [
      "seq 11681 item 1820 (J0112) [invalid]: name \"JOYCE JOSEPH\" is also used by external_id J0098 in the same batch",
      "seq 11681 item 2401 (V003) [invalid]: name is required",
      "seq 11690: moved to the dead-letter queue: 3 vendor items not saved from message seq 11681 (3 invalid); 2550 items saved"
    ]
  }
}
```

| Field | Meaning |
|---|---|
| `received` | Messages taken from the queue in this run. |
| `completed` | Message **deleted** from the queue. Its inserted/updated items were saved; every other item (invalid, duplicate, unchanged) was moved to a new message for the DLQ. |
| `deadLettered` | Messages moved to the dead-letter queue: the not-saved-items messages, or a message with an unreadable body. |
| `abandoned` | Put back because of a temporary error. They are retried in the next run. |
| `counts.inserted / updated / unchanged` | What happened to the vendors (duplicates in the queue count once). |
| `counts.skipped` | **Invalid** items (validation error or name clash). Sent to the DLQ. |
| `counts.duplicates` | Items whose `external_id` is sent again later (the later one is used). Sent to the DLQ. |
| `counts.unchanged` | Items already in the DB with the same name (nothing to update). **Also sent to the DLQ.** |
| `counts.storeLinksAdded` | New `store_vendors` rows (every vendor is linked to every store). |
| `errors` | The first 100 not-saved items: `seq <message> item <position> (<external_id>) [<type>]: <reason>`, invalid first, then duplicate, then unchanged. |
| `stoppedEarly` | Only present when the run stopped because of a temporary error. |

### What happens inside, step by step

1. **One run at a time.** A second call while a run is busy gets **409**, so runs never overlap.
2. `QUEUE_WORKERS` workers start. Each takes up to `QUEUE_BATCH_SIZE` messages in **PeekLock**
   mode. Azure never gives the same locked message to two workers.
3. Each body is split into items (one object, or every element of an array), and **every item** is checked.
4. Duplicates are merged by `external_id`: the newest copy is used, every earlier copy is a **duplicate** (goes to the DLQ).
5. **Name clashes are found before saving.** `vendors.name` is unique, so these items are not saved:
   - the name already belongs to a vendor with another `external_id` in the DB;
   - the same name is used by two `external_id`s in the batch (the first one wins).
6. The valid vendors are saved in chunks of **1,000 per transaction** (2 SQL statements each),
   using the same rules as `POST /vendors`:
   - `vendors`: insert a new `external_id`, update `name` if it changed, otherwise leave it alone.
   - `store_vendors`: link every vendor to every store (`picklist_creation = false`). Existing links are never changed.
7. Only **after the commit** is each message settled. Only **inserted** and **updated** items count as saved:
   - every item inserted or updated → the message is **completed** (deleted);
   - otherwise → a **new message with only the not-saved items** (invalid, duplicate, unchanged) is
     sent to the queue, then the original is completed. When the new message is received (usually in
     the same run) it goes straight to the **dead-letter queue** with the reasons.
8. While a big batch is being saved, the message locks are renewed every 20 s, so they never expire mid-save.

### What happens to each kind of message

| Situation | Result |
|---|---|
| Every item inserted or updated | Saved, then the message is **deleted** from the queue. Nothing in the DLQ. |
| Some items **invalid** (empty `name` / `external_id`, longer than 255, not an object, name already used by another `external_id`) | Type `invalid`. Not saved; sent to the DLQ with the reason. |
| Same `external_id` sent **twice or more** | The newest copy is saved; each earlier copy is type `duplicate` and goes to the DLQ ("sent again at item N of message seq S; the later one is used"). |
| Item **already in the DB with the same name** | Nothing to update. Type `unchanged`; goes to the DLQ ("already in the database with the same name (nothing to update)"). |
| Mixed, e.g. 100 items: 70 new/changed, 20 unchanged, 8 invalid, 2 duplicate | 70 saved; one 30-item message goes to the DLQ with each item's type and reason. |
| Same, but sending the new message fails (key without **Send** right, Azure error) | Fallback: the valid items are still saved, and the **whole original message** is dead-lettered with the reasons. Nothing is lost. |
| Body is not JSON at all | **Dead-lettered** ("invalid message body"). |
| Database down or other temporary error | **Abandoned** (stays in the queue), and the run stops (`stoppedEarly`). The next run retries it. |
| Same message delivered twice (e.g. the app crashed after saving, before deleting) | **No duplicate rows**, ever. The second time its items are `unchanged` and go to the DLQ. |
| App stopped in the middle | Messages not yet deleted come back when their lock expires. Nothing is lost. |

> **Note:** if the source re-sends the same full list, almost every item is `unchanged`, so almost
> the whole message ends up in the DLQ each time. That is intended: the DLQ shows exactly which items
> changed nothing.

### Reading the dead-letter reasons

The dead-lettered message's **body is only the not-saved items, exactly as they were sent**
(e.g. `[{"external_id":"V003","name":""}, ...]`), plus:

| Field | Example |
|---|---|
| `DeadLetterReason` | `5 vendor items not saved from message seq 11681 (1 invalid, 1 duplicate, 3 unchanged); 70 items saved` |
| `DeadLetterErrorDescription` | `item 3 (V003) [invalid]: name is required; item 4 (A0004) [duplicate]: external_id "A0004" is sent again at item 5 of message seq 11681; the later one is used; item 1 (A0001) [unchanged]: already in the database with the same name (nothing to update); ...` (cut at 4,000 characters) |
| property `invalidItems` | `[{"item":3,"external_id":"V003","name":"","type":"invalid","reason":"name is required"}, ...]` (item = position in the original message; type = `invalid`, `duplicate` or `unchanged`) |
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

**Where the time goes** (measured on `boh` from a PC in Bangladesh). Every process response has
`timings` (ms; summed over the parallel workers), and the server log has one line per run:

```json
"timings": { "countMs": 171, "receiveMs": 14583, "handleMs": 147, "settleMs": 1801, "emptyWaitMs": 0 }
```

| Run | Time |
|---|---|
| Empty queue | **0.17 s** (0.75 s for the first call after a restart) |
| 1 message | **~2.1–2.8 s** |
| 3 messages | **~2.9 s** |
| 8 messages | **~2.6 s** (received in parallel) |
| Database part (`handleMs`) | 3–150 ms per batch. Not the bottleneck. |

**What makes it fast:**

1. **Message count first.** Each run asks Service Bus how many messages are waiting (~0.17 s; needs
   the **Manage** right, which the current key has).
   - With **0 messages**, the run returns at once.
   - Otherwise it stops exactly after that many messages: there is **no end-of-queue wait**. Before this
     change, every run waited 3 s at the end.
   - Without the Manage right, the run falls back to a `QUEUE_EMPTY_WAIT_MS` wait (default 1 s).
2. **Links opened at startup.** On start, every receiver link is opened in the background (each takes
   about 2 s, so all of them take about 2 minutes). A run uses the links that are already open, so calls
   right after a restart work too, with fewer links.
3. **Parallel receive.** Service Bus delivers a message about **2 s after the receiver asks**. This was
   measured: even a receive that is already waiting gets a new message ~2.06 s after it was sent. This is
   Azure's own delay for this namespace and can't be changed by code. So a run spreads the known messages
   over the open links and receives them **in parallel**, paying that ~2 s once instead of once per message.
4. **Batch database save** (`boh`): a whole batch is saved in one transaction with 5 queries (stores,
   products, store_qty, boh upsert, store_products upsert) instead of ~7 round trips per count.

**So "under 3 s"** holds for small and medium runs. A big queue (thousands of messages) is limited by
Azure delivery. Raise `QUEUE_WORKERS` (up to 32) for more parallel links. A server in the same Azure
region as the namespace, or the Premium tier, delivers faster.

### Running it automatically

Call the same endpoint on a schedule, e.g. a systemd timer on the VPS every minute:

```sh
curl -s -X POST http://localhost:8080/queue/vendor/process
```

Overlapping calls are safe: a call that arrives while a run is busy just gets 409.

---

## 5b. The `product-hierarchies` queue

Same routes, same workers, same dead-letter handling as `vendor`. Only the processing steps differ:
they are **exactly the steps of `POST /product-hierarchies`** (the queue calls the same code).

### Message format

One row, or an array of rows. Each row has the same fields as `POST /product-hierarchies`:

```json
[
  { "level1Key": "", "level1Name": "", "level2Key": "", "level2Name": "",
    "level3Key": "100", "level3Name": "GROCERY (FOOD)", "level4Key": "105", "level4Name": "OIL",
    "level5Key": "10505", "level5Name": "OLIVE OIL", "level6Key": "" },
  { "level1Key": "", "level1Name": "", "level2Key": "", "level2Name": "",
    "level3Key": "100", "level3Name": "GROCERY (FOOD)", "level4Key": "105", "level4Name": "OIL",
    "level5Key": "10511", "level5Name": "OTHER OILS", "level6Key": "" }
]
```

### What happens to each row, step by step

1. **Normalize:** values are trimmed, numbers become text (`300` = `"300"`), and missing or `null` fields become `""`.
2. **Validate:** `level3Key` and `level3Name` are required, and every field is at most 255 characters.
   A failure makes the row **invalid**, with the endpoint's message, e.g. "The level3 key field is required.".
3. **Junk check:** a row containing `OLD-OLD` or `NULL` is type **junk** ("Row contains 'NULL', nothing saved").
4. **Shift rule:** if `level3Key` is a shift key (100, 200, 350, 400, ...), the levels move up one.
   - L3 becomes L2, L4 becomes L3, L5 becomes L4.
   - A shifted row without level 4 is **invalid**.
5. **De-duplicate:** rows with the same 5 level keys (after the shift) are merged. The newest is used;
   each earlier copy is a **duplicate**.
6. **Save:** each row is saved in **its own transaction**, the same as the endpoint:
   - departments (insert or update name and level)
   - parent links
   - category (insert or update names and `department_id`)
   - store links

   If two workers insert the same new department at the same moment, the row is retried (up to 3 times).
7. **Unchanged:** a row where nothing changed is type **unchanged** ("already in the database with the
   same data (nothing to update)"). That means every department unchanged, no parent change, category
   unchanged, and no new store link.
8. **Settle:** if all rows were saved, the message is deleted. Otherwise the saved rows stay saved, and
   only the not-saved rows go to the DLQ with their type and reason (the same split as vendor).
   - A body that is not JSON (e.g. the string `"body_product_hierarchies.json"`) is dead-lettered whole.
   - A database error abandons the message, and it is retried.

| Type | Meaning |
|---|---|
| `invalid` | Validation failed, shift without level 4, not an object, or a unique-key conflict that kept failing. |
| `junk` | Contains `OLD-OLD` or `NULL`; the feed rules ignore it (the endpoint answers `"status": "skipped"`). |
| `duplicate` | The same 5 level keys were sent again later; the later row is used. |
| `unchanged` | Already in the DB with the same data. |

In the DLQ, `external_id` holds the row's level keys joined by `/` (e.g. `100/105/10505`), and `name` holds its deepest name.

### Process response counts

`rowsSaved`, `shifted`, `departmentsInserted`, `departmentsUpdated`, `parentsChanged`, `categoriesInserted`,
`categoriesUpdated`, `storeLinksAdded`, `skipped` (= invalid), `junk`, `duplicates`, `unchanged`.

### Speed

A row takes about 10 small queries (with row locks), so this queue is slower per item than `vendor`
(which saves 1,000 per statement). Hierarchy feeds are small (thousands of rows), so a full run takes
seconds to a minute.

---

## 5c. The `products` queue

Same routes, workers and dead-letter handling as the other queues. The processing steps are **exactly
the steps of `POST /products`** (the queue calls the same code: `dto.ProductRow.Prepare`,
`ProductRepository.FindCategory` and `ProductRepository.Import`).

### Message format

One product, or an array of products. Each item has the same fields as `POST /products`:

```json
[
  { "level1Key": "", "level2Key": "", "level3Key": "100", "level4Key": "101", "level5Key": "10101", "level6Key": "",
    "sku": "0003640911312", "name": "Ctry Kitch Essence Mixed 355 ml", "brand": "COUNTRY KITCHEN",
    "status": "0", "vendorKey": "AA01", "uomBuy": "CU", "buyQty": 24, "uomSell": "1", "innerPack": 24,
    "masterPack": 0, "image": "", "primaryUpc": "0003640911312", "retailPrice": 5.29,
    "unitLandedCost": 3.3646, "skuType": "", "parentSku": "", "qtyPerParent": 0, "referenceSku": "", "shelfDays": 0 }
]
```

`moq` is optional (store_products.moq, default 1).

### What happens to each product, step by step

1. **Clean input:** values are trimmed, numbers become text, and missing or `null` fields become `""`.
2. **Validate:**
   - `sku` is required, max 255
   - `status` is empty, `0` or `1`
   - `buyQty`, `innerPack`, `masterPack`, `retailPrice`, `unitLandedCost`, `qtyPerParent`, `shelfDays` and
     `moq` must be numbers
   - level keys max 255

   A failure makes the product **invalid**, with the endpoint's message, e.g. "The buy qty field must be a number.".
3. **Calculations:**
   - status `"1"` → `A` (active), anything else (including `"0"`) → `I` (inactive)
   - empty numbers → 0, and whole-number fields drop decimals (`12.7` → 12)
   - `innerPack` 0 → 1
   - `moq` empty → 1
4. **Shift rule:** if `level3Key` is a shift key (100, 200, 350, ...): L2 = L3, L3 = L4, L4 = L5, L5 = "".
5. **De-duplicate by `sku`:** the newest copy is used, and each earlier copy is a **duplicate**.
6. **Find the category:** all 5 keys must match. If none matches, the product is **invalid**: "Category not
   found for these level keys (...). Send the product hierarchy first."
7. **Save in one transaction per product**, the same as the endpoint:
   - **brand:** key = UPPERCASE A–Z/0–9, insert or update the name
   - **product:** matched on `sku`, insert or update
   - **status I:** picklist cleanup
   - **status A:** Back Room (BOS) and "Virtual FoS - {id}" (FOS) locations in **every store**, plus
     `store_products` with the moq

   A brand name already used under another key keeps failing, so after 3 tries the product is **invalid**
   ("Conflicting row ...").
8. **Unchanged:** a product where nothing changed is type **unchanged** ("already in the database with
   the same data (nothing to update)"). That means:
   - product unchanged
   - brand unchanged or none
   - nothing cleaned up
   - no location added
9. **Settle:** the same as the other queues. Saved products stay saved, and only the not-saved ones go to
   the DLQ. A body that is not JSON (e.g. `"body_products.json"`) is dead-lettered whole. A database error
   abandons the message, and it is retried.

| Type | Meaning |
|---|---|
| `invalid` | Validation failed, category not found, not an object, or a unique-key conflict (brand name). |
| `duplicate` | The same `sku` was sent again later; the later one is used. |
| `unchanged` | Already in the DB with the same data. |

In the DLQ, `external_id` holds the `sku` and `name` holds the product name.

### Process response counts

- **Products:** `productsInserted`, `productsUpdated`, `active`, `inactive`, `shifted`
- **Brands:** `brandsInserted`, `brandsUpdated`
- **Locations:** `backRoomsCreated`, `bosAdded`, `fosLocationsCreated`, `fosAdded`, `picklistProductsRemoved`
- **Not saved:** `skipped` (= invalid), `duplicates`, `unchanged`

### Speed

Each product is one transaction. An **active** product also checks and fills BOS + FOS in every store
(25 stores = 25 checks), so it is the slowest item type. Inactive products are faster.

---

## 5d. The `product-upcs` queue

Same routes, workers and dead-letter handling as the other queues. The processing steps are **exactly
the steps of `POST /product-upcs`**. The queue calls the same code: `dto.CheckSkuLength`,
`ProductUpcRepository.FindProductBySku` and `ProductUpcRepository.Save`.

### Message format

One item, or an array of items, with the same fields as `POST /product-upcs`:

```json
[
  { "upc": "999000000002", "sku": "1250563665" },
  { "upc": "999000000002", "sku": "078933354" }
]
```

Send both values as JSON **strings**. A number with a leading zero (`078933354`) is not valid JSON.

### What happens to each item, step by step

1. **Clean input:** `upc` and `sku` are trimmed. Missing or `null` values become `""`.
2. **Validate:** `upc` and `sku` are required, max 255. A failure makes the item **invalid**, with the
   endpoint's message, e.g. "The upc field is required.".
3. **SKU length:** the SKU must be 8 to 13 characters and is used as sent (no zeros added or removed).
   Otherwise the item is **invalid**: "SKU must be 8 to 13 characters (got 4).".
4. **De-duplicate by `upc`:** a UPC belongs to **one** product, so when the same UPC is sent twice the
   later item wins (the endpoint would move it there). Each earlier copy is a **duplicate**.
   In the example above, item 1 is a duplicate and item 2 is saved.
5. **Find the product** by exact `sku` (inactive products count too). If none matches, the item is
   **invalid**: "Product not found for this SKU (…). Send the product feed first."
6. **Save in one transaction per item**, the same as the endpoint:
   - **UPC not found:** insert it (counts as `inserted`).
   - **UPC on another product:** move it to this product (counts as `moved`).
   - **UPC already on this product:** nothing changes; the item is type **unchanged** ("already in the
     database for this sku (nothing to update)").

   If two workers insert the same new UPC at once, the item is retried (up to 3 times).
7. **Settle:** the same as the other queues.
   - Saved items stay saved, and only the not-saved ones go to the DLQ.
   - A body that is not JSON (e.g. `"body_product_upcs.json"`) is dead-lettered whole.
   - A database error abandons the message, and it is retried.

| Type | Meaning |
|---|---|
| `invalid` | Validation failed, SKU not 8–13 characters, product not found, or not an object. |
| `duplicate` | The same `upc` was sent again later; the later one is used. |
| `unchanged` | The UPC is already on this product. |

In the DLQ, `external_id` holds the `upc` and `name` holds `sku <sku>`.

### Process response counts

`inserted`, `moved`, `skipped` (= invalid), `duplicates`, `unchanged`.

---

## 5e. The `boh` queue

BOH = back-of-house stock counts. Same routes, workers and dead-letter handling as the other queues. The
processing steps are **exactly the steps of `POST /boh`**. The queue calls the same code: `dto.BohQty`,
`BohRepository.FindStoreByExternalID`, `BohRepository.FindProductBySku` and `BohRepository.Save`.

### Message format

One count, or an array of counts, with the same fields as `POST /boh`:

```json
[
  { "storeId": "112", "sku": "0004145811704", "qty": "58", "date": "2026-10-14" },
  { "storeId": "112", "sku": "0 04003213006", "qty": "0", "date": "2022-03-01" }
]
```

### What happens to each count, step by step

1. **Clean input:** values are trimmed. Missing or `null` values become `""`.
2. **Validate**, with the endpoint's messages:
   - `storeId` must be a whole number ("The store id field must be an integer.")
   - `sku` is required, max 255
   - `qty` must be a number ("The qty field must be a number.")
   - `date` must be a real date in `YYYY-MM-DD` ("The date field must match the format Y-m-d.")

   A failure makes the count **invalid**.
3. **Quantity rule:** round UP to a whole number, then a negative becomes 0 (`56.2` → 57, `-2.5` → 0).
4. **De-duplicate** by (store, product, date), the BOH row's unique key. The newest copy is used;
   each earlier copy is a **duplicate**.
5. **Find the store** by `stores.external_id` (not `stores.id`). If none matches → **invalid**: "Store not
   found for this storeId (…) (stores.external_id)."
6. **Find the product** by exact `sku`. It must be **active** (status A).
   - Not found → **invalid**: "Product not found for this SKU (…)."
     SKUs like `.750154559506` or `0 04003213006` only match if a product has exactly that sku.
   - Not active → **invalid**: "Product is not active (status I). BOH is only stored for active products."
7. **Save the whole batch in ONE transaction** (`BohRepository.SaveBatch`, 5 queries for up to 100
   messages). The rules are the same as the endpoint, and it was tested to give exactly the same rows:
   - `store_qty`: what the system holds in this store before the count (the sum of the product's location qty)
   - the `boh` rows: insert, or update the qty (`ON CONFLICT (store_id, product_id, date)`)
   - `store_products.total_qty` = the **last** count of the batch for each store + product (the
     endpoint's "last count wins")

   Stores and products are looked up once per batch. Rows are written in key order, so parallel batches
   never deadlock. A unique-key race with another writer is retried (up to 3 times).
8. **Unchanged:** a count is type **unchanged** ("already in the database with the same qty for this
   store, sku and date (nothing to update)") when its boh row didn't change AND it didn't set a new
   `total_qty`. Sending the same message again gives all `unchanged`, even with several dates for the
   same store + sku.
9. **Settle:** the same as the other queues.
   - Saved counts stay saved, and only the not-saved ones go to the DLQ.
   - A body that is not JSON (e.g. `"body_boh.json"`) is dead-lettered whole.
   - A database error abandons the message, and it is retried.

| Type | Meaning |
|---|---|
| `invalid` | Validation failed, store not found, product not found, or product not active. |
| `duplicate` | The same store + sku + date was sent again later; the later one is used. |
| `unchanged` | Already in the DB with the same qty. |

In the DLQ, `external_id` holds the `sku` and `name` holds `store <storeId> / <date>`.

### Process response counts

`bohInserted`, `bohUpdated`, `storeProductsInserted`, `storeProductsUpdated`, `skipped` (= invalid),
`duplicates`, `unchanged`.

---

## 5f. The `deliveries` queue

The steps are **exactly the steps of `POST /deliveries`**. The queue uses `dto.DeliveryRequest.Check` and
the same `DeliveryRepository` calls.

```json
[
  { "timestamp": "2024-11-26T00:00:00Z", "asn": "1", "eta": "2024-11-26T16:59:14Z", "deliveryUnitCode": "1",
    "deliveryUnitNumber": "1", "sku": "0006661348701", "description": "Brunswick Vienna Sausages Chicken 141g",
    "expectedCases": "5", "expectedUnits": "0", "damagedCases": "0", "from": "BS03", "storeId": "112" }
]
```

1. **Validate:**
   - `asn` is a whole number from 1 to 2147483647
   - `eta` is a real date (ISO recommended)
   - `deliveryUnitCode`, `sku` and `storeId` are required
   - text fields are max 255
   - quantities must be numbers

   A failure makes the row **invalid**, with the endpoint's message, e.g. "The asn field must be between 1 and 2147483647.".
2. **Convert:** quantities round UP (`2.1` → 3); negatives are **kept** (`-1.5` → -1); empty → 0. The ETA
   is stored in UTC. `timestamp` and `deliveryUnitNumber` are accepted but not stored.
3. **De-duplicate** by store + asn + deliveryUnitCode + sku (the newest is used).
4. **Store** not found → **invalid**. **Product** not found or not active → **invalid**.
5. **Save in one transaction per row:** the delivery (asn + store), the pallet (delivery unit) and the
   delivery product.
   - If the delivery has **already started** (status is not `planned`), nothing changes and the row is
     **invalid**: "Delivery has already started (status …). Row skipped."
   - If nothing changed, the row is **unchanged**.
6. **Activity logs:** the endpoint's three events, written **once per event and store per batch**, each
   with every skipped row in it (the endpoint writes one per row):
   - `DELIVERY_STORE_NOT_FOUND_SKIPPED`
   - `DELIVERY_PRODUCTS_NOT_FOUND_SKIPPED`
   - `DELIVERY_FEED_RECORDS_SKIPPED`
7. Post-feed processing stays **disabled**, as in the endpoint.

**Counts:** `deliveriesInserted`, `deliveriesUpdated`, `unitsInserted`, `productsInserted`,
`productsUpdated`, `blocked`, `activityLogs`, `skipped` (= invalid), `duplicates`, `unchanged`.
In the DLQ, `external_id` holds the `sku` and `name` holds `store … / asn … / unit …`.

---

## 5g. The `stocktake` queue

The steps are **exactly the steps of `POST /stocktake`**. The queue uses `dto.StockTakeRequest.Counts`
and the same `StockTakeRepository` calls.

```json
[ { "storeId": "112", "sku": "0000050147311", "bosQty": "0", "fosQty": "24" } ]
```

1. **Validate:** `storeId` is a whole number, `sku` is required, `bosQty` and `fosQty` are required
   numbers. A failure makes the count **invalid**.
2. **Convert:**
   - locations: below 1 → cleared (0), otherwise rounded DOWN (`5.7` → 5)
   - `stock_takes`: rounded UP (`5.7` → 6)
3. **De-duplicate** by store + sku (the newest is used).
4. **Store** not found → **invalid**. **Product** not found or not active → **invalid**.
5. **Safety checks**, each → **invalid**:
   - the product's department is in **sync-up** mode in this store
   - the product has **no back-of-store location** in this store
   - the open-picklist check stays **disabled**, as in the endpoint
6. **Save in one transaction per count:** replace the FOS and BOS location quantities, then the
   `stock_takes` row. If nothing changed, the count is **unchanged**.

**Counts:** `locationsUpdated`, `stockTakesInserted`, `stockTakesUpdated`, `skipped`, `duplicates`, `unchanged`.
In the DLQ, `external_id` holds the `sku` and `name` holds `store …`.

---

## 5h. The `product-variants` queue

The steps are **exactly the steps of `POST /product-variants`**.

```json
[ { "parentSku": "0000000003145", "variantSku": "0020408700000" }, { "parentSku": "0000000003145", "variantSku": "4087" } ]
```

1. **Validate:** `parentSku` and `variantSku` are required, max 255.
2. **De-duplicate** by parentSku + variantSku.
3. **Parent product** not found or not active → **invalid**.
4. **Link in one transaction:** a new (product, variant) link is inserted. If the link already exists,
   the item is **unchanged**. The same variant code may be linked to several products, as in the endpoint.

**Counts:** `linked`, `skipped`, `duplicates`, `unchanged`.
In the DLQ, `external_id` holds the `variantSku` and `name` holds `parent …`.

---

## 5i. The `pos` queue

The steps are **exactly the steps of `POST /pos`**. The queue uses `dto.PosRequest.Check` and
`PosRepository.FindProductsByCodes` / `Import` (`POST /pos` uses `FindProducts`, which calls the same code).

```json
[
  { "timestamp": "2026-09-22T18:01:14Z", "storeId": "106", "register": "002", "operator": "",
    "transactionType": "SALE", "transaction": "716886", "seqNumber": "1", "sku": "0078758001005", "qty": "1",
    "regularPriceEach": "3.59", "soldPriceEach": "3.59", "isPriceOverride": "0", "discountCode": "0" }
]
```

1. **Validate:**
   - `timestamp` is a real date
   - `storeId` and `register` are required whole numbers, and `register` must not be 0
   - `operator`, `transaction` and `seqNumber` must fit an integer column
   - `sku` is required
   - qty and prices must be numbers

   A failure makes the line **invalid**.
2. **Convert:**
   - qty rounds UP (`1.2` → 2), and a negative becomes 0
   - `transaction` / `seqNumber`: empty or 0 → 1
   - `isPriceOverride`: `1` or `true` → true
   - `discountCode` is truncated
3. **De-duplicate** by store + register + transaction + seqNumber + sku. A repeated line in a batch is a
   **duplicate** and is **never applied twice**.
4. **Store** not found → **invalid**. **No active product** for this code (by sku, primary_upc or variant)
   → **invalid**. The stores and the products of the whole batch are looked up **once** (one query for
   all the codes) before any line is saved.
5. **Import in one transaction per line.** The lines of one store run one by one, in message order. Up
   to 8 different stores of the batch run **at the same time** (they share no rows). A deadlock or
   unique-key race with another worker is retried (up to 3 times; the transaction was rolled back, so
   nothing is counted twice). Each line does:
   - deduct `store_products.total_qty` (never below 0) and add to `deduction_counter`
   - replenishment picklists
   - the `pos` row (a new line, or qty added to the same store + register + product + transaction)
   - remove empty picklists

   Other outcomes:
   - A failed picklist safety check → **invalid**, and nothing is saved.
   - A code matched only by `primary_upc` → **unchanged**: the sale is not recorded or deducted, as in the endpoint.

> **Never resend a processed sale.** Unlike the other queues, POS is **not idempotent**: processing the
> same line again adds its qty again and deducts the stock again. So:
> - inside a batch, a repeated line is a `duplicate`, not applied twice;
> - if the database fails in the middle (this stops only that store's remaining lines), a message
>   with **no** saved line is retried (safe), but a
>   message that already saved some lines is **not** retried whole. Its unsaved lines go to the DLQ
>   ("database error before this sale line was saved; resend ONLY this line").
> - When you resend from the DLQ, send only those lines.

**Counts:** `recorded` (new pos rows), `merged` (qty added to an existing row), `deducted`,
`picklistsCreated`, `emptyPicklistsRemoved`, `skipped`, `duplicates`, `unchanged`.
In the DLQ, `external_id` holds the `sku` and `name` holds `store … / register … / transaction … / seq …`.

**Speed** (database part, `handleMs`), measured on the same 74-line batch across 3 stores and rolled back:

| | Before | Now |
|---|---|---|
| Whole batch | 1.71 s | **~0.29 s** with stores one by one; parallel stores make it faster still |
| Product lookup | ~17–21 ms **per line** (`sku OR primary_upc OR variant` scanned the active products) | **one query per batch**: 501 codes in 73 ms |
| Picklist step | 2 location queries, plus 1 query per candidate in 7.7 | 1 location query, plus 1 query for all candidates |

The results are **identical**: every outcome, reason, count, `store_products`, `pos` and picklist row was
compared. `products.primary_upc` has only a trigram index, which is slow for exact matches. The lookup
avoids it, so no new index is needed. A plain btree index on `products(primary_upc)` would make it faster
still, but this app does not create indexes on the shared DB.

---

## 6. Postman

1. Import `postman/feed-api.json`.
2. Open the **Queue (Service Bus)** folder. It has one sub-folder per queue, plus **Errors**:

   | Sub-folder | Requests |
   |---|---|
   | **Vendor** | peek (first 10, next page), send (array, single object), process (max 100, whole queue), dead-letter |
   | **Product hierarchies** | the same 7 requests for `product-hierarchies` |
   | **Products** | the same 7 requests for `products` (send uses a 3-product sample) |
   | **Product UPCs** | the same 7 requests for `product-upcs` |
   | **BOH** | the same 7 requests for `boh` |
   | **Deliveries**, **Stocktake**, **Product variants**, **POS** | the same 7 requests for each (send uses your sample) |
   | **Errors** | unknown queue (404 for peek, send and process); dead-letter without `?queue` (422); invalid JSON (422); empty array (422); bad `count` (422); bad `max` (422) |

   Every request has a saved **example response** (open the request, then **Examples**), so you can see the
   answer without calling it.

**Send** and **process** change real data. Peek and dead-letter only read.

---

## 7. Add another queue later (e.g. `orders`)

No new routes are needed: peek, send and dead-letter work for any queue in `SERVICE_BUS_ENTITY`.

1. `.env`: add the name, e.g. add `,orders` to the end of `SERVICE_BUS_ENTITY`, then restart.
   **Peek, send and dead-letter work now.** Peek a few messages to see the body format.
2. Write `internal/queue/order_processor.go`. `product_upc_processor.go` is the shortest example:
   parse the body with `splitBody`, validate each item, save it with the same repository the endpoint
   uses, collect the not-saved items, then call `finish(...)` (in `items.go`).
3. Register it with a single line in `internal/routes/routes.go`, next to the others:
   ```go
   consumers["orders"] = queue.NewConsumer(sbClient, "orders", queue.NewOrderProcessor(orderRepo), cfg.QueueWorkers, cfg.QueueBatchSize)
   ```
4. `POST /queue/orders/process` works. Login, workers, batching, settling, the 409 guard and the
   dead-letter split all come from `internal/queue/consumer.go` and `items.go`.

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
