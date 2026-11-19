# sqs

An Amazon SQS client for slang: send one message or a batch, receive with
long polling, delete, and change visibility. It speaks SQS's JSON protocol
and signs with the stdlib's `sigv4`. Works against AWS, LocalStack and
ElasticMQ.

Needs slang 0.2.8 or later (the `sigv4` package).

```
pkg sqs git https://github.com/dolphlabs/sqs.git tag v0.1.0 dir src/sqs
```

```slang
import "proc";
import "sqs";
import "time";

let deadline = until_of(time.mono() + 30000000000);
let r = sqs.from_env(proc.getenv);   // AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
                                     // AWS_SESSION_TOKEN, SQS_ENDPOINT
guard let c = r else let e = err_of(r) { panic(e); }
let url = sqs.queue_url(c, "orders", deadline) ?? "";

sqs.send(c, url, "{\"id\":42}", deadline);           // ok(Sent): id, sequence (FIFO)
let m = sqs.outgoing("{\"id\":43}");
m.delay_seconds = 30;
m.attributes["kind"] = "refund";
sqs.send_message(c, url, m, deadline);
sqs.send_batch(c, url, [sqs.outgoing("a"), sqs.outgoing("b")], deadline);  // a BatchResult per message

let got = sqs.receive(c, url, 10, 20, -1, deadline);  // up to 10, long poll 20 s, the queue's visibility
for msg in got ?? [] {
    // msg.body, msg.attributes["kind"], msg.system["ApproximateReceiveCount"]
    sqs.delete(c, url, msg.receipt, deadline);
}
```

- **Checked both ways.** SQS returns an MD5 of every body it stores and
  sends. The client compares it with the body sent or received, and a
  mismatch is an error, not a message.
- **Checked before sending.** Bodies (SQS's allowed characters), attribute
  names, delays and batch sizes are checked first, and every problem is
  reported at once.
- **Batches report per message.** `send_batch` and `delete_batch` return
  `err` only when the call fails. Each entry's own outcome is in its
  `BatchResult`, in the order given.
- **Errors carry SQS's code**: `sqs: QueueDoesNotExist: The specified
  queue does not exist.`
- Queues: `queue_url`, `create_queue` (attributes by SQS's names,
  `FifoQueue` included), `delete_queue`, `purge`, `attributes`.
  Messages: `change_visibility`, `delete_batch`.

Not yet: binary message attributes, automatic retries on throttling.

`make test` runs the unit tests and `make live` runs the client against
ElasticMQ in Docker. MIT licence.
