// The client against ElasticMQ in Docker (tests/live/run.sh): queues,
// one message and a batch, attributes, long polling, visibility, deletes,
// a FIFO queue's order, and SQS's errors.

import "sqs";
import "strings";
import "time";

fn dl() -> until { return until_of(time.mono() + 30000000000); }

fn must[T](r: result[T, str], what: str) -> T {
    guard let v = r else let e = err_of(r) { println(what + ": " + e); exit(1); }
    return v;
}

let c = must(sqs.new_client("http://localhost:19324", "elasticmq", "x", "y"), "client");
let attrs: map[str]str = {};
attrs["VisibilityTimeout"] = "30";
let url = must(sqs.create_queue(c, "live-orders", attrs, dl()), "create_queue");
println("queue: " + to_str(strings.has_suffix(url, "/live-orders")));
println("queue_url: " + to_str(must(sqs.queue_url(c, "live-orders", dl()), "queue_url") == url));

let s = must(sqs.send(c, url, "{\"id\":1} naïve ✓", dl()), "send");
println("sent: " + to_str(len(s.id) > 0));
let m = sqs.outgoing("with attributes");
m.attributes["kind"] = "refund";
must(sqs.send_message(c, url, m, dl()), "send_message");
let batch: [sqs.Outgoing] = [];
for i in 0..10 {
    push(batch, sqs.outgoing("batch " + to_str(i)));
}
let br = must(sqs.send_batch(c, url, batch, dl()), "send_batch");
let okn = 0;
for b in br {
    if b.ok { okn += 1; }
}
println("batch ok: " + to_str(okn) + "/10");

// Long poll until all 12 are in hand.
let got: map[str]sqs.Message = {};
let receipts: [str] = [];
let tries = 0;
while len(got) < 12 && tries < 10 {
    tries += 1;
    for msg in must(sqs.receive(c, url, 10, 2, 60, dl()), "receive") {
        got[msg.body] = msg;
        push(receipts, msg.receipt);
    }
}
println("received: " + to_str(len(got)));
println("utf-8 body intact: " + to_str(has(got, "{\"id\":1} naïve ✓")));
println("attribute: " + got["with attributes"].attributes["kind"]);
println("receive count: " + got["batch 0"].system["ApproximateReceiveCount"]);

// Make one visible again at once; it comes back.
must(sqs.change_visibility(c, url, got["batch 3"].receipt, 0, dl()), "change_visibility");
let again = must(sqs.receive(c, url, 1, 2, 60, dl()), "receive again");
println("back after visibility 0: " + to_str(len(again) == 1 && again[0].body == "batch 3"));
push(receipts, again[0].receipt);

must(sqs.delete(c, url, receipts[0], dl()), "delete");
let rest = receipts[1..len(receipts)];
let deleted = 0;
let i = 0;
while i < len(rest) {
    let end = i + 10;
    if end > len(rest) { end = len(rest); }
    for r in must(sqs.delete_batch(c, url, rest[i..end], dl()), "delete_batch") {
        if r.ok { deleted += 1; }
    }
    i = end;
}
println("deleted: " + to_str(deleted + 1));
let left = must(sqs.attributes(c, url, dl()), "attributes");
println("left: " + left["ApproximateNumberOfMessages"]);

// FIFO: order kept within a group.
let fattrs: map[str]str = {};
fattrs["FifoQueue"] = "true";
fattrs["ContentBasedDeduplication"] = "true";
let furl = must(sqs.create_queue(c, "live-steps.fifo", fattrs, dl()), "create fifo");
for n in 0..5 {
    let f = sqs.outgoing("step " + to_str(n));
    f.group_id = "g";
    must(sqs.send_message(c, furl, f, dl()), "send fifo");
}
let order: [str] = [];
let ft = 0;
while len(order) < 5 && ft < 10 {
    ft += 1;
    for msg in must(sqs.receive(c, furl, 10, 2, 60, dl()), "receive fifo") {
        push(order, msg.body);
        must(sqs.delete(c, furl, msg.receipt, dl()), "delete fifo");
    }
}
println("fifo: " + strings.join(order, ","));

// Errors carry SQS's code; bad input is refused before any request.
let missing = sqs.queue_url(c, "no-such-queue", dl());
if let _u = missing { println("missing queue found?"); } else let e = err_of(missing) {
    println("missing: " + to_str(strings.contains(e, "QueueDoesNotExist") || strings.contains(e, "NonExistentQueue")));
}
let bad = sqs.outgoing("");
bad.delay_seconds = 901;
let br2 = sqs.send_message(c, url, bad, dl());
if let _b = br2 { println("bad sent?"); } else let e = err_of(br2) { println("refused: " + e); }

must(sqs.delete_queue(c, furl, dl()), "delete fifo queue");
must(sqs.delete_queue(c, url, dl()), "delete_queue");
println("done");
