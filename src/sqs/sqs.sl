// Amazon SQS: send (one or a batch), receive with long polling, delete,
// and change visibility, over SQS's JSON protocol, signed with the
// stdlib's sigv4. Works against AWS, LocalStack and ElasticMQ.
//
//     import "sqs";
//
//     let r = sqs.from_env(proc.getenv);   // AWS_REGION, AWS_ACCESS_KEY_ID, ...
//     guard let c = r else let e = err_of(r) { panic(e); }
//     let url = sqs.queue_url(c, "orders", deadline) ?? "";
//     sqs.send(c, url, "{\"id\":42}", deadline);
//     let got = sqs.receive(c, url, 10, 20, -1, deadline);   // long poll 20 s
//     for m in got ?? [] {
//         // ... handle m.body ...
//         sqs.delete(c, url, m.receipt, deadline);
//     }

import "httpc";
import "json";
import "sigv4";
import "strings";

pub gc struct Client {
    // scheme://host[:port]
    endpoint: str,
    host: str,
    signer: sigv4.Signer,
    http: httpc.Client,
}

// A client for endpoint ("https://sqs.eu-west-1.amazonaws.com",
// "http://localhost:4566" for LocalStack). Requests go to the endpoint;
// queue URLs only name the queue.
pub fn new_client(endpoint: str, region: str, access_key: str, secret_key: str)
                  -> result[Client, str] {
    let u = strings.trim(endpoint);
    while strings.has_suffix(u, "/") {
        u = strings.slice(u, 0, len(u) - 1);
    }
    let rest = "";
    if strings.has_prefix(u, "https://") {
        rest = strings.slice(u, 8, len(u));
    } else if strings.has_prefix(u, "http://") {
        rest = strings.slice(u, 7, len(u));
    } else {
        return err("sqs: endpoint must start with https:// or http://");
    }
    if rest == "" || strings.contains(rest, "/") || strings.contains(rest, "?") {
        return err("sqs: endpoint is scheme://host[:port] with no path");
    }
    if region == "" {
        return err("sqs: region is required");
    }
    if access_key == "" || secret_key == "" {
        return err("sqs: access key and secret key are required");
    }
    let host = rest;
    if strings.has_prefix(u, "https://") && strings.has_suffix(host, ":443") {
        host = strings.slice(host, 0, len(host) - 4);
    } else if strings.has_prefix(u, "http://") && strings.has_suffix(host, ":80") {
        host = strings.slice(host, 0, len(host) - 3);
    }
    let scheme = strings.slice(u, 0, strings.find(u, "://"));
    return ok(Client { endpoint: scheme + "://" + host, host: host,
                       signer: sigv4.new_signer(access_key, secret_key, region, "sqs"),
                       http: httpc.new_client() });
}

// AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN (optional),
// AWS_REGION (or AWS_DEFAULT_REGION), and SQS_ENDPOINT (default: the
// region's AWS endpoint).
pub fn from_env(getenv: fn(str) -> opt[str]) -> result[Client, str] {
    let region = getenv("AWS_REGION") ?? (getenv("AWS_DEFAULT_REGION") ?? "");
    if region == "" {
        return err("sqs: AWS_REGION is not set");
    }
    let endpoint = getenv("SQS_ENDPOINT") ?? ("https://sqs." + region + ".amazonaws.com");
    let r = new_client(endpoint, region, getenv("AWS_ACCESS_KEY_ID") ?? "",
                       getenv("AWS_SECRET_ACCESS_KEY") ?? "");
    guard let c = r else let e = err_of(r) { return err(e); }
    c.signer.session_token = getenv("AWS_SESSION_TOKEN") ?? "";
    return ok(c);
}

// One signed call; the response body, or SQS's error.
fn call(c: Client, action: str, body: str, deadline: until) -> result[str, str] {
    let payload = to_bytes(body);
    let h: map[str]str = {};
    h["content-type"] = "application/x-amz-json-1.0";
    h["x-amz-target"] = "AmazonSQS." + action;
    let amzdate = sigv4.amz_date_now();
    let auth = sigv4.sign(c.signer, "POST", c.host, "/", {}, h, sigv4.payload_hash(payload), amzdate);
    let req = httpc.new_request("POST", c.endpoint + "/");
    req.headers["Content-Type"] = "application/x-amz-json-1.0";
    req.headers["X-Amz-Target"] = "AmazonSQS." + action;
    req.headers["X-Amz-Date"] = amzdate;
    if c.signer.session_token != "" {
        req.headers["X-Amz-Security-Token"] = c.signer.session_token;
    }
    req.headers["Authorization"] = auth;
    req.body = payload;
    // a redirect would change the host the signature covers
    req.max_redirects = 0;
    let rr = httpc.client_send(c.http, req, deadline);
    guard let resp = rr else let e = err_of(rr) { return err("sqs: " + action + ": " + e); }
    let text = to_str(resp.body);
    if resp.status < 200 || resp.status > 299 {
        return err(error_text(resp.status, text));
    }
    return ok(text);
}

fn check_url(url: str) -> result[bool, str] {
    if !strings.has_prefix(url, "https://") && !strings.has_prefix(url, "http://") {
        return err("sqs: a queue URL starts with https:// (get one with queue_url)");
    }
    return ok(true);
}

// ---- queues -----------------------------------------------------------------------

pub fn queue_url(c: Client, name: str, deadline: until) -> result[str, str] {
    let r = call(c, "GetQueueUrl", "{\"QueueName\":" + json.encode(name) + "}", deadline);
    guard let body = r else let e = err_of(r) { return err(e); }
    return decode_url(body);
}

// Creates the queue, or returns the URL of an existing one with the same
// attributes. Attributes are SQS's names: VisibilityTimeout, FifoQueue,
// ContentBasedDeduplication, MessageRetentionPeriod, RedrivePolicy, ...
pub fn create_queue(c: Client, name: str, attributes: map[str]str, deadline: until)
                    -> result[str, str] {
    let b = "{\"QueueName\":" + json.encode(name);
    if len(attributes) > 0 {
        b = b + ",\"Attributes\":" + json.encode(attributes);
    }
    let r = call(c, "CreateQueue", b + "}", deadline);
    guard let body = r else let e = err_of(r) { return err(e); }
    return decode_url(body);
}

pub fn delete_queue(c: Client, url: str, deadline: until) -> result[bool, str] {
    let r = call(c, "DeleteQueue", "{\"QueueUrl\":" + json.encode(url) + "}", deadline);
    guard let _b = r else let e = err_of(r) { return err(e); }
    return ok(true);
}

pub fn purge(c: Client, url: str, deadline: until) -> result[bool, str] {
    let r = call(c, "PurgeQueue", "{\"QueueUrl\":" + json.encode(url) + "}", deadline);
    guard let _b = r else let e = err_of(r) { return err(e); }
    return ok(true);
}

// Every attribute of the queue (ApproximateNumberOfMessages, ...).
pub fn attributes(c: Client, url: str, deadline: until) -> result[map[str]str, str] {
    let r = call(c, "GetQueueAttributes",
                 "{\"QueueUrl\":" + json.encode(url) + ",\"AttributeNames\":[\"All\"]}", deadline);
    guard let body = r else let e = err_of(r) { return err(e); }
    return decode_attributes(body);
}

// ---- messages ---------------------------------------------------------------------

pub fn send(c: Client, url: str, body: str, deadline: until) -> result[Sent, str] {
    return send_message(c, url, outgoing(body), deadline);
}

pub fn send_message(c: Client, url: str, m: Outgoing, deadline: until) -> result[Sent, str] {
    let ur = check_url(url);
    guard let _u = ur else let e = err_of(ur) { return err(e); }
    let problems = check_outgoing(m);
    if len(problems) > 0 {
        return err("sqs: " + problems);
    }
    let r = call(c, "SendMessage", send_body(url, m), deadline);
    guard let body = r else let e = err_of(r) { return err(e); }
    return decode_sent(body, m.body);
}

// Up to 10 messages in one call. err only when the call itself failed;
// each entry's own outcome is in its BatchResult, in the order given.
pub fn send_batch(c: Client, url: str, ms: [Outgoing], deadline: until)
                  -> result[[BatchResult], str] {
    let ur = check_url(url);
    guard let _u = ur else let e = err_of(ur) { return err(e); }
    if len(ms) == 0 || len(ms) > max_batch() {
        return err("sqs: a batch holds 1 to 10 messages");
    }
    let problems: [str] = [];
    let bodies: [str] = [];
    for i in 0..len(ms) {
        let p = check_outgoing(ms[i]);
        if len(p) > 0 {
            push(problems, "message " + to_str(i) + ": " + p);
        }
        push(bodies, ms[i].body);
    }
    if len(problems) > 0 {
        return err("sqs: " + strings.join(problems, "; "));
    }
    let r = call(c, "SendMessageBatch", send_batch_body(url, ms), deadline);
    guard let body = r else let e = err_of(r) { return err(e); }
    return decode_batch(body, bodies);
}

// Up to `max` (1-10) messages. wait_seconds (0-20) long-polls: the call
// returns as soon as a message arrives, or empty after the wait. The
// deadline must leave room for the wait. visibility is the seconds the
// messages stay hidden from other receivers, or -1 for the queue's own.
pub fn receive(c: Client, url: str, max: int, wait_seconds: int, visibility: int,
               deadline: until) -> result[[Message], str] {
    let ur = check_url(url);
    guard let _u = ur else let e = err_of(ur) { return err(e); }
    if max < 1 || max > 10 {
        return err("sqs: receive takes 1 to 10 messages");
    }
    if wait_seconds < 0 || wait_seconds > 20 {
        return err("sqs: wait_seconds is 0 to 20");
    }
    if visibility < -1 || visibility > 43200 {
        return err("sqs: visibility is 0 to 43200 seconds, or -1 for the queue's");
    }
    let r = call(c, "ReceiveMessage", receive_body(url, max, wait_seconds, visibility), deadline);
    guard let body = r else let e = err_of(r) { return err(e); }
    return decode_receive(body);
}

pub fn delete(c: Client, url: str, receipt: str, deadline: until) -> result[bool, str] {
    let r = call(c, "DeleteMessage",
                 "{\"QueueUrl\":" + json.encode(url) + ",\"ReceiptHandle\":" + json.encode(receipt) + "}",
                 deadline);
    guard let _b = r else let e = err_of(r) { return err(e); }
    return ok(true);
}

// Up to 10 receipts; each one's outcome in order.
pub fn delete_batch(c: Client, url: str, receipts: [str], deadline: until)
                    -> result[[BatchResult], str] {
    if len(receipts) == 0 || len(receipts) > max_batch() {
        return err("sqs: a batch holds 1 to 10 receipts");
    }
    let r = call(c, "DeleteMessageBatch", receipts_body(url, receipts), deadline);
    guard let body = r else let e = err_of(r) { return err(e); }
    let none_: [str] = [];
    for _x in receipts {
        push(none_, "");
    }
    return decode_batch(body, none_);
}

// Hides the message for `seconds` more from now (0 makes it visible at
// once): more time for slow work, or an early retry.
pub fn change_visibility(c: Client, url: str, receipt: str, seconds: int, deadline: until)
                         -> result[bool, str] {
    if seconds < 0 || seconds > 43200 {
        return err("sqs: visibility is 0 to 43200 seconds");
    }
    let r = call(c, "ChangeMessageVisibility",
                 "{\"QueueUrl\":" + json.encode(url) + ",\"ReceiptHandle\":" + json.encode(receipt) +
                 ",\"VisibilityTimeout\":" + to_str(seconds) + "}", deadline);
    guard let _b = r else let e = err_of(r) { return err(e); }
    return ok(true);
}
