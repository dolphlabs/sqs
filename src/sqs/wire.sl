// SQS's JSON protocol (AWS JSON 1.0) as pure functions: request bodies
// built from checked values, responses and errors decoded, body digests
// verified. No network and no clock, so every rule here is a unit test.

import "builder";
import "crypto";
import "encoding";
import "json";
import "strings";

// One message to send.
pub gc struct Outgoing {
    body: str,
    // 0 to 900 seconds before the message can be received.
    delay_seconds: int,
    // FIFO queues: required group id, and a deduplication id unless the
    // queue derives it from the body (ContentBasedDeduplication).
    group_id: str,
    dedup_id: str,
    // String message attributes.
    attributes: map[str]str,
}

pub fn outgoing(body: str) -> Outgoing {
    let a: map[str]str = {};
    return Outgoing { body: body, delay_seconds: 0, group_id: "", dedup_id: "", attributes: a };
}

// One received message. `receipt` deletes it or changes its visibility.
pub gc struct Message {
    id: str,
    receipt: str,
    body: str,
    // System attributes SQS sends: ApproximateReceiveCount,
    // SentTimestamp, MessageGroupId, ...
    system: map[str]str,
    // String and Number message attributes the sender set.
    attributes: map[str]str,
}

pub struct Sent { id: str, sequence: str }

// The outcome of one entry of a batch: ok, or SQS's code and message.
pub gc struct BatchResult { index: int, ok: bool, id: str, error: str }

// The largest batch SQS takes.
pub fn max_batch() -> int { return 10; }

fn q(s: str) -> str { return json.encode(s); }

// SQS's limits on a body (its allowed characters, RFC XML 1.0's): tab,
// LF, CR, and U+0020 upward but the surrogates and U+FFFE/U+FFFF.
// Checked here so the error names the message rather than the request.
pub fn check_body(s: str) -> result[bool, str] {
    if len(s) == 0 {
        return err("a message body cannot be empty");
    }
    let b = to_bytes(s);
    let i = 0;
    let n = len(b);
    while i < n {
        let c = b[i] as int;
        if c < 32 {
            if c != 9 && c != 10 && c != 13 {
                return err("a message body cannot hold control character " + to_str(c) + " (at byte " + to_str(i) + ")");
            }
        } else if c == 239 && i + 2 < n && (b[i + 1] as int) == 191 && ((b[i + 2] as int) == 190 || (b[i + 2] as int) == 191) {
            return err("a message body cannot hold U+FFFE or U+FFFF (at byte " + to_str(i) + ")");
        }
        i += 1;
    }
    return ok(true);
}

// Attribute names: up to 256 of A-Z a-z 0-9 _ - ., not starting with
// "AWS." or "Amazon.", no "..", not starting or ending with ".".
pub fn check_attribute_name(name: str) -> result[bool, str] {
    let b = to_bytes(name);
    if len(b) == 0 || len(b) > 256 {
        return err("attribute names are 1 to 256 characters");
    }
    let lower = strings.to_lower(name);
    if strings.has_prefix(lower, "aws.") || strings.has_prefix(lower, "amazon.") {
        return err("attribute names cannot start with AWS. or Amazon.");
    }
    if strings.has_prefix(name, ".") || strings.has_suffix(name, ".") || strings.contains(name, "..") {
        return err("attribute names cannot start or end with '.' or hold '..'");
    }
    for c in b {
        let v = c as int;
        let okc = (v >= 65 && v <= 90) || (v >= 97 && v <= 122) || (v >= 48 && v <= 57) ||
            v == 95 || v == 45 || v == 46;
        if !okc {
            return err("attribute names use letters, digits, '_', '-' and '.'");
        }
    }
    return ok(true);
}

// Every problem with one message, joined; "" when it can be sent.
pub fn check_outgoing(m: Outgoing) -> str {
    let ps: [str] = [];
    let br = check_body(m.body);
    if let _b = br {
    } else let e = err_of(br) {
        push(ps, e);
    }
    if m.delay_seconds < 0 || m.delay_seconds > 900 {
        push(ps, "delay_seconds must be 0 to 900");
    }
    if len(m.attributes) > 10 {
        push(ps, "a message has at most 10 attributes");
    }
    for k, _v in m.attributes {
        let ar = check_attribute_name(k);
        if let _a = ar {
        } else let e = err_of(ar) {
            push(ps, "attribute " + q(k) + ": " + e);
        }
    }
    if len(m.group_id) > 128 || len(m.dedup_id) > 128 {
        push(ps, "group_id and dedup_id are at most 128 characters");
    }
    return strings.join(ps, "; ");
}

fn entry_fields(b: builder.Str, m: Outgoing) {
    b.write("\"MessageBody\":" + q(m.body));
    if m.delay_seconds > 0 {
        b.write(",\"DelaySeconds\":" + to_str(m.delay_seconds));
    }
    if len(m.group_id) > 0 {
        b.write(",\"MessageGroupId\":" + q(m.group_id));
    }
    if len(m.dedup_id) > 0 {
        b.write(",\"MessageDeduplicationId\":" + q(m.dedup_id));
    }
    if len(m.attributes) > 0 {
        b.write(",\"MessageAttributes\":{");
        let first = true;
        for k, v in m.attributes {
            if !first {
                b.write(",");
            }
            first = false;
            b.write(q(k) + ":{\"DataType\":\"String\",\"StringValue\":" + q(v) + "}");
        }
        b.write("}");
    }
}

pub fn send_body(queue_url: str, m: Outgoing) -> str {
    let b = builder.new_str();
    b.write("{\"QueueUrl\":" + q(queue_url) + ",");
    entry_fields(b, m);
    b.write("}");
    return b.finish();
}

pub fn send_batch_body(queue_url: str, ms: [Outgoing]) -> str {
    let b = builder.new_str();
    b.write("{\"QueueUrl\":" + q(queue_url) + ",\"Entries\":[");
    for i in 0..len(ms) {
        if i > 0 {
            b.write(",");
        }
        b.write("{\"Id\":\"" + to_str(i) + "\",");
        entry_fields(b, ms[i]);
        b.write("}");
    }
    b.write("]}");
    return b.finish();
}

pub fn receive_body(queue_url: str, max: int, wait_seconds: int, visibility: int) -> str {
    let b = builder.new_str();
    b.write("{\"QueueUrl\":" + q(queue_url) + ",\"MaxNumberOfMessages\":" + to_str(max) +
        ",\"WaitTimeSeconds\":" + to_str(wait_seconds) +
        ",\"MessageSystemAttributeNames\":[\"All\"],\"MessageAttributeNames\":[\"All\"]");
    if visibility >= 0 {
        b.write(",\"VisibilityTimeout\":" + to_str(visibility));
    }
    b.write("}");
    return b.finish();
}

pub fn receipts_body(queue_url: str, receipts: [str]) -> str {
    let b = builder.new_str();
    b.write("{\"QueueUrl\":" + q(queue_url) + ",\"Entries\":[");
    for i in 0..len(receipts) {
        if i > 0 {
            b.write(",");
        }
        b.write("{\"Id\":\"" + to_str(i) + "\",\"ReceiptHandle\":" + q(receipts[i]) + "}");
    }
    b.write("]}");
    return b.finish();
}

// ---- responses ----------------------------------------------------------------------

struct WireAttr { DataType: str, StringValue: opt[str] }

gc struct WireMessage {
    MessageId: str,
    ReceiptHandle: str,
    MD5OfBody: str,
    Body: str,
    Attributes: opt[map[str]str],
    MessageAttributes: opt[map[str]WireAttr],
}

gc struct WireReceive { Messages: opt[[WireMessage]] }

gc struct WireSent { MessageId: str, MD5OfMessageBody: str, SequenceNumber: opt[str] }

gc struct WireOk { Id: str, MessageId: opt[str], MD5OfMessageBody: opt[str] }
gc struct WireFail { Id: str, Code: str, Message: opt[str], SenderFault: opt[bool] }
gc struct WireBatch { Successful: opt[[WireOk]], Failed: opt[[WireFail]] }

gc struct WireUrl { QueueUrl: str }
gc struct WireAttrs { Attributes: opt[map[str]str] }
gc struct WireError { __type: opt[str], message: opt[str], Message: opt[str] }

pub fn md5_hex(s: str) -> str { return encoding.hex_encode(crypto.md5(to_bytes(s))); }

// "sqs: QueueDoesNotExist: The specified queue does not exist." from an
// error response; the code is the part of __type after '#'.
pub fn error_text(status: int, body: str) -> str {
    let r: result[WireError, str] = json.decode(body);
    guard let e = r else { return "sqs: HTTP " + to_str(status); }
    let t = e.__type ?? "";
    let hash = strings.rfind(t, "#");
    if hash >= 0 {
        t = strings.slice(t, hash + 1, len(t));
    }
    let msg = e.message ?? (e.Message ?? "");
    if len(t) == 0 {
        return "sqs: HTTP " + to_str(status) + ": " + msg;
    }
    if len(msg) == 0 {
        return "sqs: " + t;
    }
    return "sqs: " + t + ": " + msg;
}

pub fn decode_sent(body: str, sent: str) -> result[Sent, str] {
    let r: result[WireSent, str] = json.decode(body);
    guard let s = r else let e = err_of(r) { return err("sqs: SendMessage response: " + e); }
    if s.MD5OfMessageBody != md5_hex(sent) {
        return err("sqs: the body SQS stored does not match the body sent (MD5 " + s.MD5OfMessageBody + ")");
    }
    return ok(Sent { id: s.MessageId, sequence: s.SequenceNumber ?? "" });
}

pub fn decode_batch(body: str, sent: [str]) -> result[[BatchResult], str] {
    let r: result[WireBatch, str] = json.decode(body);
    guard let b = r else let e = err_of(r) { return err("sqs: batch response: " + e); }
    let out: [BatchResult] = [];
    for i in 0..len(sent) {
        push(out, BatchResult { index: i, ok: false, id: "", error: "sqs: no result for this entry" });
    }
    for s in b.Successful ?? [] {
        let ir = to_int(s.Id);
        guard let i = ir else { return err("sqs: batch response names an unknown entry " + s.Id); }
        if i < 0 || i >= len(sent) {
            return err("sqs: batch response names an unknown entry " + s.Id);
        }
        let md5 = s.MD5OfMessageBody ?? "";
        if len(sent[i]) > 0 && md5 != md5_hex(sent[i]) {
            out[i] = BatchResult { index: i, ok: false, id: s.MessageId ?? "", error: "sqs: the body SQS stored does not match the body sent" };
            continue;
        }
        out[i] = BatchResult { index: i, ok: true, id: s.MessageId ?? "", error: "" };
    }
    for f in b.Failed ?? [] {
        let ir = to_int(f.Id);
        guard let i = ir else { return err("sqs: batch response names an unknown entry " + f.Id); }
        if i < 0 || i >= len(sent) {
            return err("sqs: batch response names an unknown entry " + f.Id);
        }
        let m = f.Message ?? "";
        let text = "sqs: " + f.Code;
        if len(m) > 0 {
            text = text + ": " + m;
        }
        out[i] = BatchResult { index: i, ok: false, id: "", error: text };
    }
    return ok(out);
}

// Received messages, each body checked against its MD5: a body changed
// between SQS and here is an error, not a message.
pub fn decode_receive(body: str) -> result[[Message], str] {
    let r: result[WireReceive, str] = json.decode(body);
    guard let w = r else let e = err_of(r) { return err("sqs: ReceiveMessage response: " + e); }
    let out: [Message] = [];
    for m in w.Messages ?? [] {
        if m.MD5OfBody != md5_hex(m.Body) {
            return err("sqs: message " + m.MessageId + " arrived with a body that does not match its MD5");
        }
        let attrs: map[str]str = {};
        let wa: map[str]WireAttr = m.MessageAttributes ?? {};
        for k, v in wa {
            if let sv = v.StringValue {
                attrs[k] = sv;
            }
        }
        let sys: map[str]str = m.Attributes ?? {};
        push(out, Message { id: m.MessageId, receipt: m.ReceiptHandle, body: m.Body, system: sys, attributes: attrs });
    }
    return ok(out);
}

pub fn decode_url(body: str) -> result[str, str] {
    let r: result[WireUrl, str] = json.decode(body);
    guard let u = r else let e = err_of(r) { return err("sqs: response: " + e); }
    return ok(u.QueueUrl);
}

pub fn decode_attributes(body: str) -> result[map[str]str, str] {
    let r: result[WireAttrs, str] = json.decode(body);
    guard let a = r else let e = err_of(r) { return err("sqs: GetQueueAttributes response: " + e); }
    let m: map[str]str = a.Attributes ?? {};
    return ok(m);
}
