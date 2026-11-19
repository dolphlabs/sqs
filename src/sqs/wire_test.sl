import "json";
import "strings";

gc struct SendCheck { QueueUrl: str, MessageBody: str, DelaySeconds: int }

fn test_send_body() {
    let m = outgoing("hi \"there\"");
    assert(send_body("https://q/1/a", m) == "{\"QueueUrl\":\"https://q/1/a\",\"MessageBody\":\"hi \\\"there\\\"\"}",
           send_body("https://q/1/a", m));
    m.delay_seconds = 5;
    m.group_id = "g";
    m.dedup_id = "d";
    m.attributes["kind"] = "order";
    let s = send_body("u", m);
    assert(s == "{\"QueueUrl\":\"u\",\"MessageBody\":\"hi \\\"there\\\"\",\"DelaySeconds\":5,\"MessageGroupId\":\"g\"," +
           "\"MessageDeduplicationId\":\"d\",\"MessageAttributes\":{\"kind\":{\"DataType\":\"String\",\"StringValue\":\"order\"}}}", s);
    // Every body is valid JSON.
    let r: result[SendCheck, str] = json.decode(s);
    guard let _v = r else let e = err_of(r) { panic(e); }
}

fn test_batch_and_receipt_bodies() {
    let s = send_batch_body("u", [outgoing("a"), outgoing("b")]);
    assert(s == "{\"QueueUrl\":\"u\",\"Entries\":[{\"Id\":\"0\",\"MessageBody\":\"a\"},{\"Id\":\"1\",\"MessageBody\":\"b\"}]}", s);
    assert(receipts_body("u", ["r1"]) == "{\"QueueUrl\":\"u\",\"Entries\":[{\"Id\":\"0\",\"ReceiptHandle\":\"r1\"}]}");
    let rb = receive_body("u", 10, 20, -1);
    assert(strings.contains(rb, "\"MaxNumberOfMessages\":10,\"WaitTimeSeconds\":20,") && !strings.contains(rb, "Visibility"), rb);
    assert(strings.contains(receive_body("u", 1, 0, 30), "\"VisibilityTimeout\":30"));
}

fn test_checks() {
    assert(check_body("plain\ttext\r\n") ?? false);
    assert(check_body("日本語 ok") ?? false);
    assert(!(check_body("") ?? false));
    let bad = to_bytes("ab");
    bad[1] = 1;
    assert(!(check_body(to_str(bad)) ?? false));
    let fffe = to_bytes("x...");
    fffe[1] = 239;
    fffe[2] = 191;
    fffe[3] = 190;
    assert(!(check_body(to_str(fffe)) ?? false));
    for n in ["kind", "a.b", "a-b_c"] {
        assert(check_attribute_name(n) ?? false, n);
    }
    for n in ["", "AWS.x", "amazon.y", ".a", "a.", "a..b", "a b", strings.repeat("a", 257)] {
        assert(!(check_attribute_name(n) ?? false), n);
    }
    let m = outgoing("");
    m.delay_seconds = 901;
    m.attributes["AWS.x"] = "v";
    let p = check_outgoing(m);
    assert(strings.contains(p, "empty") && strings.contains(p, "delay_seconds") && strings.contains(p, "AWS."), p);
    assert(check_outgoing(outgoing("ok")) == "");
}

fn test_md5_is_sqs_digest() {
    // MD5 of "hello", as SQS reports it.
    assert(md5_hex("hello") == "5d41402abc4b2a76b9719d911017c592");
}

fn test_decode_sent_checks_the_digest() {
    let good = "{\"MessageId\":\"m1\",\"MD5OfMessageBody\":\"5d41402abc4b2a76b9719d911017c592\"}";
    let s = decode_sent(good, "hello") ?? Sent { id: "", sequence: "" };
    assert(s.id == "m1" && s.sequence == "");
    let r = decode_sent(good, "hellO");
    guard let _s = r else let e = err_of(r) { assert(strings.contains(e, "does not match"), e); return; }
    panic("a wrong digest was accepted");
}

fn test_decode_receive() {
    let body = "{\"Messages\":[{\"MessageId\":\"m1\",\"ReceiptHandle\":\"rh\",\"MD5OfBody\":\"5d41402abc4b2a76b9719d911017c592\"," +
        "\"Body\":\"hello\",\"Attributes\":{\"ApproximateReceiveCount\":\"3\"}," +
        "\"MessageAttributes\":{\"kind\":{\"DataType\":\"String\",\"StringValue\":\"order\"},\"bin\":{\"DataType\":\"Binary\",\"BinaryValue\":\"AA==\"}}}]}";
    let r = decode_receive(body);
    guard let ms = r else let e = err_of(r) { panic(e); }
    assert(len(ms) == 1 && ms[0].receipt == "rh" && ms[0].body == "hello");
    assert(ms[0].system["ApproximateReceiveCount"] == "3" && ms[0].attributes["kind"] == "order" && !has(ms[0].attributes, "bin"));
    let empty = decode_receive("{}");
    assert(len(empty ?? [ms[0]]) == 0);
    let tampered = decode_receive(strings.replace(body, "\"Body\":\"hello\"", "\"Body\":\"hellO\""));
    guard let _t = tampered else let e = err_of(tampered) { assert(strings.contains(e, "does not match"), e); return; }
    panic("a tampered body was accepted");
}

fn test_decode_batch_in_order() {
    let body = "{\"Successful\":[{\"Id\":\"1\",\"MessageId\":\"m1\",\"MD5OfMessageBody\":\"" + md5_hex("b") + "\"}]," +
        "\"Failed\":[{\"Id\":\"0\",\"Code\":\"InvalidParameterValue\",\"Message\":\"too big\",\"SenderFault\":true}]}";
    let r = decode_batch(body, ["a", "b", "c"]);
    guard let rs = r else let e = err_of(r) { panic(e); }
    assert(!rs[0].ok && rs[0].error == "sqs: InvalidParameterValue: too big", rs[0].error);
    assert(rs[1].ok && rs[1].id == "m1");
    assert(!rs[2].ok && strings.contains(rs[2].error, "no result"));
    let bad = decode_batch("{\"Successful\":[{\"Id\":\"7\"}]}", ["a"]);
    guard let _b = bad else { return; }
    panic("an unknown entry id was accepted");
}

fn test_error_text() {
    assert(error_text(400, "{\"__type\":\"com.amazonaws.sqs#QueueDoesNotExist\",\"message\":\"The specified queue does not exist.\"}") ==
           "sqs: QueueDoesNotExist: The specified queue does not exist.");
    assert(error_text(403, "{\"__type\":\"UnrecognizedClientException\",\"Message\":\"bad token\"}") ==
           "sqs: UnrecognizedClientException: bad token");
    assert(error_text(502, "<html>") == "sqs: HTTP 502");
}
