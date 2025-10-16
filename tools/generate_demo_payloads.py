#!/usr/bin/env python3
# Simple script to generate demo JSON events for Snowpipe ingestion into S3 staged paths (writes local JSONL)
import json, uuid, datetime, os

OUT_DIR = "demo_payloads"
os.makedirs(OUT_DIR, exist_ok=True)

def make_signature_event(request_id, signer_id, event_type):
    return {
        "event_id": f"evt_{uuid.uuid4().hex}",
        "request_id": request_id,
        "signer_id": signer_id,
        "event_type": event_type,
        "device_info": {"device":"demo-browser","fingerprint":"fp_demo_123"},
        "ip": "1.2.3.4",
        "ua": "demo-agent/1.0",
        "ts": datetime.datetime.utcnow().isoformat()
    }

def write_events(filename, events):
    path = os.path.join(OUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as fh:
        for e in events:
            fh.write(json.dumps(e) + "\n")
    print("Wrote", path)

if __name__ == "__main__":
    req_id = "req_demo_1"
    signer = "s_demo_1"
    evs = [make_signature_event(req_id, signer, t) for t in ("VIEWED","SIGNED")]
    write_events("signature_events_1.jsonl", evs)

