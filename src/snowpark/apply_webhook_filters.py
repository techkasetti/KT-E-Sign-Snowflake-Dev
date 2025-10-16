def handler(session, provider, raw_payload):
    # example filter: ignore provider heartbeat events
    if raw_payload.get('type') == 'heartbeat':
        return {'ignored': True}
    return {'ignored': False}

