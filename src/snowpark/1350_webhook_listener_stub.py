# Simple webhook listener stub used as example middleware for provider webhooks; not executed in Snowflake.
from flask import Flask, request, jsonify
app = Flask(__name__)
@app.route('/webhook', methods=['POST'])
def webhook():
    payload = request.get_json()
    # In production, validate HMAC and call validate_and_route_webhook via middleware or Snowflake External Function
    return jsonify({'status':'received'}), 200

