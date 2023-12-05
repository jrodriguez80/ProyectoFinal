from flask import Flask, jsonify, request

class Follower:
    def __init__(self, node_id, port):
        self.node_id = node_id
        self.port = port
        self.is_ready = False

    def add_operation(self):
        if not self.is_ready:
            return jsonify({"message": "Follower not ready"}), 400

        operation = request.json
        print(f"Received operation: {operation}")
        return jsonify({"message": "Operation received and confirmed"}), 200

    def reconnect(self):
        print("Reconnected to the leader.")
        self.is_ready = True
        return jsonify({"message": "Reconnection successful"}), 200

    def sync_state(self):
        state = request.json.get("state")
        print(f"Synchronized state with leader: {state}")
        return jsonify({"message": "State synchronized"}), 200

    def run(self):
        app.run(port=self.port)
