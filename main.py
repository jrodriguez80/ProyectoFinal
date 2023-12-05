from threading import Thread
from follower import Follower
from flask import Flask, jsonify, request

app = Flask(__name__)

if __name__ == "__main__":
    # Crear una instancia de Follower
    follower = Follower(node_id=1, port=5001)

    # Configurar las rutas en Flask para la instancia de Follower
    @app.route('/add', methods=['POST'])
    def add_operation():
        return follower.add_operation()

    @app.route('/reconnect', methods=['POST'])
    def reconnect():
        return follower.reconnect()

    @app.route('/sync_state', methods=['POST'])
    def sync_state():
        return follower.sync_state()


    # Iniciar la aplicación Flask en un hilo separado
    flask_thread = Thread(target=app.run, kwargs={"port": 5001})
    flask_thread.start()

