from flask import Flask, request, jsonify
from threading import Lock, Thread
import logging
import requests
import os
import time

app = Flask(__name__)
lock = Lock()
data_folder = "data"
replicas = [1, 2, 3]  # Lista de IDs de las replicas
storage_node = None  # Variable global para el nodo de almacenamiento


class StorageNode:
    def __init__(self, node_id, is_leader=False):
        self.node_id = node_id
        self.is_leader = is_leader
        self.log = []
        self.data = {}
        self.followers = []  # Lista para almacenar instancias de nodos seguidores

    def inicializar_almacenamiento(self):
        # Iniciar carpetas si no existe
        if not os.path.exists(data_folder):
            os.makedirs(data_folder)

        # Inicializar el nodo de almacenamiento
        self.storage_node = StorageNode(node_id=1, is_leader=True) # ID y el estado lider

    @app.route('/guardar_formulario', methods=['POST'])
    def api_guardar_formulario():
        formulario = request.json
        cedula = str(formulario.get("cedula"))
        storage_node.write_operation({"type": "add", "id": cedula, "formulario": formulario})
        return jsonify({"message": "Formulario guardado exitosamente"}), 200

    def start_follower(self, port):
        # Metodo para iniciar un nuevo nodo seguidor
        follower = Follower(port)
        self.followers.append(follower)
        Thread(target=follower.run).start()

    def start_followers_dynamically(self, num_followers):
        # Método para iniciar nodos seguidores dinámicamente
        for port in range(5000, 5002 + num_followers):  # es posible ajustarlo
            self.start_follower(port)

    def write_operation(self, operation):
        with lock:
            # Operacion al registro de operaciones en disco y actualizar los datos
            self.log.append(operation)
            operation_type = operation.get("type")
            if operation_type == "add":
                self.data[str(operation.get("id"))] = operation.get("formulario")
            elif operation_type == "delete":
                del self.data[str(operation.get("id"))]

            # Logica para replicar la operacion a otros nodos (seguidores)
            self.replicate_operation(operation)

    def replicate_operation(self, operation):
        if self.is_leader:
            # Logica para replicar la operacion a nodos seguidores
            for replica_id in replicas:
                if replica_id != self.node_id:
                    # Logica para enviar la operacion a la replica con ID replica_id
                    self.send_operation_to_replica(replica_id, operation)

    def send_operation_to_replica(self, replica_id, operation):
        replica_address = f"http://localhost:{5000 + replica_id}/add"
        response = requests.post(replica_address, json=operation)
        logging.info(f"Réplica {replica_id}: {response.json().get('message', 'Error en la respuesta')}")

    def handle_failure(self):
        logging.warning("¡Fallo!")
        # Logica para manejar la caida del nodo lider
        if self.is_leader:
            logging.info("El nodo líder ha fallado. Iniciando proceso de elección de nuevo líder.")
            self.elect_new_leader()

    def handle_reconnection(self):
        logging.info("Reconexión de un nodo seguidor. Se realizará una 'puesta al día'.")
        # Lógica para sincronizar el estado con el líder
        self.sync_with_leader()

    def handle_new_follower(self, new_follower_id):
        logging.info(f"Nodo seguidor agregado: Nodo {new_follower_id}")
        # Logica para sincronizar el nuevo nodo seguidor con el lider
        self.sync_with_new_follower(new_follower_id)

    def elect_new_leader(self):
        # Logica para elegir un nuevo lider
        # el nodo con el ID más alto como líder
        new_leader_id = max(replicas)
        logging.info(f"Nuevo líder elegido: Nodo {new_leader_id}")
        # Iniciar el proceso de reconexion para los nodos seguidores
        self.reconnect_followers()
        # Actualizar el estado para reflejar que este nodo es el nuevo lider
        self.is_leader = True

    def reconnect_followers(self):
        # Logica para manejar la reconexion de los nodos seguidores
        for replica_id in replicas:
            if replica_id != self.node_id:
                follower_address = f"http://localhost:{5000 + replica_id}/reconnect"
                response = requests.post(follower_address)
                if response.status_code == 200:
                    logging.info(f"Réplica {replica_id}: Reconexión exitosa.")
                else:
                    logging.warning(f"Réplica {replica_id}: Fallo en la reconexión.")

    def sync_with_leader(self):
        # Logica para obtener el estado actual del lider y actualizar el estado local
        leader_address = f"http://localhost:{5000}/get_state"
        response = requests.get(leader_address)
        if response.status_code == 200:
            leader_state = response.json().get("state")
            self.sync_state_with_leader(leader_state)
            logging.info("Sincronización con el líder exitosa.")
        else:
            logging.warning("Fallo al obtener el estado del líder.")

    def sync_state_with_leader(self, leader_state):
        # Logica para sincronizar el estado local con el estado del lider
        self.data = leader_state.get("data", {})
        self.log = leader_state.get("log", [])

    def sync_with_new_follower(self, new_follower_id):
        # Logica para enviar el estado actual al nuevo nodo seguidor
        new_follower_address = f"http://localhost:{5000 + new_follower_id}/sync_state"
        response = requests.post(new_follower_address, json={"state": {"data": self.data, "log": self.log}})
        if response.status_code == 200:
            logging.info(f"Sincronización con el nuevo seguidor {new_follower_id} exitosa.")
        else:
            logging.warning(f"Fallo al sincronizar con el nuevo seguidor {new_follower_id}.")


    def replication_worker(self):
        while True:
            time.sleep(5)  # Simular el tiempo entre las operaciones de replicación
            self.replicate_operation()

    def check_leader_status(self):
        while True:
            time.sleep(5)
            if not self.is_leader:
                continue

            # Verificar si el lider estq activo
            if not self.is_leader_alive():
                logging.warning("El nodo líder ha fallado. Iniciando proceso de elección de nuevo líder.")
                self.elect_new_leader()

    def is_leader_alive(self):
        if not self.is_leader:
            # No se aplica si no eres el lider
            return True

        # Verificar la salud del lider haciendo una solicitud HTTP a una ruta especifica
        leader_health_check_url = f"http://localhost:{5000}/health_check"
        try:
            response = requests.get(leader_health_check_url, timeout=2)
            return response.status_code == 200
        except requests.RequestException:
            return False

class Follower:
    def __init__(self, port):
        self.port = port
        self.is_ready = False  # Indica si el seguidor está listo para confirmar operaciones

    def run(self):
        app = Flask(__name__)

        @app.route('/add', methods=['POST'])
        def add_operation():
            if not self.is_ready:
                return jsonify({"message": "Follower not ready"}), 400

            operation = request.json
            # Lógica para procesar la operación (puedes aplicarla a tus datos locales)
            print(f"Received operation: {operation}")
            # Lógica adicional según tus requisitos

            # Confirmar operación al líder
            return jsonify({"message": "Operation received and confirmed"}), 200

        @app.route('/reconnect', methods=['POST'])
        def reconnect():
            # Lógica para manejar la reconexión del líder
            print("Reconnected to the leader.")
            self.is_ready = True  # El seguidor está listo para confirmar operaciones
            return jsonify({"message": "Reconnection successful"}), 200

        @app.route('/sync_state', methods=['POST'])
        def sync_state():
            state = request.json.get("state")
            # Lógica para sincronizar el estado con el líder
            print(f"Synchronized state with leader: {state}")
            return jsonify({"message": "State synchronized"}), 200

        app.run(port=self.port)

if __name__ == "__main__":
    # Configuracion de logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Inicializar el sistema de almacenamiento
    if not app.config.get("INITIALIZED"):
        storage_node = StorageNode(node_id=1, is_leader=True)
        storage_node.start_followers_dynamically(3)
        app.config["INITIALIZED"] = True

    # Iniciar el hilo de replicacion
    replication_thread = Thread(target=storage_node.replication_worker)
    replication_thread.start()

    # Ejecucion de la aplicacion Flask
    app.run(port=5000)
