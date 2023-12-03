# Storage.py

from flask import Flask, request, jsonify
from threading import Lock, Thread
import logging
import requests
import os
import time

app = Flask(__name__)
lock = Lock()
data_folder = "data"
replicas = [1, 2, 3]  # Lista de IDs de las réplicas
storage_node = None  # Variable global para el nodo de almacenamiento

class StorageNode:
    def __init__(self, node_id, is_leader=False):
        self.node_id = node_id
        self.is_leader = is_leader
        self.log = []
        self.data = {}

    def write_operation(self, operation):
        with lock:
            # Añadir la operación al registro de operaciones en disco y actualizar los datos
            self.log.append(operation)
            operation_type = operation.get("type")
            if operation_type == "add":
                self.data[str(operation.get("id"))] = operation.get("formulario")
            elif operation_type == "delete":
                del self.data[str(operation.get("id"))]

            # Lógica para replicar la operacion a otros nodos (seguidores)
            self.replicate_operation(operation)

    def replicate_operation(self, operation):
        if self.is_leader:
            # Lógica para replicar la operación a nodos seguidores
            for replica_id in replicas:
                if replica_id != self.node_id:
                    # Lógica para enviar la operación a la réplica con ID replica_id
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
        logging.info("Reconexion de un nodo seguidor. Se realizarq una 'puesta al dia'.")
        # Logica para sincronizar el estado con el lider
        self.sync_with_leader()

    def handle_new_follower(self, new_follower_id):
        logging.info(f"Nodo seguidor agregado: Nodo {new_follower_id}")
        # Logica para sincronizar el nuevo nodo seguidor con el lider
        self.sync_with_new_follower(new_follower_id)

    def elect_new_leader(self):
        # Logica para elegir un nuevo lider
        # En este ejemplo, simplemente seleccionamos el nodo con el ID mqs alto como lider
        new_leader_id = max(replicas)
        logging.info(f"Nuevo líder elegido: Nodo {new_leader_id}")
        # Iniciar el proceso de reconexión para los nodos seguidores
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
        # Lógica para obtener el estado actual del líder y actualizar el estado local
        leader_address = f"http://localhost:{5000}/get_state"
        response = requests.get(leader_address)
        if response.status_code == 200:
            leader_state = response.json().get("state")
            self.sync_state_with_leader(leader_state)
            logging.info("Sincronización con el líder exitosa.")
        else:
            logging.warning("Fallo al obtener el estado del líder.")

    def sync_state_with_leader(self, leader_state):
        # Lógica para sincronizar el estado local con el estado del líder
        self.data = leader_state.get("data", {})
        self.log = leader_state.get("log", [])

    def sync_with_new_follower(self, new_follower_id):
        # Lógica para enviar el estado actual al nuevo nodo seguidor
        new_follower_address = f"http://localhost:{5000 + new_follower_id}/sync_state"
        response = requests.post(new_follower_address, json={"state": {"data": self.data, "log": self.log}})
        if response.status_code == 200:
            logging.info(f"Sincronización con el nuevo seguidor {new_follower_id} exitosa.")
        else:
            logging.warning(f"Fallo al sincronizar con el nuevo seguidor {new_follower_id}.")


def replication_worker(storage):
    while True:
        time.sleep(5)  # Simular el tiempo entre las operaciones de replicación
        storage.replicate()


def inicializar_almacenamiento():
    global storage_node
    # Iniciar carpetas si no existe
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    # Inicializar el nodo de almacenamiento
    storage_node = StorageNode(node_id=1, is_leader=True)  # ID y el estado líder 


@app.route('/guardar_formulario', methods=['POST'])
def api_guardar_formulario():
    formulario = request.json
    cedula = str(formulario.get("cedula"))
    storage_node.write_operation({"type": "add", "id": cedula, "formulario": formulario})

    return jsonify({"message": "Formulario guardado exitosamente"}), 200


if __name__ == "__main__":
    # Configuración de logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Inicializar el sistema de almacenamiento
    if not app.config.get("INITIALIZED"):
        inicializar_almacenamiento()
        app.config["INITIALIZED"] = True

    # Iniciar el hilo de replicación
    replication_thread = Thread(target=replication_worker, args=(storage_node,))
    replication_thread.start()

    # Ejecutar la aplicación Flask
    app.run(port=5000)
