from flask import Flask, request, jsonify
from threading import Lock, Thread
import logging
import requests
import os
import time
import yaml
import json
import uuid

# Informacion de red de los nodos es parametrizable
def load_config(file_path='/home/kali/Desktop/Tarea2/config.yaml'):
    try:
        with open(file_path, 'r') as config_file:
            config_data = yaml.safe_load(config_file)
        return config_data
    except Exception as e:
        print(f"Error al cargar el archivo de configuración ({file_path}): {str(e)}")
        return None

# Carga la configuración
config = load_config()
print(config)
node_ip = config['node_ip']
node_port = config['node_port']
replica_ids = config['replicas']

app = Flask(__name__)
# Configuración del sistema de logs
logging.basicConfig(level=logging.DEBUG)
lock = Lock()
data_folder = "data"
replicas = [1, 2, 3]
node_id = str(uuid.uuid4())




class StorageNode:
    def __init__(self, node_id=node_id, node_ip=node_ip, node_port=node_port, is_leader=False):
        self.node_id = node_id
        self.is_leader = is_leader
        self.log = []  # Registro de operaciones
        self.data = {}  # Datos almacenados
        self.followers = []  # Nodos seguidores

    def initialize_storage(self):
        """Inicializar la carpeta de almacenamiento si no existe."""
        if not os.path.exists(data_folder):
            print(f"El directorio {data_folder} no existe. Creándolo...")
            os.makedirs(data_folder)
            print("Directorio creado exitosamente.")
        else:
            print(f"El directorio {data_folder} ya existe.")

    def start_follower(self, replica_id):
        # Metodo para iniciar un nuevo nodo seguidor
        follower = Follower(replica_id, port=5000 + replica_id)
        self.followers.append(follower)
        Thread(target=follower.run).start()

    def start_followers_dynamically(self):
        # Metodo para iniciar nodos seguidores dinámicamente
        for replica_id in replica_ids:
            if replica_id != self.node_id:
                self.start_follower(replica_id)

    def write_operation(self, operation):
        """Realizar operación de escritura en el registro y actualizar datos."""
        with lock:
            self.log.append(operation)
            operation_type = operation.get("type")
            if operation_type == "add":
                self.data[str(operation.get("id"))] = operation.get("form_data")
                self.save_to_file(operation.get("id"), operation.get("form_data"))
            elif operation_type == "delete":
                del self.data[str(operation.get("id"))]
            self.delete_from_file(operation.get("id"))
        self.replicate_operation(operation)

    def save_to_file(self, cedula, form_data):
        """Guardar datos en el sistema de archivos."""
        file_path = os.path.join(data_folder, f"{cedula}.json")
        try:
            with open(file_path, "w") as file:
                json.dump(form_data, file)
            logging.info(f"Datos guardados en el archivo: {file_path}")
        except Exception as e:
            logging.error(f"Error al guardar en el archivo {file_path}: {str(e)}")

    def delete_from_file(self, cedula):
        """Eliminar datos del sistema de archivos."""
        file_path = os.path.join(data_folder, f"{cedula}.json")
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logging.info(f"Archivo eliminado: {file_path}")
            else:
                logging.warning(f"Intento de eliminar archivo inexistente: {file_path}")
        except Exception as e:
            logging.error(f"Error al eliminar el archivo {file_path}: {str(e)}")


    def replicate_operation(self, operation):
        """Replicar la operación a nodos seguidores."""
        if self.is_leader:
            for replica_id in replica_ids:
                if replica_id != self.node_id:
                    self.send_operation_to_replica(replica_id, operation)

    def send_operation_to_replica(self, replica_id, operation):
        """Enviar la operación a un nodo seguidor específico."""
        replica_address = f"http://{node_ip}:{5000 + replica_id}/add"
        response = requests.post(replica_address, json=operation)
        logging.info(f"Replica {replica_id}: {response.json().get('message', 'Error in response')}")

    def handle_failure(self):
        logging.warning("¡Fallo!")
        # Lógica para manejar la caída del nodo líder
        if self.is_leader:
            logging.info("El nodo líder ha fallado. Iniciando proceso de elección de nuevo líder.")
            self.elect_new_leader()

    def handle_reconnection(self):
        logging.info("Reconexión de un nodo seguidor. Se realizará una 'puesta al día'.")
        # Lógica para sincronizar el estado con el líder
        self.sync_with_leader()

    def handle_new_follower(self, new_follower_id):
        logging.info(f"Nodo seguidor agregado: Nodo {new_follower_id}")
        # Lógica para sincronizar el nuevo nodo seguidor con el líder
        self.sync_with_new_follower(new_follower_id)

    def elect_new_leader(self):
        # Lógica para elegir un nuevo líder, el nodo con el ID más alto como líder
        new_leader_id = max(replicas)
        logging.info(f"Nuevo líder elegido: Nodo {new_leader_id}")
        # Iniciar el proceso de reconexión para los nodos seguidores
        self.reconnect_followers()
        # Actualizar el estado para reflejar que este nodo es el nuevo líder
        self.is_leader = True

    def reconnect_followers(self):
        # Lógica para manejar la reconexión de los nodos seguidores
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

    def replication_worker(self):
        while True:
            try:
                time.sleep(5)  # Simular el tiempo entre las operaciones de replicacion
                next_operation = self.get_next_operation()
                if next_operation:
                    self.replicate_operation(next_operation)
            except Exception as e:
                logging.error(f"Error durante la replicación: {str(e)}")

    def get_next_operation(self):
        with lock:
            if self.log:
                return self.log.pop(0)  # Obtiene y elimina la primera operación del registro
        return None

    def check_leader_status(self):
        while True:
            time.sleep(5)
            if not self.is_leader:
                continue

            # Verificar si el líder está activo
            if not self.is_leader_alive():
                logging.warning("El nodo líder ha fallado. Iniciando proceso de elección de nuevo líder.")
                self.elect_new_leader()

    def is_leader_alive(self):
        if not self.is_leader:
            # No se aplica si no eres el líder
            return True

        # Verificar la salud del líder haciendo una solicitud HTTP a una ruta específica
        leader_health_check_url = f"http://localhost:{5000}/health_check"
        try:
            response = requests.get(leader_health_check_url, timeout=2)
            return response.status_code == 200
        except requests.RequestException:
            return False
        
    def get_all_forms_data(self):
        """Obtener todos los formularios almacenados."""
        return list(self.data.values())
    
    def run_flask_app(self):
        app.run(port=node_port)


@app.route('/')
def hello():
    return f'Hello from {node_ip}:{node_port}'

@app.route('/guardar_formulario', methods=['POST'])
def guardar_formulario():
    try:
        formulario = request.get_json()
        cedula = formulario.get("cedula")
        storage_node.write_operation({"type": "add", "id": cedula, "form_data": formulario})
        
        logging.info("Formulario guardado correctamente.")
        
        return jsonify({"message": "Formulario guardado correctamente"}), 200
    except Exception as e:
        logging.error(f"Error al procesar el formulario: {str(e)}")
        return jsonify({"message": "Error al procesar el formulario"}), 500


@app.route('/get_all_forms', methods=['GET'])
def get_all_forms():
    storage_node = StorageNode()
    try:
        
        forms = storage_node.get_all_forms_data()
        return jsonify({"forms": forms}), 200
    except Exception as e:
        logging.error(f"Error al obtener formularios: {str(e)}")
        return jsonify({"message": "Error al obtener formularios"}), 500

 

@app.route('/delete_form/<cedula>', methods=['DELETE'])
def delete_form(cedula):
    """Eliminar un formulario del nodo de almacenamiento líder."""
    storage_node.write_operation({"type": "delete", "id": cedula})
    return jsonify({"message": "Form deleted successfully"}), 200

@app.route('/replace_form/<cedula>', methods=['PUT'])
def replace_form(cedula):
    """Reemplazar un formulario existente en el nodo de almacenamiento líder."""
    form_data = request.json
    storage_node.write_operation({"type": "replace", "id": cedula, "form_data": form_data})
    return jsonify({"message": "Form replaced successfully"}), 200
    

class Follower:
    def __init__(self, node_id, port):
        self.node_id = node_id
        self.port = port
        self.is_ready = False  # Indica si el seguidor está listo para confirmar operaciones
   

        @app.route('/add', methods=['POST'])
        def add_operation():
            if not self.is_ready:
                return jsonify({"message": "Follower not ready"}), 400

            operation = request.json
            # Lógica para procesar la operación (puedes aplicarla a tus datos locales)
            print(f"Received operation: {operation}")
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

    # Crear e inicializar la instancia de StorageNode
    storage_node = StorageNode(node_id=node_id, node_ip=node_ip, node_port=node_port, is_leader=True)
    storage_node.initialize_storage()

    # Iniciar el hilo de replicacion
    replication_thread = Thread(target=storage_node.replication_worker)
    replication_thread.start()

    # Iniciar dinámicamente nodos seguidores
    dynamic_followers_thread = Thread(target=storage_node.start_followers_dynamically)
    dynamic_followers_thread.start()

    # Iniciar la aplicación Flask con la instancia de StorageNode
    storage_node.run_flask_app()


