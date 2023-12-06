from flask import Flask, request, jsonify
from threading import Lock, Thread
import logging
import requests
import os
import time
import yaml
import json
import uuid
from follower import Follower

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
node_mode = config.get('node_mode', 'follower')  # Por defecto, actuará como seguidor
app = Flask(__name__)
# Configuración del sistema de logs
logging.basicConfig(level=logging.DEBUG)
lock = Lock()
data_folder = "/home/kali/Desktop/Tarea2/data"
replicas = [1, 2, 3]
node_id = str(uuid.uuid4())

class StorageNode:
    def __init__(self, node_id=node_id, node_ip=node_ip, node_port=node_port):
        self.node_id = node_id
        self.is_leader = node_mode == 'leader'
        self.log = []  # Registro de operaciones
        self.data = {}  # Datos almacenados
        self.followers = []  # Nodos seguidores
        self.node_ip = node_ip
        self.node_port = node_port

    def initialize_storage(self):
        """Inicializar la carpeta de almacenamiento si no existe."""
        if not os.path.exists(data_folder):
            print(f"El directorio {data_folder} no existe. Creándolo...")
            os.makedirs(data_folder)
            print("Directorio creado exitosamente.")
        else:
            print(f"El directorio {data_folder} ya existe.")

    def start_follower(self, replica_id):
        # Método para iniciar un nuevo nodo seguidor
        follower = Follower(replica_id, port=5000 + replica_id)
        self.followers.append(follower)
        Thread(target=follower.run).start()

    def start_followers_dynamically(self):
        # Método para iniciar nodos seguidores dinámicamente
        for replica_id in replica_ids:
            if replica_id != self.node_id:
                self.start_follower(replica_id)

    def write_operation(self, operation):
        """Realizar operación de escritura en el registro y actualizar datos."""
        with lock:
            print(f"Nodo {self.node_id} - Nueva operación: {operation}")
            self.log.append(operation)
            operation_type = operation.get("type")
            if operation_type == "add":
                # Almacena el formulario en el nodo líder
                self.data[str(operation.get("id"))] = operation.get("form_data")
                self.save_to_file(operation.get("id"), operation.get("form_data"))
                # Replicar la operación a los nodos seguidores
                self.replicate_operation(operation)
            elif operation_type == "delete":
                del self.data[str(operation.get("id"))]
                # Replicar la operación de eliminación a los nodos seguidores
                self.replicate_operation(operation)


    def replication_worker(self):
        while True:
            try:
                time.sleep(5)  # Simular el tiempo entre las operaciones de replicación
                next_operation = self.get_next_operation()
                if next_operation:
                    print(f"Nodo {self.node_id} - Iniciando replicación de operación: {next_operation}")
                    # Esperar a que los seguidores estén listos antes de replicar
                    for replica_id in replica_ids:
                        if replica_id != self.node_id:
                            self.wait_for_follower_ready(replica_id)
                            print(f"Nodo {self.node_id} - Replicando operación a réplica {replica_id}: {next_operation}")
                            self.replicate_operation(next_operation, replica_id)
            except Exception as e:
                logging.error(f"Error durante la replicación: {str(e)}")




    def get_next_operation(self):
        with lock:
            if self.log:
                return self.log.pop(0)  # Obtiene y elimina la primera operación del registro
        return None

    def save_to_file(self, cedula, form_data):
        """Guardar datos en el sistema de archivos."""
        file_path = os.path.join(data_folder, f"{cedula}.json")
        try:
            with open(file_path, "w") as file:
                json.dump(form_data, file)
            logging.info(f"Datos guardados en el archivo: {file_path}")
        except Exception as e:
            logging.error(f"Error al guardar en el archivo {file_path}: {str(e)}")
            print(f"Error al guardar en el archivo {file_path}: {str(e)}")

    def get_all_forms_data(self):
        """Obtener todos los formularios almacenados en el nodo."""
        return list(self.data.values())

    def replicate_operation(self, operation):
        """Replicar la operación a nodos seguidores."""
        if self.is_leader:
            replica_ids = [...]  # lista correcta de IDs de réplica

            for replica_id in replica_ids:
                if replica_id != self.node_id:
                    self.wait_for_follower_ready(replica_id)
                    print(f"Nodo {self.node_id} - Replicando operación a réplica {replica_id}: {operation}")
                    self.send_operation_to_replica(replica_id, operation)
                    print(f"Nodo {self.node_id} - Replicación completada a réplica {replica_id}")



    def send_operation_to_replica(self, replica_id, operation):
        replica_address = f"http://{node_ip}:{5000 + replica_id}/add"
        print(f"Nodo {self.node_id} - Enviando operación a réplica {replica_id}: {operation}")
        response = requests.post(replica_address, json=operation).json()
        print(f"Nodo {self.node_id} - Respuesta de réplica {replica_id}: {response}")
        if response.get('success', False):
            logging.info(f"Replica {replica_id}: Operación replicada correctamente.")
        else:
            logging.warning(f"Replica {replica_id}: Fallo en la replicación. {response.get('message', 'No message')}")



    def handle_failure(self):
        logging.warning("¡Fallo!")
        # Lógica para manejar la caída del nodo líder
        if self.is_leader:
            logging.info("El nodo líder ha fallado. Iniciando proceso de elección de nuevo líder.")
            self.elect_new_leader()

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

    def wait_for_follower_ready(self, replica_id):
        follower_address = f"http://127.0.0.1:{5000 + replica_id}/check_ready"
        while True:
            try:
                response = requests.get(follower_address)
                if response.status_code == 200 and response.json().get('ready', False):
                    break
            except requests.RequestException:
                pass
            time.sleep(1)

    def run_flask_app(self):
        app.run(port=self.node_port)

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

@app.route('/check_ready', methods=['GET'])
def check_ready():
    return jsonify({"status": "ready"}), 200

@app.route('/get_replica_status', methods=['GET'])
def get_replica_status():
    replica_statuses = {}
    for replica_id in replica_ids:
        if replica_id != self.node_id:
            replica_address = f"http://localhost:{5000 + replica_id}/get_status"
            try:
                response = requests.get(replica_address)
                if response.status_code == 200:
                    replica_status = response.json()
                    replica_statuses[f"Replica {replica_id}"] = replica_status
                else:
                    replica_statuses[f"Replica {replica_id}"] = {"status": "Error", "message": f"HTTP {response.status_code}"}
            except requests.RequestException as e:
                replica_statuses[f"Replica {replica_id}"] = {"status": "Error", "message": str(e)}
    return jsonify(replica_statuses), 200


if __name__ == "__main__":
    # Configuración de logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Crear e inicializar la instancia de StorageNode
    storage_node = StorageNode(node_id=node_id, node_ip=node_ip, node_port=node_port)
    storage_node.initialize_storage()

    # Iniciar el hilo de replicación
    replication_thread = Thread(target=storage_node.replication_worker)
    replication_thread.start()

    # Iniciar dinámicamente nodos seguidores
    dynamic_followers_thread = Thread(target=storage_node.start_followers_dynamically)
    dynamic_followers_thread.start()

    # Iniciar la aplicación Flask con la instancia de StorageNode
    storage_node.run_flask_app()
