from flask import Flask, jsonify, request
import requests
import yaml
import time
import logging

class Follower:
    def __init__(self, node_id, port, leader_address, node_mode):
        """Inicializa un seguidor con la información necesaria."""
        self.node_id = node_id
        self.port = port
        self.is_ready = False
        self.forms = {}
        self.leader_address = leader_address
        self.node_mode = node_mode

    def add_operation(self):
        try:
            operation = request.json
            form_data = operation.get("form_data")
            cedula = form_data.get("cedula")
            
            self.forms[cedula] = form_data
            logging.info(f"Formulario {cedula} recibido y almacenado en el {self.node_mode} {self.node_id}")

            return jsonify({"message": "Formulario recibido y almacenado correctamente"}), 200
        except Exception as e:
            logging.error(f"Error al procesar la operación: {str(e)}")
            return jsonify({"message": "Error al procesar la operación"}), 500


    def reconnect(self):
        """Maneja la operación de reconexión."""
        logging.info(f"Reconexión exitosa del {self.node_mode} {self.node_id} al líder.")
        self.is_ready = True
        return jsonify({"message": "Reconexión exitosa"}), 200

    def sync_state(self):
        """Maneja la operación de sincronización de estado."""
        try:
            state = request.json.get("state")
            logging.info(f"Sincronización de estado con el líder en el {self.node_mode} {self.node_id}: {state}")

            self.forms = state.get("forms", {})

            return jsonify({"message": "Estado sincronizado"}), 200
        except Exception as e:
            logging.error(f"Error al sincronizar el estado: {str(e)}")
            return jsonify({"message": "Error al sincronizar el estado"}), 500

    def check_leader_ready(self):
        """Espera a que el líder esté listo antes de aceptar operaciones."""
        while True:
            try:
                response = requests.get(f"{self.leader_address}/check_ready")
                if response.status_code == 200 and response.json().get('status') == 'ready':
                    break
            except requests.RequestException:
                pass
            time.sleep(1)

    def run(self):
        """Ejecuta la aplicación Flask del seguidor."""
        # Esperar a que el líder esté listo antes de aceptar operaciones
        self.check_leader_ready()

        app.run(port=self.port)

# Crear una instancia de la aplicación Flask
app = Flask(__name__)
# La instancia de Follower se inicializará después de cargar la configuración
follower = None  

# Rutas de la API REST
@app.route('/add', methods=['POST'])
def add_operation():
    return follower.add_operation()

@app.route('/reconnect', methods=['POST'])
def reconnect():
    return follower.reconnect()

@app.route('/sync_state', methods=['POST'])
def sync_state():
    return follower.sync_state()

@app.route('/check_ready', methods=['GET'])
def check_ready():
    return jsonify({"status": "ready"}), 200

@app.route('/get_status', methods=['GET'])
def get_status():
    return jsonify({"status": "OK"}), 200

if __name__ == "__main__":
    # Configurar el registro para mostrar información
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Cargar la configuración desde el archivo YAML
    config_file_path = '/home/kali/Desktop/Tarea2/config.yaml'
    try:
        with open(config_file_path, 'r') as config_file:
            config_data = yaml.safe_load(config_file)
        node_id = config_data.get('node_id')
        port = config_data.get('node_port')
        leader_address = config_data.get('leader_address')
        node_mode = config_data.get('node_mode')

        # Inicializar el seguidor
        follower = Follower(node_id, port, leader_address, node_mode)
        # Ejecutar la aplicación del seguidor
        follower.run()
    except Exception as e:
        # Manejar errores al cargar la configuración
        logging.error(f"Error al cargar la configuración ({config_file_path}): {str(e)}")
