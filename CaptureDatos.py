import socket
import json
import random
import time

# Configuracion de la direction y del puerto para la communication
HOST = 'localhost'
PORT = 2500

#Generar formularios en formatos json

def generate_form():
    form_data = {
        'field1': random.randint(1, 100),
        'field2': random.choice(['A', 'B', 'C']),
        'timestamp': time.time()
    }
    return json.dumps(form_data)

#Crear un socket pqrq connectarse al servidor de mensages
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
    client_socket.connect((HOST, PORT))

# Generar y enviar un formulario a la cola de mensajes
    form = generate_form()
    client_socket.sendall(form.encode())

    print(f"Formulario enviado a la cola de mensajes: {form}")