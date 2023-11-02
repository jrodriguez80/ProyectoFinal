import socket
import json

# Configuracion de la direction y del puerto para la communication
HOST = 'localhost'
PORT = 9999

# Crear un socket para conectarse al servidor de mensajes
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
    client_socket.connect((HOST, PORT))

    # Escuchar mensajes de la cola de mensajes y realizar validación
    while True:
        data = client_socket.recv(1024)
        if not data:
            break
        print(f"Formulario recibido y validado: {data.decode()}")