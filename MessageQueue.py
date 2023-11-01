import socket
import threading


# Configuracion de la direction y del puerto para la communication
HOST = 'localhost'
PORT = 2500

# Crear un socket para escuchar conexiones entrantes

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
    server_socket.bind((HOST, PORT))
    server_socket.listen()

    print(f"Escuchando en {HOST}:{PORT}...")

# aceptar una connection entrqnte
conn, addr = server_socket.accept()
with conn:
        print(f"Conectado por {addr}")

        # Escuchar mensajes entrantes y reenviarlos a todos los clientes conectados
        while True:
            data = conn.recv(1024)
            if not data:
                break
            print(f"Mensaje recibido: {data.decode()}")
