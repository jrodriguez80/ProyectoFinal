import socket
import threading


# Configuracion de la direction y del puerto para la communication
HOST = 'localhost'
PORT = 2500

# Función para manejar la comunicación con un cliente individual
def handle_client(client_socket):
    print(f"Conectado por {client_socket.getpeername()}")
    
    while True:
        data = client_socket.recv(1024)
        if not data:
            break
        print(f"Mensaje recibido: {data.decode()}")
    
    client_socket.close()

# Crear un socket para escuchar conexiones entrantes
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
    server_socket.bind((HOST, PORT))
    server_socket.listen()

    print(f"Escuchando en {HOST}:{PORT}...")

    while True:
        conn, addr = server_socket.accept()
        client_thread = threading.Thread(target=handle_client, args=(conn,))
        client_thread.start()