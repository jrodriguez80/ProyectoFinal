import json
import random
import os
import threading
from MessageQueue import MessageQueue

# Función para generar una cédula
def generar_cedula():
    if random.random() < 0.5:
        # Generar una cédula con 6 cifras
        return random.randint(100000, 999999)
    else:
        # Generar una cédula con 9 cifras
        return random.randint(100000000, 999999999)

# Función para generar un formulario del censo
def generar_formulario():
    formulario = {
        "cedula": generar_cedula(),
        "nombre": "Nombre",
        "apellido": "Apellido",
        "direccion": "Dirección",
        "telefono": "1234567890",
        "email": "correo@example.com",
        "edad": random.randint(18, 100),
        "genero": random.choice(["Masculino", "Femenino"]),
        "ocupacion": "Ocupación",
        "nacionalidad": "Nacionalidad"
    }
    return formulario

# Conexión a la cola de mensajes
message_queue = MessageQueue()
if message_queue.connect():
    message_queue.declare_queue('formulario_censo')

# Crear la carpeta "archivos" si no existe
if not os.path.exists("archivos"):
    os.makedirs("archivos")

# Función para enviar un formulario a la cola
def enviar_formulario():
    formulario = generar_formulario()
    cedula = str(formulario["cedula"])

    # Enviar el formulario en formato JSON a la cola de mensajes
    message = json.dumps(formulario)
    message_queue.publish_message('formulario_censo', message)
    print(f"Formulario enviado a la cola:\n{message}\n")

    # Guardar el formulario en la carpeta "archivos"
    file_path = os.path.join("archivos", f"Archivo_{cedula}.json")
    with open(file_path, "w") as file:
        file.write(json.dumps(formulario))

# Generar y enviar 3 formularios dos veces en hilos separados
threads = []
for _ in range(2):
    for _ in range(3):
        thread = threading.Thread(target=enviar_formulario)
        threads.append(thread)
        thread.start()

    # Esperar a que todos los hilos terminen antes de la siguiente iteración
    for thread in threads:
        thread.join()

# Cerrar la conexión a la cola de mensajes
message_queue.close_connection()
