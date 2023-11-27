import json
import random
import os
import threading
import concurrent.futures
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

def enviar_formulario(message_queue):
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

# Conexión a la cola de mensajes
message_queue = MessageQueue()
if message_queue.connect():
    message_queue.declare_queue('formulario_censo')

# Crear la carpeta "archivos" si no existe
if not os.path.exists("archivos"):
    os.makedirs("archivos")

# Crear y enviar formularios concurrentemente
with concurrent.futures.ThreadPoolExecutor() as executor:
    for _ in range(2):
        futures = [executor.submit(enviar_formulario, message_queue) for _ in range(3)]
        concurrent.futures.wait(futures)

# Cerrar la conexión a la cola de mensajes
message_queue.close_connection()
