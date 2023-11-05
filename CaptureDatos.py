import pika
import json
import random
import os
from MessageQueue import MessageQueue

# Funcion para generar una cedula 
def generar_cedula():
    if random.random() < 0.5:
        # Generar una cedula con 6 cifras
        return random.randint(100000, 999999)
    else:
        # Generar una cedula con 9 cifras
        return random.randint(100000000, 999999999)

# Funcion para generar un formulario del censo
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

# Conexion a la cola de mensajes
message_queue = MessageQueue()
if message_queue.connect():
    message_queue.declare_queue('formulario_censo')

# Crear la carpeta "archivos" si no existe para aue cualquier persona puedq correrlo
if not os.path.exists("archivos"):
    os.makedirs("archivos")

# Generar y enviar 10 formularios a la cola
for i in range(10):
    formulario = generar_formulario()
    cedula = str(formulario["cedula"])
    
    # Enviar el formulario en formato JSON a la cola de mensajes
    message = json.dumps(formulario)
    message_queue.publish_message('formulario_censo', message)
    print(f"Formulario {i+1} enviado a la cola:\n{message}\n")

    # Guardar el formulario en la carpeta "archivos"
    file_path = os.path.join("archivos", f"Archivo{i + 1}.json")
    with open(file_path, "w") as file:
        file.write(message)

# Cerrar la conexion a la cola de mensajes
message_queue.close_connection()
