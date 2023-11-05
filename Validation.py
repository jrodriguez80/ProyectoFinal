import pika
import json
from MessageQueue import MessageQueue
import os

# Funcion para validar una cedula (debe tener 9 cifras)
def validar_cedula(cedula):
    return len(cedula) == 9

# Conexion a la cola de mensajes
message_queue = MessageQueue()
if message_queue.connect():
    message_queue.declare_queue('formulario_censo')

# Crear carpetas "validos" y "novalidos" 
if not os.path.exists("validos"):
    os.makedirs("validos")
if not os.path.exists("novalidos"):
    os.makedirs("novalidos")

# Funcion para procesar los mensajes de la cola
def callback(ch, method, properties, body):
    formulario = json.loads(body)

    cedula = str(formulario["cedula"])

    if validar_cedula(cedula):
        print("Formulario válido:")
        for campo, valor in formulario.items():
            print(f"{campo}: {valor}")

        # Guardar el formulario en la carpeta "validos"
        file_path = os.path.join("validos", f"Archivo_{cedula}.json")
        with open(file_path, "w") as file:
            file.write(json.dumps(formulario))

    else:
        print("Formulario inválido:")
        for campo, valor in formulario.items():
            print(f"{campo}: {valor}")

        # Guardar el formulario en la carpeta "novalidos"
        file_path = os.path.join("novalidos", f"Archivo_{cedula}.json")
        with open(file_path, "w") as file:
            file.write(json.dumps(formulario))

# Consumir mensajes de la cola
message_queue.start_consuming('formulario_censo', callback)
