import pika
import json
from MessageQueue import MessageQueue

# Función de validación de cédula
def validar_cedula(cedula):
    return len(cedula) == 9  # Validar que la cédula tenga 9 cifras

# Conexión a la cola de mensajes
message_queue = MessageQueue()
if message_queue.connect():
    message_queue.declare_queue('formulario_censo')

# Función para procesar los mensajes de la cola
def callback(ch, method, properties, body):
    formulario = json.loads(body)
    cedula = str(formulario["cedula"])

    if validar_cedula(cedula):
        print(f"Formulario válido recibido: {cedula}")
    else:
        print(f"Formulario con cédula inválida recibido: {cedula}")

# Consumir mensajes de la cola
message_queue.start_consuming('formulario_censo', callback)
