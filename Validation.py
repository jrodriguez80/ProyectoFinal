import pika
import json
from MessageQueue import MessageQueue

# Función de validación de cédula
def validar_cedula(cedula):
    return len(cedula) == 9  # Validar que la cédula tenga 9 cifras

# Función para procesar los mensajes de la cola
def callback(ch, method, properties, body):
    formulario = json.loads(body)

    # Imprimir todos los campos del formulario
    print("Formulario recibido:")
    for campo, valor in formulario.items():
        print(f"{campo}: {valor}")

    cedula = str(formulario["cedula"])

    if validar_cedula(cedula):
        print(f"Cédula válida: {cedula}")
    else:
        print(f"Cédula inválida: {cedula}")

# Conexión a la cola de mensajes
message_queue = MessageQueue()
if message_queue.connect():
    message_queue.declare_queue('formulario_censo')

# Consumir mensajes de la cola
message_queue.start_consuming('formulario_censo', callback)

