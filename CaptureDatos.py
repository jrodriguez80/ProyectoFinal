import pika
import json
import random
from MessageQueue import MessageQueue

# Función para generar una cédula válida (9 cifras) o inválida (6 cifras)
def generar_cedula():
    if random.random() < 0.5:
        # Generar una cédula con 6 cifras
        return random.randint(100000, 999999)
    else:
        # Generar una cédula con 9 cifras
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

# Generar y enviar 10 formularios a la cola
for i in range(10):
    formulario = generar_formulario()
    cedula = str(formulario["cedula"])
    
    # Enviar el formulario en formato JSON a la cola de mensajes
    message = json.dumps(formulario)
    message_queue.publish_message('formulario_censo', message)
    print(f"Formulario {i+1} enviado a la cola:\n{message}\n")

# Cerrar la conexión a la cola de mensajes
message_queue.close_connection()
