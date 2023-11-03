import pika
import json
import random
from MessageQueue import MessageQueue

# Funcion para generar un numero de cedulq unico
def generar_cedula_unico():
    return random.randint(100000000, 999999999)

# Funcion para generar un formulario del censo
def generar_formulario():
    formulario = {
        "cedula": generar_cedula_unico(),
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

# Generar y enviar formularios a la cola
for _ in range(10):  # Generar 10 formularios
    formulario = generar_formulario()
    cedula = str(formulario["cedula"])

    # Validación de cédula (9 cifras)
    if len(cedula) != 9:
        print(f"Formulario con cédula inválida: {cedula}")
    else:
        # Enviar el formulario en formato JSON a la cola de mensajes
        message = json.dumps(formulario)
        message_queue.publish_message('formulario_censo', message)
        print(f"Formulario con cédula válida enviado a la cola: {cedula}")

# Cerrar la conexion a la cola de mensajes
message_queue.close_connection()


