import json
import requests
from MessageQueue import MessageQueue
import os
import threading
import logging

class ValidationDeduplicationModule:
    def __init__(self, message_queue, storage_api_url):
        # Inicialización del módulo de validación y deduplicación con la cola de mensajes y la URL de la API de almacenamiento
        self.message_queue = message_queue
        self.storage_api_url = storage_api_url

    def validar_cedula(self, cedula):
        # Función para validar la longitud de la cédula (9 dígitos)
        return len(cedula) == 9

    def procesar_formulario(self, formulario):
        # Procesa el formulario, lo guarda en archivos y registra en logs
        cedula = str(formulario["cedula"])

        if self.validar_cedula(cedula):
            logging.info("Formulario válido: %s", formulario)
            file_path = os.path.join("validos", f"Archivo_{cedula}.json")
        else:
            logging.warning("Formulario inválido: %s", formulario)
            file_path = os.path.join("novalidos", f"Archivo_{cedula}.json")

        with open(file_path, "w") as file:
            file.write(json.dumps(formulario))
            logging.info("Formulario guardado en el archivo: %s", file_path)

    def enviar_a_almacenamiento(self, formulario):
        # Envia el formulario al modulo de almacenamiento a través de la api rest
        try:
            response = requests.post(f"{self.storage_api_url}/guardar_formulario", json=formulario)
            if response.status_code == 200:
                print("Formulario enviado exitosamente al módulo de almacenamiento.")
            else:
                print(f"Error al enviar formulario al módulo de almacenamiento. Código de estado: {response.status_code}")
        except Exception as e:
            print(f"Error al enviar formulario al módulo de almacenamiento: {str(e)}")

    def callback(self, ch, method, properties, body):
        # Función de callback para procesar mensajes recibidos de la cola de mensajes
        formulario = json.loads(body)
        cedula = str(formulario["cedula"])

        if not self.verificar_duplicado(cedula):
            self.procesar_formulario(formulario)
            self.enviar_a_almacenamiento(formulario)

    def verificar_duplicado(self, cedula):
        # Verifica si hay duplicados según la cédula
        duplicado_path = os.path.join("duplicados", f"Archivo_{cedula}.json")
        return os.path.exists(duplicado_path)

    def consumir_mensajes(self):
        # Inicia el consumo de mensajes desde la cola de mensajes
        self.message_queue.start_consuming('formulario_censo', self.callback)

if __name__ == "__main__":
    # Configuración de logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Inicializar la cola de mensajes
    message_queue = MessageQueue()
    if message_queue.connect():
        message_queue.declare_queue('formulario_censo')

        # Crear las carpetas "validos", "duplicados" y "novalidos" si no existen
        for folder in ["validos", "duplicados", "novalidos"]:
            if not os.path.exists(folder):
                os.makedirs(folder)

        # url de la api rest del modulo de qlmqcenamiento
        storage_api_url = "http://localhost:5000"

        # Inicializar el modulo de validacion y deduplicacion
        validation_deduplication_module = ValidationDeduplicationModule(message_queue, storage_api_url)

        # Iniciar el consumo de mensajes en un hilo separado
        thread = threading.Thread(target=validation_deduplication_module.consumir_mensajes)
        thread.start()
        thread.join()  # Esperar a que el hilo de consumo termine

