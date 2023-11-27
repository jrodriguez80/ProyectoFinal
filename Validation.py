import pika
import json
from MessageQueue import MessageQueue
import os
import threading
import logging

class ValidationDeduplicationModule:
    def __init__(self, message_queue):
        self.message_queue = message_queue

    def validar_cedula(self, cedula):
        return len(cedula) == 9

    def procesar_formulario(self, formulario):
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

    def callback(self, ch, method, properties, body):
        formulario = json.loads(body)
        cedula = str(formulario["cedula"])

        if not self.verificar_duplicado(cedula):
            self.procesar_formulario(formulario)

    def verificar_duplicado(self, cedula):
        duplicado_path = os.path.join("duplicados", f"Archivo_{cedula}.json")
        return os.path.exists(duplicado_path)

    def consumir_mensajes(self):
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

        # Inicializar el modulo de validacion y deduplicacion
        validation_deduplication_module = ValidationDeduplicationModule(message_queue)

        # Iniciar el consumo de mensajes en un hilo separado
        thread = threading.Thread(target=validation_deduplication_module.consumir_mensajes)
        thread.start()
        thread.join()  # Esperar a que el hilo de consumo termine
