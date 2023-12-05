import json
import requests
from Storage import StorageNode

class ReportesModule:
    def __init__(self, storage_node, storage_api_url):
        self.storage_node = storage_node
        self.storage_api_url = storage_api_url

    def _obtener_formularios(self):
        try:
            response = requests.get(f"{self.storage_api_url}/get_all_forms")
            response.raise_for_status()

            # Imprime la respuesta del servidor
            print("Respuesta del servidor:", response.text)

            return response.json()
        except requests.RequestException as e:
            print(f"Error al obtener formularios del módulo de almacenamiento: {str(e)}")
            return []

    def _calcular_porcentaje(self, filtro):
        formularios = self._obtener_formularios()

        # Verificar si la respuesta es una cadena (sin formularios) o un diccionario (con formularios)
        if not formularios:
            print("No hay formularios disponibles.")
            return 0  # O cualquier otro valor predeterminado que desees

        # Si la respuesta es una cadena, conviértela a un diccionario
        if isinstance(formularios, str):
            formularios = json.loads(formularios)

        # Ahora, puedes trabajar con la lista de formularios
        filtrados = [formulario for formulario in formularios.get("forms", []) if filtro(formulario)]
        total_formularios = len(formularios.get("forms", []))
        return (len(filtrados) / total_formularios) * 100 if total_formularios > 0 else 0

    def obtener_porcentaje_mujeres(self):
        return self._calcular_porcentaje(lambda formulario: formulario.get("genero") == "Femenino")

    def obtener_porcentaje_hombres(self):
        return self._calcular_porcentaje(lambda formulario: formulario.get("genero") == "Masculino")

    def obtener_porcentaje_menores_edad(self):
        return self._calcular_porcentaje(lambda formulario: formulario.get("edad", 0) < 18)

if __name__ == "__main__":
    storage_api_url = "http://localhost:5000"
    storage_node = StorageNode(node_id=1, is_leader=True)
    storage_node.initialize_storage()

    # Inicializar el modulo de reportes con la instancia de StorageNode
    reportes_module = ReportesModule(storage_node, storage_api_url)

    # Obtener y mostrar los resultados de los reportes
    porcentaje_mujeres = reportes_module.obtener_porcentaje_mujeres()
    porcentaje_hombres = reportes_module.obtener_porcentaje_hombres()
    porcentaje_menores_edad = reportes_module.obtener_porcentaje_menores_edad()

    print(f"Porcentaje de mujeres: {porcentaje_mujeres}%")
    print(f"Porcentaje de hombres: {porcentaje_hombres}%")
    print(f"Porcentaje de menores de edad: {porcentaje_menores_edad}%")
