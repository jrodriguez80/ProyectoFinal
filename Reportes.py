import requests
import json

class ReportesModule:
    def __init__(self, storage_api_url):
        self.storage_api_url = storage_api_url

    def obtener_porcentaje_mujeres(self):
        # Obtener datos del modulo de almacenamiento
        formularios = self.obtener_formularios()
        
        # Filtrar formularios para obtener solo mujeres
        mujeres = [formulario for formulario in formularios if formulario.get("genero") == "Femenino"]
        
        # Calcular el porcentaje
        total_formularios = len(formularios)
        porcentaje_mujeres = (len(mujeres) / total_formularios) * 100 if total_formularios > 0 else 0

        return porcentaje_mujeres

    def obtener_porcentaje_hombres(self):
        # Obtener datos del modulo de almacenamiento
        formularios = self.obtener_formularios()
        
        # Filtrar formularios para obtener solo hombres
        hombres = [formulario for formulario in formularios if formulario.get("genero") == "Masculino"]
        
        # Calcular el porcentaje
        total_formularios = len(formularios)
        porcentaje_hombres = (len(hombres) / total_formularios) * 100 if total_formularios > 0 else 0

        return porcentaje_hombres

    def obtener_porcentaje_menores_edad(self):
        # Obtener datos del módulo de almacenamiento
        formularios = self.obtener_formularios()
        
        # Filtrar formularios para obtener solo menores de edad
        menores_edad = [formulario for formulario in formularios if formulario.get("edad", 0) < 18]
        
        # Calcular el porcentaje
        total_formularios = len(formularios)
        porcentaje_menores_edad = (len(menores_edad) / total_formularios) * 100 if total_formularios > 0 else 0

        return porcentaje_menores_edad

    def obtener_formularios(self):
        try:
            response = requests.get(f"{self.storage_api_url}/get_all_forms")
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error al obtener formularios del modulo de almacenamiento. Código de estado: {response.status_code}")
                return []
        except Exception as e:
            print(f"Error al obtener formularios del modulo de almacenamiento: {str(e)}")
            return []

if __name__ == "__main__":
    storage_api_url = "http://localhost:5000" 
    reportes_module = ReportesModule(storage_api_url)

    # Obtener y mostrar los resultados de los reportes
    porcentaje_mujeres = reportes_module.obtener_porcentaje_mujeres()
    porcentaje_hombres = reportes_module.obtener_porcentaje_hombres()
    porcentaje_menores_edad = reportes_module.obtener_porcentaje_menores_edad()

    print(f"Porcentaje de mujeres: {porcentaje_mujeres}%")
    print(f"Porcentaje de hombres: {porcentaje_hombres}%")
    print(f"Porcentaje de menores de edad: {porcentaje_menores_edad}%")
