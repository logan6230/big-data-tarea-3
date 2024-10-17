import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

class GreenhouseDataProducer:
    """
    Clase para generar y enviar datos de sensores de invernaderos a un tema de Kafka.

    Atributos:
    ----------
    producer : KafkaProducer
        Productor de Kafka configurado para enviar datos al tema 'greenhouse_data'.
    greenhouse_configs : dict
        Diccionario que contiene configuraciones de invernaderos, incluyendo los rangos de temperatura y humedad para cada tipo de cultivo.

    Métodos:
    --------
    generate_sensor_data(greenhouse_id):
        Genera datos simulados de temperatura y humedad para los sensores de un invernadero específico.
    run():
        Inicia el ciclo de generación y envío de datos de los invernaderos a Kafka en intervalos regulares.
    """

    def __init__(self, bootstrap_servers=['localhost:9092']):
        """
        Inicializa el productor de Kafka y configura los invernaderos.

        Parámetros:
        -----------
        bootstrap_servers : list
            Lista de servidores de Kafka a los que se conecta el productor.
        """
        # Inicializar productor de Kafka
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        # Configuraciones de los invernaderos con sus rangos de temperatura y humedad
        self.greenhouse_configs = {
            1: {"name": "Tomates", "temp_range": (20, 25), "humidity_range": (60, 75)},
            2: {"name": "Lechugas", "temp_range": (15, 20), "humidity_range": (70, 80)},
            3: {"name": "Pimientos", "temp_range": (22, 28), "humidity_range": (65, 75)}
        }

    def generate_sensor_data(self, greenhouse_id):
        """
        Genera datos simulados para los sensores de un invernadero específico, basado en su rango de temperatura y humedad.

        Parámetros:
        -----------
        greenhouse_id : int
            ID del invernadero del que se van a generar los datos.

        Retorna:
        --------
        dict:
            Diccionario que contiene los datos generados para los sensores, con la temperatura y humedad para cada sensor.
        """
        config = self.greenhouse_configs[greenhouse_id]  # Obtener la configuración del invernadero
        temp_range = config["temp_range"]  # Rango de temperatura para el cultivo
        humidity_range = config["humidity_range"]  # Rango de humedad para el cultivo

        # Generar datos base aleatorios dentro de los rangos de temperatura y humedad
        base_temp = random.uniform(*temp_range)
        base_humidity = random.uniform(*humidity_range)

        # Crear datos de sensores simulados
        sensors = {
            f"sensor_{i}": {
                "temperature": round(base_temp + random.uniform(-0.5, 0.5), 2),
                "humidity": round(base_humidity + random.uniform(-2, 2), 2)
            } for i in range(1, 4)  # Tres sensores por invernadero
        }

        # Retornar los datos generados en un formato estructurado
        return {
            "greenhouse_id": greenhouse_id,
            "greenhouse_name": config["name"],
            "timestamp": datetime.now().isoformat(),
            "sensors": sensors
        }

    def run(self):
        """
        Inicia el proceso de generación de datos y los envía continuamente al tema 'greenhouse_data' de Kafka.
        Los datos se generan para todos los invernaderos configurados cada 5 segundos.

        La ejecución se puede detener con un teclado interrumpido (Ctrl+C).
        """
        print("Iniciando productor de datos de invernadero...")
        try:
            # Bucle infinito para generar y enviar datos en intervalos
            while True:
                for greenhouse_id in self.greenhouse_configs.keys():
                    data = self.generate_sensor_data(greenhouse_id)  # Generar los datos del invernadero
                    self.producer.send('greenhouse_data', value=data)  # Enviar datos a Kafka
                    print(f"Enviado: {json.dumps(data, indent=2)}")  # Imprimir datos enviados
                time.sleep(1)  # Esperar 5 segundos antes de la próxima iteración
        except KeyboardInterrupt:
            # Manejo de la interrupción por teclado para cerrar el productor de manera segura
            print("\nDeteniendo el productor...")
            self.producer.close()  # Cerrar la conexión del productor con Kafka

if __name__ == "__main__":
    producer = GreenhouseDataProducer()  # Crear una instancia del productor
    producer.run()  # Iniciar la generación y envío de datos
