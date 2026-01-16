import boto3
import json
import time
from loguru import logger
import datetime
import pandas as pd
import random

# CONFIGURACIÓN
STREAM_NAME = 'fuel-consumption-stream'
REGION = 'us-east-1' # Cambia si usas otra región
INPUT_FILE = 'FuelConsumption.csv'

kinesis = boto3.client('kinesis', region_name=REGION)

def load_data(file_path):
    """Carga datos desde el archivo CSV"""
    df = pd.read_csv(file_path)
    return df

def run_producer():
    """
    Productor de datos que lee el CSV de FuelConsumption y envía
    registros a Kinesis Data Stream
    """
    df = load_data(INPUT_FILE)
    records_sent = 0
    
    logger.info(f"Iniciando transmisión al stream: {STREAM_NAME}...")
    logger.info(f"Total de registros a procesar: {len(df)}")
    
    # Iterar sobre cada fila del DataFrame
    for index, row in df.iterrows():
        # Estructura del mensaje a enviar
        payload = {
            'marca': row['MAKE'],
            'modelo': row['MODEL'],
            'clase_vehiculo': row['VEHICLECLASS'],
            'año': int(row['MODELYEAR']),
            'tamaño_motor': float(row['ENGINESIZE']),
            'cilindros': int(row['CYLINDERS']),
            'transmision': row['TRANSMISSION'],
            'tipo_combustible': row['FUELTYPE'],
            'consumo_ciudad': float(row['FUELCONSUMPTION_CITY']),
            'consumo_carretera': float(row['FUELCONSUMPTION_HWY']),
            'consumo_combinado': float(row['FUELCONSUMPTION_COMB']),
            'consumo_mpg': int(row['FUELCONSUMPTION_COMB_MPG']),
            'emisiones_co2': int(row['CO2EMISSIONS'])
        }
        
        # Enviar a Kinesis
        try:
            response = kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(payload),
                PartitionKey=row['MAKE']  # Particionar por marca
            )
            
            records_sent += 1
            
            if records_sent % 50 == 0:
                logger.info(f"Progreso: {records_sent}/{len(df)} registros enviados")
            
            time.sleep(0.1)  # Pausa para simular streaming
            
        except Exception as e:
            logger.error(f"Error al enviar registro {index}: {str(e)}")
            continue

    logger.info(f"Fin de la transmisión. Total registros enviados: {records_sent}")

if __name__ == '__main__':
    run_producer()