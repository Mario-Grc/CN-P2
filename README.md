# AWS Data Pipeline - Consumo de Combustible

Este proyecto despliega una tubería de datos completa en AWS para ingerir, procesar y analizar datos de consumo de combustible de vehículos.

## Arquitectura

1.  **Ingesta**: Script `kinesis.py` envía datos desde un CSV a un **Kinesis Data Stream**.
2.  **Transformación**: **Kinesis Firehose** con una **Lambda** de transformación almacena los datos en **S3** (zona `raw`).
3.  **Procesamiento ETL**: **AWS Glue** rastrea los datos (`Crawler`) y ejecuta trabajos (`Jobs`) para agregar la información:
    *   Agregación por Fabricante (`by_make`).
    *   Agregación por Clase de Vehículo (`by_vehicle_class`).
4.  **Almacenamiento**: Los resultados se guardan en S3 (zona `processed`).

## Requisitos Previos

*   **Linux/MacOS** (o WSL en Windows).
*   **AWS CLI v2** instalado y configurado con tus credenciales (`aws configure`).
*   **uv** instalado (Gestor de paquetes de Python).
    *   Instalar: `curl -LsSf https://astral.sh/uv/install.sh | sh`

## Instalación

Clona el repositorio y sincroniza las dependencias:

```bash
# Instalar dependencias definidas en uv.lock
uv sync
```

## Ejecución del Proyecto

El script principal `script.sh` automatiza todo el proceso: crea recursos, sube scripts, ejecuta crawlers y jobs.

```bash
# Dar permisos de ejecución
chmod +x script.sh

# Ejecutar con uv para asegurar el entorno Python correcto
uv run script.sh
```

El script tomará unos minutos. Verás logs de progreso en la consola. Al finalizar, mostrará un resumen de los recursos creados.

## Limpieza de Recursos

Para eliminar **todos** los recursos creados en AWS y evitar costes:

```bash
# Dar permisos
chmod +x cleanup.sh

# Ejecutar limpieza
uv run cleanup.sh
```

> [!WARNING]
> Esto eliminará el Bucket S3, las bases de datos de Glue y todos los flujos de Kinesis creados por este proyecto.
