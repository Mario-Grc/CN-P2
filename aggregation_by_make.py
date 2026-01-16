import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, avg, min as spark_min, max as spark_max, count
from awsglue.dynamicframe import DynamicFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    args = getResolvedOptions(sys.argv, ['database', 'table', 'output_path'])
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    
    # Leer datos desde Glue Catalog
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=args['database'],
        table_name=args['table']
    )
    
    df = dynamic_frame.toDF()
    logger.info(f"Registros leídos: {df.count()}")
    
    # Agregación por marca
    aggregated_df = df.groupBy("marca") \
        .agg(
            count("*").alias("total_modelos"),
            avg("emisiones_co2").alias("emisiones_promedio"),
            avg("consumo_combinado").alias("consumo_promedio"),
            spark_min("emisiones_co2").alias("emisiones_minimas"),
            spark_max("emisiones_co2").alias("emisiones_maximas"),
            avg("tamaño_motor").alias("tamaño_motor_promedio")
        ) \
        .orderBy(col("emisiones_promedio").desc())
    
    # Escribir resultado
    output_dynamic_frame = DynamicFrame.fromDF(aggregated_df, glueContext, "output")
    
    glueContext.write_dynamic_frame.from_options(
        frame=output_dynamic_frame,
        connection_type="s3",
        connection_options={"path": args['output_path']},
        format="parquet"
    )
    
    logger.info(f"Completado. Marcas analizadas: {aggregated_df.count()}")

if __name__ == "__main__":
    main()
