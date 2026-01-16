import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, avg, count
from awsglue.dynamicframe import DynamicFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    args = getResolvedOptions(sys.argv, ['database', 'table', 'output_path'])
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=args['database'],
        table_name=args['table']
    )
    
    df = dynamic_frame.toDF()
    
    # Agregación por clase de vehículo
    aggregated_df = df.groupBy("clase_vehiculo") \
        .agg(
            count("*").alias("total_vehiculos"),
            avg("emisiones_co2").alias("emisiones_promedio"),
            avg("consumo_ciudad").alias("consumo_ciudad_promedio"),
            avg("consumo_carretera").alias("consumo_carretera_promedio"),
            avg("cilindros").alias("cilindros_promedio")
        ) \
        .orderBy(col("emisiones_promedio").desc())
    
    output_dynamic_frame = DynamicFrame.fromDF(aggregated_df, glueContext, "output")
    
    glueContext.write_dynamic_frame.from_options(
        frame=output_dynamic_frame,
        connection_type="s3",
        connection_options={"path": args['output_path']},
        format="parquet"
    )
    
    logger.info(f"Completado. Clases analizadas: {aggregated_df.count()}")

if __name__ == "__main__":
    main()
