#!/bin/bash

export AWS_REGION="us-east-1"
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME="datalake-fuel-consumption-${ACCOUNT_ID}"
export ROLE_ARN=$(aws iam get-role --role-name LabRole --query 'Role.Arn' --output text)

echo "Usando Bucket: $BUCKET_NAME y Role: $ROLE_ARN"

# ========== S3 BUCKET ==========
# Crear el bucket
aws s3 mb s3://$BUCKET_NAME

# Crear carpetas
aws s3api put-object --bucket $BUCKET_NAME --key raw/
aws s3api put-object --bucket $BUCKET_NAME --key raw/vehicle_fuel_data/
aws s3api put-object --bucket $BUCKET_NAME --key processed/
aws s3api put-object --bucket $BUCKET_NAME --key config/
aws s3api put-object --bucket $BUCKET_NAME --key scripts/
aws s3api put-object --bucket $BUCKET_NAME --key queries/
aws s3api put-object --bucket $BUCKET_NAME --key errors/

# ========== KINESIS DATA STREAM ==========
aws kinesis create-stream --stream-name fuel-consumption-stream --shard-count 1

# ========== LAMBDA + FIREHOSE ==========
# Crear zip con la función Lambda
python -c "import zipfile, sys; z=zipfile.ZipFile('firehose.zip', 'w'); z.write(sys.argv[1]); z.close()" "firehose.py"

# Crear función Lambda
aws lambda create-function \
--function-name fuel-firehose-lambda \
--runtime python3.12 \
--role $ROLE_ARN \
--handler firehose.lambda_handler \
--zip-file fileb://firehose.zip \
--timeout 60 \
--memory-size 128

export LAMBDA_ARN=$(aws lambda get-function --function-name fuel-firehose-lambda --query 'Configuration.FunctionArn' --output text)

aws lambda update-function-code \
--function-name fuel-firehose-lambda \
--zip-file fileb://firehose.zip

# Crear Kinesis Firehose
aws firehose create-delivery-stream \
--delivery-stream-name fuel-delivery-stream \
--delivery-stream-type KinesisStreamAsSource \
--kinesis-stream-source-configuration "KinesisStreamARN=arn:aws:kinesis:$AWS_REGION:$ACCOUNT_ID:stream/fuel-consumption-stream,RoleARN=$ROLE_ARN" \
--extended-s3-destination-configuration '{
  "BucketARN": "arn:aws:s3:::'$BUCKET_NAME'",
  "RoleARN": "'$ROLE_ARN'",
  "Prefix": "raw/vehicle_fuel_data/processing_date=!{partitionKeyFromLambda:processing_date}/",
  "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/",
  "BufferingHints": {
    "SizeInMBs": 64,
    "IntervalInSeconds": 60
  },
  "DynamicPartitioningConfiguration": {
    "Enabled": true,
    "RetryOptions": {
      "DurationInSeconds": 300
    }
  },
  "ProcessingConfiguration": {
    "Enabled": true,
    "Processors": [
      {
        "Type": "Lambda",
        "Parameters": [
          {
            "ParameterName": "LambdaArn",
            "ParameterValue": "'$LAMBDA_ARN'"
          },
          {
            "ParameterName": "BufferSizeInMBs",
            "ParameterValue": "1"
          },
          {
            "ParameterName": "BufferIntervalInSeconds",
            "ParameterValue": "60"
          }
        ]
      }
    ]
  }
}'

# ========== GLUE DATABASE Y CRAWLER ==========
aws glue create-database --database-input "{\"Name\":\"fuel_consumption_db\"}"

aws glue create-crawler \
--name fuel-raw-crawler \
--role $ROLE_ARN \
--database-name fuel_consumption_db \
--targets "{\"S3Targets\": [{\"Path\": \"s3://$BUCKET_NAME/raw/vehicle_fuel_data\"}]}"

# ========== ENVIAR DATOS A KINESIS ==========
echo "Enviando datos del CSV a Kinesis..."
python kinesis.py

# Esperar a que Firehose escriba los datos en S3 (buffer de 60 segundos)
echo "Esperando 60 segundos para que Firehose procese los datos..."
sleep 60

# ========== EJECUTAR CRAWLER ==========
echo "Ejecutando crawler para detectar esquema..."
aws glue start-crawler --name fuel-raw-crawler

# Esperar a que el crawler termine
echo "Esperando a que el crawler termine..."
sleep 120

# ========== SUBIR SCRIPTS DE AGREGACIÓN ==========
aws s3 cp aggregation_by_make.py s3://$BUCKET_NAME/scripts/
aws s3 cp aggregation_by_vehicle_class.py s3://$BUCKET_NAME/scripts/

# ========== CREAR GLUE ETL JOBS ==========
export DATABASE="fuel_consumption_db"
export TABLE="vehicle_fuel_data"
export BY_MAKE_OUTPUT="s3://$BUCKET_NAME/processed/by_make/"
export BY_CLASS_OUTPUT="s3://$BUCKET_NAME/processed/by_vehicle_class/"

# Job 1: Agregación por marca
aws glue create-job \
--name fuel-aggregation-by-make \
--role $ROLE_ARN \
--command '{
  "Name": "glueetl",
  "ScriptLocation": "s3://'$BUCKET_NAME'/scripts/aggregation_by_make.py",
  "PythonVersion": "3"
}' \
--default-arguments '{
  "--database": "'$DATABASE'",
  "--table": "'$TABLE'",
  "--output_path": "s3://'$BUCKET_NAME'/processed/by_make/",
  "--enable-continuous-cloudwatch-log": "true",
  "--spark-event-logs-path": "s3://'$BUCKET_NAME'/logs/"
}' \
--glue-version "4.0" \
--number-of-workers 2 \
--worker-type "G.1X"

# Job 2: Agregación por clase de vehículo
aws glue create-job \
--name fuel-aggregation-by-vehicle-class \
--role $ROLE_ARN \
--command '{
  "Name": "glueetl",
  "ScriptLocation": "s3://'$BUCKET_NAME'/scripts/aggregation_by_vehicle_class.py",
  "PythonVersion": "3"
}' \
--default-arguments '{
  "--database": "'$DATABASE'",
  "--table": "'$TABLE'",
  "--output_path": "s3://'$BUCKET_NAME'/processed/by_vehicle_class/",
  "--enable-continuous-cloudwatch-log": "true",
  "--spark-event-logs-path": "s3://'$BUCKET_NAME'/logs/"
}' \
--glue-version "4.0" \
--number-of-workers 2 \
--worker-type "G.1X"

# ========== EJECUTAR GLUE JOBS ==========
# ESTO ES LO QUE TENÍA ANTES
# echo "Ejecutando jobs de agregación..."
# aws glue start-job-run --job-name fuel-aggregation-by-make
# aws glue start-job-run --job-name fuel-aggregation-by-vehicle-class

wait_glue_job () {
  local JOB_NAME="$1"
  local RUN_ID="$2"

  while true; do
    STATE=$(aws glue get-job-run \
      --job-name "$JOB_NAME" \
      --run-id "$RUN_ID" \
      --query 'JobRun.JobRunState' \
      --output text)

    echo "Job $JOB_NAME ($RUN_ID) => $STATE"

    case "$STATE" in
      SUCCEEDED) return 0 ;;
      FAILED|TIMEOUT|ERROR|STOPPED) return 1 ;;
      *) sleep 30 ;;
    esac
  done
}

echo "Lanzando job 1..."
RUN_ID_1=$(aws glue start-job-run --job-name fuel-aggregation-by-make --query 'JobRunId' --output text)  # devuelve JobRunId
wait_glue_job "fuel-aggregation-by-make" "$RUN_ID_1" || exit 1

echo "Lanzando job 2..."
RUN_ID_2=$(aws glue start-job-run --job-name fuel-aggregation-by-vehicle-class --query 'JobRunId' --output text)  # devuelve JobRunId
wait_glue_job "fuel-aggregation-by-vehicle-class" "$RUN_ID_2" || exit 1


# ========== VER ESTADO DE LOS JOBS ==========
echo "Estado de los jobs:"
aws glue get-job-runs --job-name fuel-aggregation-by-make --max-items 1
aws glue get-job-runs --job-name fuel-aggregation-by-vehicle-class --max-items 1

# ========== CREAR CRAWLER PARA TABLAS PROCESADAS ==========
echo "Creando crawler para las tablas procesadas..."

# Crawler para datos procesados
aws glue create-crawler \
--name fuel-processed-crawler \
--role $ROLE_ARN \
--database-name fuel_consumption_db \
--targets "{\"S3Targets\": [{\"Path\": \"s3://$BUCKET_NAME/processed/\"}]}"

# Ejecutar el crawler
echo "Ejecutando crawler de datos procesados..."
aws glue start-crawler --name fuel-processed-crawler

echo "========== SETUP COMPLETADO =========="
echo "Bucket: s3://$BUCKET_NAME"
echo "Base de datos: $DATABASE"
echo "Tabla: $TABLE"
