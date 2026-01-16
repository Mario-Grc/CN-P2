#!/bin/bash
set -euo pipefail

export AWS_REGION="us-east-1"
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME="datalake-fuel-consumption-${ACCOUNT_ID}"

log() { echo "[$(date +'%H:%M:%S')] $*"; }

# ---------- Helpers Glue ----------
stop_all_job_runs () {
  local JOB_NAME="$1"

  # Obtener runs recientes; filtrar los que no son terminales
  local RUN_IDS
  RUN_IDS=$(aws glue get-job-runs \
    --job-name "$JOB_NAME" \
    --max-items 50 \
    --query 'JobRuns[?JobRunState!=`SUCCEEDED` && JobRunState!=`FAILED` && JobRunState!=`STOPPED` && JobRunState!=`TIMEOUT`].Id' \
    --output text) || return 0  # si el job no existe, no pasa nada

  if [[ -n "${RUN_IDS:-}" && "${RUN_IDS}" != "None" ]]; then
    log "Deteniendo job runs de $JOB_NAME: $RUN_IDS"
    # batch-stop-job-run acepta lista; en CLI se pasa como lista de args
    aws glue batch-stop-job-run --job-name "$JOB_NAME" --job-run-ids $RUN_IDS  # stop runs
  else
    log "No hay runs activos para $JOB_NAME"
  fi
}

wait_crawler_stopped () {
  local CRAWLER="$1"
  for i in {1..20}; do
    STATE=$(aws glue get-crawler --name "$CRAWLER" --query 'Crawler.State' --output text 2>/dev/null || true)
    [[ -z "$STATE" || "$STATE" == "None" ]] && return 0
    if [[ "$STATE" == "READY" ]]; then
      return 0
    fi
    log "Crawler $CRAWLER state=$STATE (esperando...)"
    sleep 5
  done
}

# ---------- Helpers Firehose ----------
wait_firehose_deleted () {
  local NAME="$1"
  for i in {1..30}; do
    # Si describe falla, ya no existe.
    if ! aws firehose describe-delivery-stream --delivery-stream-name "$NAME" >/dev/null 2>&1; then
      log "Firehose $NAME eliminado."
      return 0
    fi
    STATUS=$(aws firehose describe-delivery-stream --delivery-stream-name "$NAME" --query 'DeliveryStreamDescription.DeliveryStreamStatus' --output text 2>/dev/null || true)
    log "Firehose $NAME status=$STATUS (DeleteDeliveryStream es asíncrono)..."  # async delete
    sleep 10
  done
}

# ---------- Helpers Kinesis ----------
wait_kinesis_deleted () {
  local NAME="$1"
  for i in {1..30}; do
    if ! aws kinesis describe-stream-summary --stream-name "$NAME" >/dev/null 2>&1; then
      log "Kinesis stream $NAME eliminado."
      return 0
    fi
    STATUS=$(aws kinesis describe-stream-summary --stream-name "$NAME" --query 'StreamDescriptionSummary.StreamStatus' --output text 2>/dev/null || true)
    log "Kinesis $NAME status=$STATUS (esperando...)"
    sleep 10
  done
}

# ---------- Start ----------
log "========================================="
log "ELIMINANDO RECURSOS AWS DEL PROYECTO"
log "Bucket: $BUCKET_NAME"
log "========================================="

log "1) Parando runs de Glue jobs (antes de borrar jobs)..."
stop_all_job_runs "fuel-aggregation-by-make"
stop_all_job_runs "fuel-aggregation-by-vehicle-class"

sleep 10

log "2) Parando y borrando crawlers..."
for c in fuel-raw-crawler fuel-processed-crawler; do
  aws glue stop-crawler --name "$c" >/dev/null 2>&1 || true
  wait_crawler_stopped "$c"
  aws glue delete-crawler --name "$c" >/dev/null 2>&1 || true
done

log "3) Borrando Glue jobs..."
aws glue delete-job --job-name fuel-aggregation-by-make >/dev/null 2>&1 || true  # delete-job
aws glue delete-job --job-name fuel-aggregation-by-vehicle-class >/dev/null 2>&1 || true


log "4) Borrando tablas y DB de Glue..."
aws glue delete-table --database-name fuel_consumption_db --name vehicle_fuel_data >/dev/null 2>&1 || true
aws glue delete-table --database-name fuel_consumption_db --name by_make >/dev/null 2>&1 || true
aws glue delete-table --database-name fuel_consumption_db --name by_vehicle_class >/dev/null 2>&1 || true
aws glue delete-database --name fuel_consumption_db >/dev/null 2>&1 || true



log "5) Borrando Firehose..."
aws firehose delete-delivery-stream --delivery-stream-name fuel-delivery-stream >/dev/null 2>&1 || true
wait_firehose_deleted "fuel-delivery-stream"

log "6) Borrando Lambda..."
aws lambda delete-function --function-name fuel-firehose-lambda >/dev/null 2>&1 || true

log "7) Borrando Kinesis streams..."
aws kinesis delete-stream --stream-name fuel-consumption-stream --enforce-consumer-deletion >/dev/null 2>&1 || true
wait_kinesis_deleted "fuel-consumption-stream"

log "8) Vaciando y borrando buckets S3..."
aws s3 rm "s3://$BUCKET_NAME" --recursive >/dev/null 2>&1 || true
aws s3 rb "s3://$BUCKET_NAME" --force >/dev/null 2>&1 || true



log "9) Limpieza local..."
rm -f firehose.zip >/dev/null 2>&1 || true

log "========================================="
log "LIMPIEZA COMPLETADA"
log "========================================="
