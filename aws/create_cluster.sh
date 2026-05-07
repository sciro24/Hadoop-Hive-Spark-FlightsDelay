#!/bin/bash
# ─── Crea cluster EMR su AWS Academy ─────────────────────────────────────────
# Prerequisiti:
#   1. AWS CLI configurato con credenziali Academy
#   2. upload_data.sh già eseguito (dati e bootstrap su S3)
#   3. Una EC2 Key Pair creata nella tua regione (per SSH)
#
# Uso:
#   bash aws/create_cluster.sh [--key-name NomeKeyPair]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/config.sh"

KEY_NAME="${1:-vockey}"    # Cambia con il nome della tua key pair AWS Academy

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║            CREAZIONE CLUSTER EMR                        ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Nome:    $EMR_CLUSTER_NAME"
echo "║  Release: $EMR_RELEASE"
echo "║  Master:  $MASTER_TYPE (x1)"
echo "║  Worker:  $WORKER_TYPE (x${NUM_WORKERS})"
echo "║  Regione: $EMR_REGION"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

CLUSTER_ID=$(aws emr create-cluster \
    --name "$EMR_CLUSTER_NAME" \
    --release-label "$EMR_RELEASE" \
    --region "$EMR_REGION" \
    --applications Name=Hadoop Name=Hive Name=Spark \
    --instance-groups \
        InstanceGroupType=MASTER,InstanceCount=1,InstanceType="$MASTER_TYPE" \
        InstanceGroupType=CORE,InstanceCount="$NUM_WORKERS",InstanceType="$WORKER_TYPE" \
    --use-default-roles \
    --ec2-attributes KeyName="$KEY_NAME" \
    --log-uri "${S3_LOGS}/emr/" \
    --bootstrap-actions \
        Path="${S3_SCRIPTS}/bootstrap.sh",Name="Install Python deps" \
    --configurations '[
        {
          "Classification": "spark",
          "Properties": {
            "maximizeResourceAllocation": "false"
          }
        },
        {
          "Classification": "spark-defaults",
          "Properties": {
            "spark.sql.shuffle.partitions": "'"$SHUFFLE_PARTITIONS"'",
            "spark.executor.memory": "'"$EXECUTOR_MEMORY"'",
            "spark.executor.cores":  "'"$EXECUTOR_CORES"'"
          }
        },
        {
          "Classification": "hive-site",
          "Properties": {
            "hive.metastore.client.factory.class":
              "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
          }
        }
    ]' \
    --no-auto-terminate \
    --query 'ClusterId' \
    --output text)

echo "✅  Cluster creato: $CLUSTER_ID"
echo ""
echo "Attendi che il cluster raggiunga lo stato WAITING (~5-10 min):"
echo "  aws emr describe-cluster --cluster-id $CLUSTER_ID --region $EMR_REGION --query 'Cluster.Status.State'"
echo ""
echo "Per ottenere l'IP del master node:"
echo "  aws emr describe-cluster --cluster-id $CLUSTER_ID --region $EMR_REGION --query 'Cluster.MasterPublicDnsName' --output text"
echo ""
echo "Per SSH sul master (con la key pair '$KEY_NAME'):"
echo "  ssh -i ~/.ssh/${KEY_NAME}.pem hadoop@\$(aws emr describe-cluster --cluster-id $CLUSTER_ID --region $EMR_REGION --query 'Cluster.MasterPublicDnsName' --output text)"
echo ""
echo "Salva il Cluster ID:"
echo "  export EMR_CLUSTER_ID=$CLUSTER_ID"

# Salva l'ID in un file per gli script successivi
echo "$CLUSTER_ID" > "$SCRIPT_DIR/.cluster_id"
echo "(Cluster ID salvato in aws/.cluster_id)"
