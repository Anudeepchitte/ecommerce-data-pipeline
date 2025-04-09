#!/bin/bash
# AWS deployment script for E-Commerce Data Pipeline
# This script deploys the pipeline to AWS cloud environment

set -e

echo "Starting AWS deployment of E-Commerce Data Pipeline..."

# Define variables
REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || echo "/home/ubuntu/ecommerce_pipeline_repo")
CONFIG_FILE="$REPO_ROOT/config/production/aws_config.yaml"
STACK_NAME="ecommerce-pipeline"
S3_BUCKET="ecommerce-pipeline-artifacts"
REGION="us-east-1"

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI is not installed. Please install AWS CLI first."
    exit 1
fi

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: AWS configuration file not found at $CONFIG_FILE"
    exit 1
fi

# Load configuration
echo "Loading configuration from $CONFIG_FILE..."
eval $(python -c "
import yaml
with open('$CONFIG_FILE', 'r') as f:
    config = yaml.safe_load(f)
for key, value in config.items():
    if isinstance(value, str):
        print(f'{key}=\"{value}\"')
    else:
        print(f'{key}={value}')
")

# Create S3 bucket if it doesn't exist
echo "Creating S3 bucket for artifacts..."
aws s3api create-bucket --bucket $S3_BUCKET --region $REGION || true

# Package CloudFormation template
echo "Packaging CloudFormation template..."
aws cloudformation package \
    --template-file "$REPO_ROOT/config/production/cloudformation.yaml" \
    --s3-bucket $S3_BUCKET \
    --output-template-file "$REPO_ROOT/config/production/packaged-template.yaml"

# Deploy CloudFormation stack
echo "Deploying CloudFormation stack..."
aws cloudformation deploy \
    --template-file "$REPO_ROOT/config/production/packaged-template.yaml" \
    --stack-name $STACK_NAME \
    --capabilities CAPABILITY_IAM \
    --parameter-overrides \
        Environment=Production \
        VpcId=$VPC_ID \
        SubnetIds=$SUBNET_IDS \
        KeyName=$KEY_NAME

# Wait for stack creation to complete
echo "Waiting for stack creation to complete..."
aws cloudformation wait stack-create-complete --stack-name $STACK_NAME

# Get stack outputs
echo "Getting stack outputs..."
OUTPUTS=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --query "Stacks[0].Outputs" --output json)

# Extract important outputs
DASHBOARD_URL=$(echo $OUTPUTS | python -c "import json, sys; outputs = json.load(sys.stdin); print(next((o['OutputValue'] for o in outputs if o['OutputKey'] == 'DashboardURL'), 'N/A'))")
AIRFLOW_URL=$(echo $OUTPUTS | python -c "import json, sys; outputs = json.load(sys.stdin); print(next((o['OutputValue'] for o in outputs if o['OutputKey'] == 'AirflowURL'), 'N/A'))")
S3_DATA_BUCKET=$(echo $OUTPUTS | python -c "import json, sys; outputs = json.load(sys.stdin); print(next((o['OutputValue'] for o in outputs if o['OutputKey'] == 'DataBucket'), 'N/A'))")

# Upload code and data
echo "Uploading code and data to S3..."
aws s3 sync "$REPO_ROOT/src" "s3://$S3_DATA_BUCKET/code/"
aws s3 sync "$REPO_ROOT/config" "s3://$S3_DATA_BUCKET/config/"

# Initialize the pipeline
echo "Initializing the pipeline..."
INSTANCE_ID=$(echo $OUTPUTS | python -c "import json, sys; outputs = json.load(sys.stdin); print(next((o['OutputValue'] for o in outputs if o['OutputKey'] == 'EC2InstanceId'), 'N/A'))")
aws ssm send-command \
    --instance-ids $INSTANCE_ID \
    --document-name "AWS-RunShellScript" \
    --parameters commands=["cd /opt/ecommerce-pipeline && python -m src.orchestration.initialize_pipeline --config config/production/pipeline_config.yaml"]

echo "AWS deployment completed successfully!"
echo "Dashboard URL: $DASHBOARD_URL"
echo "Airflow URL: $AIRFLOW_URL"
echo "S3 Data Bucket: $S3_DATA_BUCKET"
echo "EC2 Instance ID: $INSTANCE_ID"

echo "To monitor the deployment: aws cloudformation describe-stack-events --stack-name $STACK_NAME"
echo "To delete the deployment: aws cloudformation delete-stack --stack-name $STACK_NAME"
