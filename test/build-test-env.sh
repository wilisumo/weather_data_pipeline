echo "Waiting for Localstack to be ready..."
waitforit -address=http://localstack:4566 -timeout=220 -retry=10 -debug

echo "AWS is UP - Executing commands..."
echo "AWS is UP - Executing NEW commands..."
echo "Creation test enviroment for validation"
aws --endpoint-url=http://localstack:4566 s3 mb s3://data-dev-refined
