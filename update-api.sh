set -e

rm -rf src/auto/
mkdir src/auto/

curl -s http://localhost:8082/api/agent/doc.json > src/auto/api.json

docker run --rm -v "${PWD}/src/auto/:/local" \
  openapitools/openapi-generator-cli generate \
  -i /local/api.json \
  -g python-prior \
  -o /local/ \
  -p packageName=auto_api_client

docker exec -ti unison-agent pip install -r requirements.txt
