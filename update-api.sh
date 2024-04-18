set -e

rm -rf src/auto/
mkdir src/auto/

curl -s http://localhost:8082/api/agent/doc.json > src/auto/api.json

docker run --rm -v "${PWD}/src/auto/:/local" \
  openapitools/openapi-generator-cli:v6.6.0 generate \
  -i /local/api.json \
  -g python-prior \
  -o /local/ \
  -p packageName=auto_api_client

docker-compose exec -ti unison-agent pip install -r requirements.txt
