up:
	docker-compose -f docker-compose.yml -f docker-compose.dev.yml down --remove-orphans ;\
	docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build ;\
    docker-compose -f docker-compose.yml -f docker-compose.dev.yml exec -T unison-agent sh -c "pip install -r requirements.txt" ;\
    docker-compose -f docker-compose.yml -f docker-compose.dev.yml exec -T unison-agent sh -c "pip install pydevd_pycharm==233.13763.5"

down:
	docker-compose -f docker-compose.yml -f docker-compose.dev.yml down --remove-orphans

up-console:
	docker-compose -f docker-compose.yml -f docker-compose.dev.yml down --remove-orphans ;\
	docker-compose -f docker-compose.yml -f docker-compose.dev.yml up ;\
    docker-compose -f docker-compose.yml -f docker-compose.dev.yml exec -T unison-agent sh -c "pip install -r requirements.txt" ;\
    docker-compose -f docker-compose.yml -f docker-compose.dev.yml exec -T unison-agent sh -c "pip install pydevd_pycharm==233.13763.5"

init:
	cp -n config-dist.yaml config.yaml

sh:
	docker compose -f docker-compose.yml -f docker-compose.dev.yml exec -it unison-agent bash

up-api:
	rm -rf src/auto/ ;\
    mkdir src/auto/ ;\
    curl -s http://localhost:8082/api/agent/doc.json > src/auto/api.json ;\
    docker run --rm -v "./src/auto/:/local" \
      openapitools/openapi-generator-cli:v6.6.0 generate \
      -i /local/api.json \
      -g python-prior \
      -o /local/ \
      -p packageName=auto_api_client ;\
    docker-compose -f docker-compose.yml -f docker-compose.dev.yml exec -T unison-agent sh -c "pip install -r requirements.txt" ;\
    docker-compose -f docker-compose.yml -f docker-compose.dev.yml exec -T unison-agent sh -c "pip install pydevd_pycharm"

deploy:
	sudo chown -R admin: Resources/.kube/cache ;\
	git pull -f;\
	docker compose build ;\
    docker compose down --remove-orphans ;\
    docker compose up -d
