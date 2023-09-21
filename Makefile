up:
	cp -n config-dist.yaml config.yaml ;\
	docker-compose -f docker-compose.yml -f docker-compose.dev.yml down --remove-orphans ;\
	docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d ;\
    docker exec -ti unison-agent sh -c "pip install -r requirements.txt"

