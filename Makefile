include .env

.PHONY:  devcontainer-build


devcontainer-build:
	docker compose -f .devcontainer/docker-compose.yml build neo-builder-devcontainer


neo4j-start:
	docker compose -f .devcontainer/docker-compose.yml up -d neo-builder-neo4j

neo4j-stop:
	docker compose -f .devcontainer/docker-compose.yml stop neo-builder-neo4j

neo4j-flush: neo4j-stop
	$(info *** WARNING you are deleting all data from neo4j ***)
	sudo rm -r resources/db/neo4j/data
	docker compose -f .devcontainer/docker-compose.yml up -d neo-builder-neo4j
