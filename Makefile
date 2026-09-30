DIR1=./postgres
DIR2=./kafka
DIR3=./shop
DIR4=./streams
DIR5=./messenger
DIR6=./connects

.PHONY: up down restart copy-env

up:
	docker compose -f $(DIR1)/docker-compose.yml up -d
	docker compose -f $(DIR2)/docker-compose.yml up -d
	docker compose -f $(DIR3)/docker-compose.yml up -d
	docker compose -f $(DIR4)/docker-compose.yml up -d
	docker compose -f $(DIR5)/docker-compose.yml up -d
	docker compose -f $(DIR6)/docker-compose.yml up -d

down:
	docker compose -f $(DIR1)/docker-compose.yml down
	docker compose -f $(DIR2)/docker-compose.yml down
	docker compose -f $(DIR3)/docker-compose.yml down
	docker compose -f $(DIR4)/docker-compose.yml down
	docker compose -f $(DIR5)/docker-compose.yml down
	docker compose -f $(DIR6)/docker-compose.yml down

restart: down up

.PHONY: copy-env

copy-env:
	-cp $(DIR1)/.env.example $(DIR1)/.env
	-cp $(DIR3)/.env.example $(DIR3)/.env
	-cp $(DIR5)/.env.example $(DIR5)/.env
	-cp $(DIR6)/.env.example $(DIR6)/.env
