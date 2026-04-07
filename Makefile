build:
	docker compose build

run:
	docker compose up -d

	@echo "=== GENERATOR ==="
	docker compose logs generator
	docker wait generator

	@echo "=== PROCESSOR ==="
	docker compose logs processor
	docker wait processor

	@echo "=== VALIDATOR ==="
	docker compose logs validator
	docker wait validator
