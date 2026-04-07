build:
	docker compose build

run:
	docker compose up -d

	@echo "=== GENERATOR ==="
	docker wait generator > NUL 2>&1 || true
	docker compose logs generator

	@echo "=== PROCESSOR ==="
	docker wait processor > NUL 2>&1 || true
	docker compose logs processor

	@echo "=== VALIDATOR ==="
	docker wait validator > NUL 2>&1 || true
	docker compose logs validator
