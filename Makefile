build:
	docker compose build

run-seq:
	docker compose up -d

	@echo "=== GENERATOR ==="
	docker compose logs -f generator & \
	LOG_PID=$$!; \
	docker wait generator > /dev/null; \
	kill $$LOG_PID;

	@echo "=== PROCESSOR ==="
	docker compose logs -f processor & \
	LOG_PID=$$!; \
	docker wait processor > /dev/null; \
	kill $$LOG_PID;

	@echo "=== VALIDATOR ==="
	docker compose logs -f validator & \
	LOG_PID=$$!; \
	docker wait validator > /dev/null; \
	kill $$LOG_PID;
