build:
	docker compose build

ifeq ($(OS),Windows_NT)
    RUN := powershell -ExecutionPolicy Bypass -File scripts/run.ps1
else
    RUN := bash scripts/run.sh
endif

run:
	start := $$(date +%s)
	@echo "Running the pipeline..."
	$(RUN)
	@echo "Pipeline execution completed in $$(( $$(date +%s) - start )) seconds."


local_run:
	@cmd /C "$(CURDIR)\scripts\local.bat"
