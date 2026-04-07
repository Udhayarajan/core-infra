build:
	docker compose build

ifeq ($(OS),Windows_NT)
    RUN := powershell -ExecutionPolicy Bypass -File scripts/run.ps1
else
    RUN := bash scripts/run.sh
endif

run:
	$(RUN)


local_run:
	@cmd /C "$(CURDIR)\scripts\local.bat"
