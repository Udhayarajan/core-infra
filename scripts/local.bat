@echo off
start "processor" cmd /k go run ./processor/main.go -max=11_000_000
start "generator" cmd /k go run ./generator/main.go -total=11_000_000
start "validator" cmd /k go run ./validator/main.go
