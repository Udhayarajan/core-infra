function Run-Service($svc) {
    Write-Host "=== $($svc.ToUpper()) ==="

    # Start logs as a child process sharing stdout
    $proc = Start-Process -FilePath "docker" `
                          -ArgumentList "compose", "logs", "-f", $svc `
                          -NoNewWindow `
                          -PassThru

    docker wait $svc | Out-Null

    # Give logs a moment to flush, then kill
    Start-Sleep -Milliseconds 500
    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
}

docker compose up -d
Run-Service "generator"
Run-Service "processor"
Run-Service "validator"
