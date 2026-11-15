# Ferma eventuali istanze locali di Node-RED avviate con node.exe
Write-Host "Ricerca e arresto istanze Node-RED in esecuzione..." -ForegroundColor Yellow

$procs = Get-CimInstance Win32_Process -Filter "Name = 'node.exe'" | Where-Object {
    $_.CommandLine -match 'node-red' -or $_.CommandLine -match 'red\.js'
}

if ($procs) {
    foreach ($p in $procs) {
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
        Write-Host "Terminato processo Node-RED PID=$($p.ProcessId)" -ForegroundColor Green
    }
} else {
    Write-Host "Nessuna istanza Node-RED attiva." -ForegroundColor DarkGray
}
