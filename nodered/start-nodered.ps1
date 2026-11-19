param(
    [ValidateSet("exam", "practice", "templates", "basics", "functions", "datasharing", "udp", "mqtt", "lab1", "lab2")]
    [string]$Mode = "exam",
    [int]$Port = 1880,
    [switch]$WithMqtt
)

if (-not $PSBoundParameters.ContainsKey("Port") -and $Mode -eq "practice") {
    $Port = 1881
}

# Assicura Node.js e npm global nel PATH della sessione corrente
$nodeDir = "$env:LOCALAPPDATA\Programs\nodejs"
$npmDir = "$env:APPDATA\npm"
if (Test-Path $nodeDir) { $env:Path = "$nodeDir;$npmDir;$env:Path" }

if ($WithMqtt -or $Mode -eq "mqtt" -or $Mode -eq "templates" -or $Mode -eq "exam" -or $Mode -eq "practice" -or $Mode -eq "lab2") {
    & "$PSScriptRoot\start-mqtt.ps1"
}

$localNodeRed = Join-Path $PSScriptRoot "node_modules\.bin\node-red.cmd"
$runner = if (Test-Path $localNodeRed) { $localNodeRed } else { "node-red" }

switch ($Mode) {
    "exam"        { $userDir = "$PSScriptRoot\nodered-exam";         $flow = "$PSScriptRoot\nodered-exam\flows.json" }
    "practice"    { $userDir = "$PSScriptRoot\nodered-practice";     $flow = "$PSScriptRoot\nodered-practice\flows.json" }
    "lab1"        { $userDir = "$PSScriptRoot\lab\lab-userdir";      $flow = "$PSScriptRoot\lab\lab1\flows.json" }
    "lab2"        { $userDir = "$PSScriptRoot\lab\lab-userdir";      $flow = "$PSScriptRoot\lab\lab2\flows.json" }
    "templates"   { $userDir = "$PSScriptRoot\nodered-lab-template"; $flow = "$PSScriptRoot\nodered-lab-template\flows\all_templates_combined.json" }
    "basics"      { $userDir = "$PSScriptRoot\nodered-lab-template"; $flow = "$PSScriptRoot\nodered-lab-template\flows\01_basics_and_async.json" }
    "functions"   { $userDir = "$PSScriptRoot\nodered-lab-template"; $flow = "$PSScriptRoot\nodered-lab-template\flows\02_function_nodes_patterns.json" }
    "datasharing" { $userDir = "$PSScriptRoot\nodered-lab-template"; $flow = "$PSScriptRoot\nodered-lab-template\flows\03_data_sharing_files_and_context.json" }
    "udp"         { $userDir = "$PSScriptRoot\nodered-lab-template"; $flow = "$PSScriptRoot\nodered-lab-template\flows\04_udp_json_networking.json" }
    "mqtt"        { $userDir = "$PSScriptRoot\nodered-lab-template"; $flow = "$PSScriptRoot\nodered-lab-template\flows\05_mqtt_pubsub_and_qos.json" }
}

Write-Host "Avvio Node-RED [$Mode] su http://localhost:$Port ..." -ForegroundColor Cyan
Write-Host "UserDir: $userDir" -ForegroundColor DarkGray
Write-Host "Flow:    $flow" -ForegroundColor DarkGray
Write-Host "Premi Ctrl+C per fermare Node-RED." -ForegroundColor Yellow

& $runner -p $Port -u $userDir $flow
