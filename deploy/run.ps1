# Runs the relay server or the agent on Windows, restarting it if it ever stops.
#   .\deploy\run.ps1 -Role server -EnvFile relay.env
#   .\deploy\run.ps1 -Role agent  -EnvFile agent.env -AgentConfig agent.yaml
# The env file holds KEY=VALUE lines (see relay.env.example; for the agent: HADOOP_USER_NAME, HADOOP_USER_PASSWORD).
param(
    [Parameter(Mandatory)][ValidateSet("server", "agent")][string]$Role,
    [Parameter(Mandatory)][string]$EnvFile,
    [string]$AgentConfig = "agent.yaml",
    [int]$Port = 8765,
    [string]$Python = ".venv\Scripts\python.exe"
)
Get-Content $EnvFile | Where-Object { $_ -match '^\s*[^#\s][^=]*=' } | ForEach-Object {
    $k, $v = $_ -split '=', 2
    [Environment]::SetEnvironmentVariable($k.Trim(), $v.Trim(), "Process")
}
while ($true) {
    if ($Role -eq "server") { & $Python -m data_gateway.server }
    else { & $Python -m data_gateway.agent $AgentConfig --port $Port }
    Write-Warning "$Role stopped (exit $LASTEXITCODE); restarting in 10s"
    Start-Sleep 10
}
