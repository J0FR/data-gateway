# Windows company PC: start the agent automatically (hidden, in the background) at logon.
#   .\deploy\install_agent_task.ps1 -InstallDir C:\data-gateway
# Expects in InstallDir: .venv, agent.yaml, agent.env (HADOOP_USER_NAME / HADOOP_USER_PASSWORD), deploy\run.ps1
# Remove with:  Unregister-ScheduledTask -TaskName data-gateway-agent -Confirm:$false
param(
    [Parameter(Mandatory)][string]$InstallDir,
    [int]$Port = 8765
)
$run = Join-Path $InstallDir "deploy\run.ps1"
$arg = "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$run`" -Role agent " +
       "-EnvFile `"$InstallDir\agent.env`" -AgentConfig `"$InstallDir\agent.yaml`" -Port $Port " +
       "-Python `"$InstallDir\.venv\Scripts\python.exe`""
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $arg -WorkingDirectory $InstallDir
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -StartWhenAvailable -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero)
Register-ScheduledTask -TaskName "data-gateway-agent" -Action $action -Trigger $trigger `
    -Settings $settings -Description "data-gateway passive agent" -Force | Out-Null
Start-ScheduledTask -TaskName "data-gateway-agent"
Write-Host "Installed and started task 'data-gateway-agent' (port $Port). Open the port to the relay server's IP only."
