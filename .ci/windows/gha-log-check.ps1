$ProgressPreference = 'Continue'
$VerbosePreference = 'Continue'
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version 2.0

$rabbitmq_log_dir = Join-Path -Path $env:AppData -ChildPath 'RabbitMQ' | Join-Path -ChildPath 'log'
Write-Host "[INFO] looking for errors in '$rabbitmq_log_dir'"

If (Get-ChildItem $rabbitmq_log_dir\*.log | Select-String -Quiet -SimpleMatch -Pattern inet_error)
{
    # Note: only issuing a warning since `inet_error,econnaborted` can be found
    # in Windows test runs
    Write-Host "[WARNING] found inet_error in '$rabbitmq_log_dir'"
}
