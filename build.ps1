[CmdletBinding(PositionalBinding=$false)]
param(
    [switch]$RunTests,
    [switch]$RunTestsUntilFailure
)

New-Variable -Name verbose -Option Constant `
    -Value ($VerbosePreference -ne 'SilentlyContinue')

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$PSNativeCommandUseErrorActionPreference = $true

Write-Host "Run Parameters:" -ForegroundColor Cyan
Write-Host "`tPSScriptRoot: $PSScriptRoot"
Write-Host "`tRunTests: $RunTests"
Write-Host "`tdotnet --version: $(dotnet --version)"

Write-Host "Building all projects (Build.csproj traversal)..." -ForegroundColor "Magenta"

New-Variable -Name build_csproj_file -Option Constant `
    -Value (Join-Path -Path $PSScriptRoot -ChildPath 'Build.csproj')

dotnet build $build_csproj_file

Write-Host "[INFO] done building." -ForegroundColor "Green"

$ErrorActionPreference = 'Continue'
$PSNativeCommandUseErrorActionPreference = $true

if ($RunTests -or $RunTestsUntilFailure)
{
    Do
    {
        Write-Host "Running tests: Build.csproj traversal (all frameworks)" -ForegroundColor "Magenta"
        if ($verbose)
        {
            # dotnet test $build_csproj_file --diag $(Join-Path -Path $PSScriptRoot -ChildPath diag.log) --environment=RABBITMQ_CLIENT_TESTS_VERBOSE=true --no-build --logger 'console;verbosity=detailed'
            dotnet test $build_csproj_file --diag $(Join-Path -Path $PSScriptRoot -ChildPath diag | Join-Path -ChildPath diag.log) --no-build --logger 'console;verbosity=detailed'
        }
        else
        {
            dotnet test $build_csproj_file --no-build --logger 'console;verbosity=detailed'
        }
        if ($LASTEXITCODE -ne 0)
        {
            Write-Host "[ERROR] FYI, tests errored" -Foreground "Red"
            # Write-Host "[ERROR] tests errored, exiting" -Foreground "Red"
            # Exit 1
        }
        else
        {
            Write-Host "[INFO] tests passed" -ForegroundColor "Green"
        }
    } While ($RunTestsUntilFailure)
}
