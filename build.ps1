[CmdletBinding(PositionalBinding=$false)]
param(
    [switch]$RunTests
)

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
Write-Host "Done building." -ForegroundColor "Green"

if ($RunTests) {
    Write-Host "Running tests: Build.csproj traversal (all frameworks)" -ForegroundColor "Magenta"
    dotnet test $build_csproj_file --no-build --logger 'console;verbosity=detailed' '/p:AltCover=true' "/p:AltCoverStrongNameKey=$PSScriptRoot/rabbit.snk"
    if ($LastExitCode -ne 0)
    {
        Write-Host "Error with tests, aborting build." -Foreground "Red"
        Exit 1
    }
    Write-Host "Tests passed!" -ForegroundColor "Green"
}

Write-Host "Done."
