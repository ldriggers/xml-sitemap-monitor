# run.ps1 - XML Sitemap Monitor Runner
# Usage: .\run.ps1 [command]
# Commands: test, monitor, status, all, install

param(
    [Parameter(Position=0)]
    [string]$Command = "help"
)

$ProjectRoot = $PSScriptRoot
$CommonLib = Join-Path $ProjectRoot "..\10.07-seo-intel-common"

function Show-Help {
    Write-Host @"

XML Sitemap Monitor - Commands
==============================

  install   - Install all dependencies
  test      - Run smoke tests (fast, deterministic)
  live      - Run live tests (network, ~30s)
  monitor   - Run sitemap monitor (all domains)
  status    - Run status checker (all domains)
  all       - Run monitor + status
  stealth   - Run stealth test (prompts for URL)
  help      - Show this help

Examples:
  .\run.ps1 install
  .\run.ps1 test
  .\run.ps1 live
  .\run.ps1 all

"@
}

function Install-Deps {
    Write-Host "`n=== Installing Dependencies ===" -ForegroundColor Cyan
    Set-Location $ProjectRoot
    pip install -r requirements.txt -q
    
    Write-Host "Installing seo-intel-common..."
    Set-Location $CommonLib
    pip install -e . -q
    
    Set-Location $ProjectRoot
    Write-Host "Done!" -ForegroundColor Green
}

function Run-Tests {
    Write-Host "`n=== Running Smoke Tests ===" -ForegroundColor Cyan
    Set-Location $ProjectRoot
    py tests/test_smoke.py
}

function Run-Live-Tests {
    Write-Host "`n=== Running Live Tests ===" -ForegroundColor Cyan
    Set-Location $ProjectRoot
    py tests/test_live.py
}

function Run-Monitor {
    Write-Host "`n=== Running Sitemap Monitor ===" -ForegroundColor Cyan
    Set-Location $ProjectRoot
    py -m src.main
}

function Run-Status {
    Write-Host "`n=== Running Status Checker ===" -ForegroundColor Cyan
    Set-Location $ProjectRoot
    py -m src.url_status_checker
}

function Run-Stealth {
    Write-Host "`n=== Stealth Fetcher ===" -ForegroundColor Cyan
    $url = Read-Host "Enter URL to test"
    Set-Location $CommonLib
    py -m seo_intel.stealth $url
    Set-Location $ProjectRoot
}

switch ($Command.ToLower()) {
    "help"    { Show-Help }
    "install" { Install-Deps }
    "test"    { Run-Tests }
    "live"    { Run-Live-Tests }
    "monitor" { Run-Monitor }
    "status"  { Run-Status }
    "all"     { Run-Monitor; Run-Status }
    "stealth" { Run-Stealth }
    default   { 
        Write-Host "Unknown command: $Command" -ForegroundColor Red
        Show-Help 
    }
}

# Return to project root
Set-Location $ProjectRoot

