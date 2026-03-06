# sync-brian.ps1 — Pull latest from Rance9/Insight-Harbor, optionally commit + push
# Usage: Right-click → "Run in Terminal"  or  Ctrl+Shift+` then: .\scripts\sync-brian.ps1

# Navigate to the repo root (parent of scripts/ folder)
if ($PSScriptRoot) {
    Set-Location (Split-Path $PSScriptRoot -Parent)
}
if (-not (Test-Path (Join-Path (Get-Location) ".git"))) {
    # Try parent of current directory as fallback
    $parent = Split-Path (Get-Location) -Parent
    if ($parent -and (Test-Path (Join-Path $parent ".git"))) {
        Set-Location $parent
    } else {
        Write-Host "ERROR: Could not find the git repository. Make sure VS Code has the Insight Harbor folder open as the workspace." -ForegroundColor Red
        Write-Host "Current directory: $(Get-Location)" -ForegroundColor Red
        exit 1
    }
}
Write-Host "Working directory: $(Get-Location)" -ForegroundColor Gray

# ─────────────────────────────────────────────────────────────
# PHASE 1: Pull latest from origin (Rance9 main repo)
# ─────────────────────────────────────────────────────────────

Write-Host "`n=== Insight Harbor — Sync (Brian) ===" -ForegroundColor Cyan

Write-Host "`n[Sync] Pulling latest from origin/main..." -ForegroundColor Green
git pull origin main
if ($LASTEXITCODE -ne 0) {
    Write-Host "`nPull failed. You may have local conflicts. Resolve them and try again." -ForegroundColor Red
    exit 1
}

Write-Host "[Sync] Local repo is up to date with origin.`n" -ForegroundColor Green

# ─────────────────────────────────────────────────────────────
# PHASE 2: Check for local changes and ask what to do
# ─────────────────────────────────────────────────────────────

$status = git status --porcelain
if (-not $status) {
    Write-Host "No local file changes detected. Your repo is fully synced — nothing to commit." -ForegroundColor Yellow
    Write-Host "`n=== Done ===" -ForegroundColor Cyan
    exit 0
}

Write-Host "Local changes detected:" -ForegroundColor Yellow
git status --short
Write-Host ""

$proceed = Read-Host "Would you like to commit and push these changes? (y/N)"
if ($proceed -notin @("y", "Y", "yes", "Yes", "YES")) {
    Write-Host "`nSync complete. Changes left uncommitted." -ForegroundColor Yellow
    Write-Host "`n=== Done ===" -ForegroundColor Cyan
    exit 0
}

# ─────────────────────────────────────────────────────────────
# PHASE 3: Commit and push
# ─────────────────────────────────────────────────────────────

Write-Host ""
$commitMsg = Read-Host "Commit message (press Enter for default: 'Insight-Harbor-v1.0.0')"
if ([string]::IsNullOrWhiteSpace($commitMsg)) { $commitMsg = "Insight-Harbor-v1.0.0" }

Write-Host "`n[Commit] Staging all changes..." -ForegroundColor Green
git add -A

Write-Host "[Commit] Committing with message: '$commitMsg'" -ForegroundColor Green
git commit -m $commitMsg

Write-Host "[Push] Pushing to origin/main..." -ForegroundColor Green
git push origin main

if ($LASTEXITCODE -eq 0) {
    Write-Host "`nChanges pushed successfully to Rance9/Insight-Harbor." -ForegroundColor Green
} else {
    Write-Host "`nPush failed. Check the output above for details." -ForegroundColor Red
}

Write-Host "`n=== Done ===" -ForegroundColor Cyan
