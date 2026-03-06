# sync-fork.ps1 — Sync fork with Rance9/Insight-Harbor, optionally commit + create PR
# Usage: Right-click → "Run in Terminal"  or  Ctrl+Shift+` then: .\scripts\sync-fork.ps1

# Navigate to the repo root (parent of scripts/ folder)
Set-Location (Split-Path $PSScriptRoot -Parent)
if (-not (Test-Path ".git")) {
    Set-Location $PSScriptRoot
    if (-not (Test-Path "..\.git")) {
        Write-Host "ERROR: Could not find the git repository. Run this script from inside the Insight Harbor folder." -ForegroundColor Red
        exit 1
    }
    Set-Location ..
}
Write-Host "Working directory: $(Get-Location)" -ForegroundColor Gray

# ─────────────────────────────────────────────────────────────
# PHASE 1: Ensure GitHub CLI is installed and authenticated
# ─────────────────────────────────────────────────────────────

Write-Host "`n=== Insight Harbor — Sync & PR Script ===" -ForegroundColor Cyan

Write-Host "`n[Setup] Checking for GitHub CLI..." -ForegroundColor Green

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    Write-Host "GitHub CLI (gh) not found. Installing via winget..." -ForegroundColor Yellow
    winget install GitHub.cli --accept-source-agreements --accept-package-agreements
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed to install GitHub CLI. Please install manually: https://cli.github.com" -ForegroundColor Red
        exit 1
    }
    # Refresh PATH so gh is available in this session
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")
    if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
        Write-Host "GitHub CLI installed but not yet on PATH. Close and reopen your terminal, then re-run this script." -ForegroundColor Yellow
        exit 1
    }
    Write-Host "GitHub CLI installed successfully." -ForegroundColor Green
}

# Check authentication status and detect active account
$authStatus = gh auth status 2>&1
Write-Host $authStatus

if ($LASTEXITCODE -ne 0) {
    Write-Host "`nGitHub CLI is not authenticated. Starting login..." -ForegroundColor Yellow
    gh auth login
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Authentication failed. Please run 'gh auth login' manually and re-run this script." -ForegroundColor Red
        exit 1
    }
}

# Detect currently logged-in account
$currentUser = (gh api user --jq '.login') 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "`nCurrently authenticated as: " -NoNewline -ForegroundColor Green
    Write-Host "$currentUser" -ForegroundColor White
    $switchAcct = Read-Host "Use this account? (Y/n)"
    if ($switchAcct -in @("n", "N", "no", "No", "NO")) {
        Write-Host "`nSwitching accounts. Follow the prompts to log in with a different account..." -ForegroundColor Yellow
        gh auth logout
        gh auth login
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Authentication failed. Please run 'gh auth login' manually and re-run this script." -ForegroundColor Red
            exit 1
        }
        $currentUser = (gh api user --jq '.login') 2>&1
        Write-Host "Now authenticated as: $currentUser" -ForegroundColor Green
    }
} else {
    Write-Host "Could not detect GitHub account. Continuing anyway..." -ForegroundColor Yellow
}

Write-Host "[Setup] GitHub CLI ready.`n" -ForegroundColor Green

# ─────────────────────────────────────────────────────────────
# PHASE 2: Sync with upstream (Rance9 main repo)
# ─────────────────────────────────────────────────────────────

Write-Host "[Sync] Fetching upstream (Rance9/Insight-Harbor)..." -ForegroundColor Green
git fetch upstream

Write-Host "[Sync] Rebasing on upstream/main..." -ForegroundColor Green
git rebase upstream/main
if ($LASTEXITCODE -ne 0) {
    Write-Host "`nRebase conflict detected. Resolve conflicts, then run:" -ForegroundColor Red
    Write-Host "  git rebase --continue" -ForegroundColor Red
    Write-Host "Then re-run this script." -ForegroundColor Red
    exit 1
}

Write-Host "[Sync] Local repo is up to date with upstream.`n" -ForegroundColor Green

# ─────────────────────────────────────────────────────────────
# PHASE 3: Check for local changes and ask user what to do
# ─────────────────────────────────────────────────────────────

$status = git status --porcelain
if (-not $status) {
    Write-Host "No local file changes detected. Your repo is fully synced with upstream — nothing to commit." -ForegroundColor Yellow
    Write-Host "`n=== Done ==="  -ForegroundColor Cyan
    exit 0
}

Write-Host "Local changes detected:" -ForegroundColor Yellow
git status --short
Write-Host ""

$proceed = Read-Host "Would you like to commit these changes and create a PR? (y/N)"
if ($proceed -notin @("y", "Y", "yes", "Yes", "YES")) {
    Write-Host "`nSync complete. Changes left uncommitted." -ForegroundColor Yellow
    Write-Host "`n=== Done ===" -ForegroundColor Cyan
    exit 0
}

# ─────────────────────────────────────────────────────────────
# PHASE 4: Gather commit message, PR title, and PR description
# ─────────────────────────────────────────────────────────────

Write-Host ""
$commitMsg = Read-Host "Commit message (press Enter for default: 'Insight-Harbor-v1.0.0')"
if ([string]::IsNullOrWhiteSpace($commitMsg)) { $commitMsg = "Insight-Harbor-v1.0.0" }

$prTitle = Read-Host "PR title (press Enter for default: 'Fork PR')"
if ([string]::IsNullOrWhiteSpace($prTitle)) { $prTitle = "Fork PR" }

$prDesc = Read-Host "PR description (press Enter for default: 'Repo changes submitted from fork.')"
if ([string]::IsNullOrWhiteSpace($prDesc)) { $prDesc = "Repo changes submitted from fork." }

# ─────────────────────────────────────────────────────────────
# PHASE 5: Commit and push
# ─────────────────────────────────────────────────────────────

Write-Host "`n[Commit] Staging all changes..." -ForegroundColor Green
git add -A

Write-Host "[Commit] Committing with message: '$commitMsg'" -ForegroundColor Green
git commit -m $commitMsg

Write-Host "[Push] Pushing to fork..." -ForegroundColor Green
git push origin main --force-with-lease

# ─────────────────────────────────────────────────────────────
# PHASE 6: Create PR via GitHub CLI
# ─────────────────────────────────────────────────────────────

Write-Host "[PR] Creating Pull Request..." -ForegroundColor Green
Write-Host "  Title: $prTitle" -ForegroundColor Gray
Write-Host "  Description: $prDesc" -ForegroundColor Gray

gh pr create `
    --repo Rance9/Insight-Harbor `
    --base main `
    --head main `
    --title $prTitle `
    --body $prDesc

if ($LASTEXITCODE -eq 0) {
    Write-Host "`nPR created successfully! Waiting for review/merge on Rance9/Insight-Harbor." -ForegroundColor Green
} else {
    Write-Host "`nPR creation may have failed. You might already have an open PR — check GitHub." -ForegroundColor Yellow
}

Write-Host "`n=== Done ===" -ForegroundColor Cyan
