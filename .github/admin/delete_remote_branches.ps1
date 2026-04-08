# Settings
$DryRun     = $true     # set to $false to actually delete
$DaysOld    = 10        # set to number of days since last commit
$Threshold  = (Get-Date).AddDays(-$DaysOld)

# Fetch the latest remote branches
git fetch --prune

# Get "origin/<branch>|<last-commit-date-iso>"
$refs = git for-each-ref --format="%(refname:short)|%(committerdate:iso8601)" refs/remotes/origin

foreach ($ref in $refs) {
    if (-not $ref) { continue }

    $parts = $ref -split '\|', 2
    $fullRef   = $parts[0]          # e.g., origin/feature/foo
    $dateIso   = $parts[1]          # e.g., 2025-10-10 12:34:56 +0000

    # Skip HEAD pointer and any malformed lines
    if ($fullRef -eq 'origin/HEAD' -or -not $dateIso) { continue }

    $branch    = $fullRef -replace '^origin/', ''
    $commitDt  = [datetime]::Parse($dateIso)

    # Keep protected branches
    if ($branch -eq 'main' -or $branch -like 'admin*') { continue }

    # Only delete branches with no commits in the last 14 days
    if ($commitDt -lt $Threshold) {
        if ($DryRun) {
            Write-Host "[DRY RUN] Would delete remote branch: $branch (last commit: $commitDt)"
        } else {
            Write-Host "Deleting remote branch: $branch (last commit: $commitDt)"
            git push origin --delete $branch
        }
    }
}

Write-Host "Remote branch cleanup completed! $(if ($DryRun) { '(dry run)' })"
