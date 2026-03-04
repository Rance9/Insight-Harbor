<#
.SYNOPSIS
    Insight Harbor — Synthetic Entra Users Generator
    Produces a fake PAX Entra CSV (`EntraUsers_MAClicensing_*.csv`) for pipeline testing.

.DESCRIPTION
    Generates a CSV that matches the exact column layout produced by the PAX
    `-OnlyUserInfo` switch, including all 37 base columns plus the 10 tenant-specific
    extension columns observed in real production output.

    CRITICAL: When run with the same Seed and UserCount as generate-synthetic.ps1,
    the generated UPNs will exactly match the UserId values in the synthetic Purview
    data — enabling the Entra → Usage JOIN to work correctly.

    Use this to:
        • Test the bronze_to_silver_entra.py transform
        • Validate the Entra ↔ Usage JOIN before real tenant data
        • Demo department/region dashboard views
        • Regression-test the pipeline

.PARAMETER OutputPath
    Where to write the generated CSV. Default: ingestion\output\EntraUsers_MAClicensing_<timestamp>.csv

.PARAMETER UserCount
    Number of users to generate (must match Purview synthetic UserCount). Default: 50.

.PARAMETER Seed
    Random seed for reproducible output. Use 42 to match the default Purview synthetic data.

.PARAMETER ExtraUnlicensedUsers
    Additional unlicensed/disabled users to add (simulates the gap between total org
    headcount and licensed Copilot users). Default: 15.

.EXAMPLE
    .\scripts\generate-synthetic-entra.ps1 -Seed 42
    # 50 licensed users + 15 unlicensed — matches synthetic Purview data

.EXAMPLE
    .\scripts\generate-synthetic-entra.ps1 -UserCount 50 -Seed 42 -ExtraUnlicensedUsers 25
    # 50 licensed + 25 unlicensed for Adoption Rate testing
#>

[CmdletBinding()]
param(
    [string] $OutputPath            = '',
    [int]    $UserCount             = 50,
    [int]    $Seed                  = -1,
    [int]    $ExtraUnlicensedUsers  = 15
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ─────────────────────────────────────────────────────────────────────────────
# Setup
# ─────────────────────────────────────────────────────────────────────────────
$rng = if ($Seed -ge 0) { [System.Random]::new($Seed) } else { [System.Random]::new() }

Push-Location (Split-Path -Parent $PSScriptRoot)

if ($OutputPath -eq '') {
    $ts         = Get-Date -Format 'yyyyMMdd_HHmmss'
    $OutputPath = "ingestion\output\EntraUsers_MAClicensing_$ts.csv"
}
$outputDir = Split-Path -Parent $OutputPath
if (-not (Test-Path $outputDir)) { New-Item -ItemType Directory -Path $outputDir -Force | Out-Null }

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║   Insight Harbor — Synthetic Entra Users Generator   ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host "  Licensed users   : $UserCount"
Write-Host "  Unlicensed users : $ExtraUnlicensedUsers"
Write-Host "  Total users      : $($UserCount + $ExtraUnlicensedUsers)"
Write-Host "  Output           : $OutputPath"
Write-Host "  Seed             : $(if ($Seed -ge 0) { $Seed } else { 'random' })"
Write-Host ""

# ─────────────────────────────────────────────────────────────────────────────
# Shared data pools  (MUST MATCH generate-synthetic.ps1 exactly for UPN alignment)
# ─────────────────────────────────────────────────────────────────────────────
$firstNames = @('Alex','Jordan','Morgan','Taylor','Casey','Riley','Drew','Parker',
                'Sam','Avery','Quinn','Blair','Reese','Logan','Skyler','Finley',
                'Robin','Sage','Hayden','Peyton','Cameron','Dakota','Emerson','Harley','Indigo')
$lastNames  = @('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
                'Wilson','Taylor','Anderson','Thomas','Jackson','White','Harris','Martin',
                'Thompson','Young','Allen','King','Wright','Scott','Torres','Hill','Green')
$domains    = @('contoso.com', 'fabrikam.onmicrosoft.com', 'woodgrove.com')

# Entra-specific data pools
$departments = @('Engineering','Sales','Marketing','Finance','HR','IT','Legal','Operations')
$countries   = @('US','GB','CA','AU','DE','FR','JP','IN')
$countryFull = @{
    'US' = 'United States'; 'GB' = 'United Kingdom'; 'CA' = 'Canada'; 'AU' = 'Australia'
    'DE' = 'Germany';       'FR' = 'France';         'JP' = 'Japan';  'IN' = 'India'
}
$cities = @{
    'US' = @('Seattle','New York','Chicago','San Francisco','Austin')
    'GB' = @('London','Manchester','Edinburgh')
    'CA' = @('Toronto','Vancouver','Montreal')
    'AU' = @('Sydney','Melbourne')
    'DE' = @('Berlin','Munich','Frankfurt')
    'FR' = @('Paris','Lyon','Marseille')
    'JP' = @('Tokyo','Osaka')
    'IN' = @('Bangalore','Mumbai','Hyderabad')
}
$states = @{
    'US' = @('WA','NY','IL','CA','TX'); 'GB' = @('England','Scotland')
    'CA' = @('ON','BC','QC');           'AU' = @('NSW','VIC')
    'DE' = @('Berlin','Bavaria','Hesse'); 'FR' = @('Île-de-France','Auvergne-Rhône-Alpes','Provence-Alpes-Côte d''Azur')
    'JP' = @('Tokyo','Osaka');          'IN' = @('Karnataka','Maharashtra','Telangana')
}
$offices = @('HQ Campus','Building A','Building B','Downtown Office','Remote')
$divisions = @('Technology','Revenue','Operations','Corporate','Product')
$costCenters = @('TECH-ENG','TECH-IT','REV-SALES','REV-MKT','OPS-HR','OPS-FIN','CORP-LEGAL','OPS-OPS')
$employeeTypes = @('Full-Time','Full-Time','Full-Time','Full-Time','Contractor','Intern')
$companyNames  = @('Contoso Corporation','Fabrikam Inc','Woodgrove Bank')
$jobTitlesByDept = @{
    'Engineering' = @('Software Engineer','Senior Software Engineer','Principal Engineer','Engineering Manager','DevOps Engineer','QA Engineer')
    'Sales'       = @('Account Executive','Sales Manager','Sales Director','Business Development Rep','Solutions Architect')
    'Marketing'   = @('Marketing Manager','Content Strategist','Digital Marketing Specialist','Brand Manager','Marketing Analyst')
    'Finance'     = @('Financial Analyst','Accounting Manager','Controller','FP&A Analyst','Treasury Analyst')
    'HR'          = @('HR Business Partner','Recruiter','Talent Acquisition Manager','HR Generalist','Compensation Analyst')
    'IT'          = @('IT Administrator','Systems Engineer','Security Analyst','Help Desk Specialist','Network Engineer')
    'Legal'       = @('Corporate Counsel','Paralegal','Compliance Officer','Contract Manager','Legal Analyst')
    'Operations'  = @('Operations Manager','Project Manager','Business Analyst','Process Improvement Specialist','Facilities Manager')
}
$managerTitles = @('Senior Manager','Director','VP','Group Manager','Team Lead')

$licenseSKUs = @(
    'SPE_E5',                           # M365 E5  (most common in demo)
    'SPE_E5;Microsoft_365_Copilot',     # E5 + Copilot add-on
    'SPE_E3',                           # M365 E3
    'SPE_E3;Microsoft_365_Copilot',     # E3 + Copilot add-on
    'ENTERPRISEPREMIUM',                # Office 365 E5
    'SPE_F1'                            # Frontline
)
# Weights: 30% E5-only, 25% E5+Copilot, 15% E3-only, 15% E3+Copilot, 10% O365E5, 5% F1
$licenseWeights = @(30, 25, 15, 15, 10, 5)
$licenseCumulative = @()
$cumSum = 0
foreach ($w in $licenseWeights) { $cumSum += $w; $licenseCumulative += $cumSum }

# Separate RNG for Entra-specific fields (so main $rng stays aligned with Purview generator)
$detailSeed = if ($Seed -ge 0) { $Seed + 1000 } else { 12345 }
$detailRng  = [System.Random]::new($detailSeed)

function Get-WeightedLicense {
    $roll = $detailRng.Next(100)
    for ($i = 0; $i -lt $licenseCumulative.Count; $i++) {
        if ($roll -lt $licenseCumulative[$i]) { return $licenseSKUs[$i] }
    }
    return $licenseSKUs[0]
}

# ─────────────────────────────────────────────────────────────────────────────
# Generate users  (same RNG sequence as generate-synthetic.ps1 for first N users)
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "[1/3] Generating $UserCount licensed users (same seed as Purview data)..." -ForegroundColor Green

# Step 1: Reproduce the EXACT same user identity generation as generate-synthetic.ps1
# This consumes the same RNG draws: firstName, lastName, domain, department, country
# CRITICAL: All other fields use a SEPARATE RNG so we don't shift the main RNG state
$licensedUsers = [System.Collections.Generic.List[object]]::new()
$seenUPNs      = @{}

for ($i = 0; $i -lt $UserCount; $i++) {
    # These 5 RNG draws must match generate-synthetic.ps1 exactly (same order, same pools)
    $fn   = $firstNames[$rng.Next($firstNames.Count)]
    $ln   = $lastNames[$rng.Next($lastNames.Count)]
    $dom  = $domains[$rng.Next($domains.Count)]
    $dept = $departments[$rng.Next($departments.Count)]
    $cc   = $countries[$rng.Next($countries.Count)]

    $upn  = "$($fn.ToLower()).$($ln.ToLower())@$dom"

    # ALL remaining fields use $detailRng (not $rng) to avoid shifting the main sequence
    $oid  = [System.Guid]::NewGuid().ToString()
    $city = $cities[$cc][$detailRng.Next($cities[$cc].Count)]
    $st   = $states[$cc][$detailRng.Next($states[$cc].Count)]
    $office   = $offices[$detailRng.Next($offices.Count)]
    $empType  = $employeeTypes[$detailRng.Next($employeeTypes.Count)]
    $empId    = "E{0:D5}" -f ($detailRng.Next(10000, 99999))
    $div      = $divisions[$detailRng.Next($divisions.Count)]
    $cc2      = $costCenters[$detailRng.Next($costCenters.Count)]
    $company  = $companyNames[$detailRng.Next($companyNames.Count)]
    $jt       = ($jobTitlesByDept[$dept])[$detailRng.Next(($jobTitlesByDept[$dept]).Count)]
    $license  = Get-WeightedLicense

    # Manager (synthetic — just a plausible name, uses detailRng)
    $mgrFn  = $firstNames[$detailRng.Next($firstNames.Count)]
    $mgrLn  = $lastNames[$detailRng.Next($lastNames.Count)]
    $mgrOid = [System.Guid]::NewGuid().ToString()
    $mgrUPN = "$($mgrFn.ToLower()).$($mgrLn.ToLower())@$dom"
    $mgrTitle = $managerTitles[$detailRng.Next($managerTitles.Count)]

    # Hire date: random within last 5 years (uses detailRng)
    $hireDaysAgo = $detailRng.Next(30, 1825)
    $hireDate    = [datetime]::UtcNow.AddDays(-$hireDaysAgo).ToString('yyyy-MM-ddTHH:mm:ssZ')

    # Created date: a few days before hire
    $createdDate = [datetime]::UtcNow.AddDays(-$hireDaysAgo - $detailRng.Next(1, 14)).ToString('M/d/yyyy h:mm:ss tt')

    $licensedUsers.Add([PSCustomObject]@{
        userPrincipalName          = $upn
        DisplayName                = "$fn $ln"
        id                         = $oid
        Email                      = $upn
        givenName                  = $fn
        surname                    = $ln
        JobTitle                   = $jt
        department                 = $dept
        employeeType               = $empType
        employeeId                 = $empId
        employeeHireDate           = $hireDate
        officeLocation             = $office
        city                       = $city
        state                      = $st
        Country                    = $countryFull[$cc]
        postalCode                 = "{0:D5}" -f $detailRng.Next(10000, 99999)
        companyName                = $company
        employeeOrgData_division   = $div
        employeeOrgData_costCenter = $cc2
        accountEnabled             = 'True'
        userType                   = 'Member'
        createdDateTime            = $createdDate
        usageLocation              = $cc
        preferredLanguage          = switch ($cc) { 'US' {'en-US'} 'GB' {'en-GB'} 'CA' {'en-CA'} 'AU' {'en-AU'} 'DE' {'de-DE'} 'FR' {'fr-FR'} 'JP' {'ja-JP'} 'IN' {'en-IN'} default {'en-US'} }
        onPremisesSyncEnabled      = (@('True','False'))[$detailRng.Next(2)]
        onPremisesImmutableId      = if ($detailRng.Next(2) -eq 0) { [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($oid.Substring(0,16))) } else { '' }
        externalUserState          = ''
        proxyAddresses_Primary     = "SMTP:$upn"
        proxyAddresses_Count       = '1'
        proxyAddresses_All         = "SMTP:$upn"
        manager_id                 = $mgrOid
        manager_displayName        = "$mgrFn $mgrLn"
        manager_userPrincipalName  = $mgrUPN
        manager_mail               = $mgrUPN
        manager_jobTitle           = $mgrTitle
        ManagerID                  = $mgrOid
        BusinessAreaLabel          = $div
        CountryofEmployment        = $countryFull[$cc]
        CompanyCodeLabel           = $company
        CostCentreLabel            = $cc2
        UserName                   = "$fn $ln"
        EffectiveDate              = ''
        FunctionType               = ''
        BusinessAreaCode           = ''
        OrgLevel_3Label            = ''
        assignedLicenses           = $license
        HasLicense                 = 'True'
    })

    if (-not $seenUPNs.ContainsKey($upn)) { $seenUPNs[$upn] = $true }
}

# ─────────────────────────────────────────────────────────────────────────────
# Generate extra unlicensed / disabled users
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "[2/3] Generating $ExtraUnlicensedUsers unlicensed/disabled users..." -ForegroundColor Green

$extraRng = [System.Random]::new(999)  # Separate RNG so it doesn't affect deterministic users

for ($i = 0; $i -lt $ExtraUnlicensedUsers; $i++) {
    $fn  = $firstNames[$extraRng.Next($firstNames.Count)]
    $ln  = $lastNames[$extraRng.Next($lastNames.Count)]
    $dom = $domains[$extraRng.Next($domains.Count)]
    $upn = "$($fn.ToLower()).$($ln.ToLower())@$dom"

    # Skip if UPN already exists
    if ($seenUPNs.ContainsKey($upn)) { $ExtraUnlicensedUsers++; continue }
    $seenUPNs[$upn] = $true

    $dept = $departments[$extraRng.Next($departments.Count)]
    $cc   = $countries[$extraRng.Next($countries.Count)]
    $enabled = if ($extraRng.Next(3) -eq 0) { 'False' } else { 'True' }
    $oid  = [System.Guid]::NewGuid().ToString()
    $city = $cities[$cc][$extraRng.Next($cities[$cc].Count)]
    $st   = $states[$cc][$extraRng.Next($states[$cc].Count)]
    $jt   = ($jobTitlesByDept[$dept])[$extraRng.Next(($jobTitlesByDept[$dept]).Count)]
    $hireDaysAgo = $extraRng.Next(30, 1825)
    $hireDate    = [datetime]::UtcNow.AddDays(-$hireDaysAgo).ToString('yyyy-MM-ddTHH:mm:ssZ')
    $createdDate = [datetime]::UtcNow.AddDays(-$hireDaysAgo - $extraRng.Next(1, 14)).ToString('M/d/yyyy h:mm:ss tt')

    $mgrFn  = $firstNames[$extraRng.Next($firstNames.Count)]
    $mgrLn  = $lastNames[$extraRng.Next($lastNames.Count)]
    $mgrOid = [System.Guid]::NewGuid().ToString()

    $licensedUsers.Add([PSCustomObject]@{
        userPrincipalName          = $upn
        DisplayName                = "$fn $ln"
        id                         = $oid
        Email                      = $upn
        givenName                  = $fn
        surname                    = $ln
        JobTitle                   = $jt
        department                 = $dept
        employeeType               = 'Full-Time'
        employeeId                 = "E{0:D5}" -f ($extraRng.Next(10000, 99999))
        employeeHireDate           = $hireDate
        officeLocation             = $offices[$extraRng.Next($offices.Count)]
        city                       = $city
        state                      = $st
        Country                    = $countryFull[$cc]
        postalCode                 = "{0:D5}" -f $extraRng.Next(10000, 99999)
        companyName                = $companyNames[$extraRng.Next($companyNames.Count)]
        employeeOrgData_division   = $divisions[$extraRng.Next($divisions.Count)]
        employeeOrgData_costCenter = $costCenters[$extraRng.Next($costCenters.Count)]
        accountEnabled             = $enabled
        userType                   = 'Member'
        createdDateTime            = $createdDate
        usageLocation              = $cc
        preferredLanguage          = 'en-US'
        onPremisesSyncEnabled      = 'False'
        onPremisesImmutableId      = ''
        externalUserState          = ''
        proxyAddresses_Primary     = "SMTP:$upn"
        proxyAddresses_Count       = '1'
        proxyAddresses_All         = "SMTP:$upn"
        manager_id                 = $mgrOid
        manager_displayName        = "$mgrFn $mgrLn"
        manager_userPrincipalName  = "$($mgrFn.ToLower()).$($mgrLn.ToLower())@$dom"
        manager_mail               = "$($mgrFn.ToLower()).$($mgrLn.ToLower())@$dom"
        manager_jobTitle           = $managerTitles[$extraRng.Next($managerTitles.Count)]
        ManagerID                  = $mgrOid
        BusinessAreaLabel          = $divisions[$extraRng.Next($divisions.Count)]
        CountryofEmployment        = $countryFull[$cc]
        CompanyCodeLabel           = $companyNames[$extraRng.Next($companyNames.Count)]
        CostCentreLabel            = $costCenters[$extraRng.Next($costCenters.Count)]
        UserName                   = "$fn $ln"
        EffectiveDate              = ''
        FunctionType               = ''
        BusinessAreaCode           = ''
        OrgLevel_3Label            = ''
        assignedLicenses           = ''
        HasLicense                 = 'False'
    })
}

# ─────────────────────────────────────────────────────────────────────────────
# Export
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "[3/3] Writing CSV..." -ForegroundColor Green

$licensedUsers | Export-Csv -Path $OutputPath -NoTypeInformation -Encoding UTF8

$totalUsers     = $licensedUsers.Count
$licensedCount  = ($licensedUsers | Where-Object { $_.HasLicense -eq 'True' }).Count
$copilotCount   = ($licensedUsers | Where-Object { $_.assignedLicenses -match 'Copilot' }).Count
$deptGroups     = $licensedUsers | Group-Object department | Sort-Object Count -Descending
$countryGroups  = $licensedUsers | Group-Object Country | Sort-Object Count -Descending

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║   Generation Complete                                ║" -ForegroundColor Green
Write-Host "╚══════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host "  Total users    : $totalUsers"
Write-Host "  Licensed       : $licensedCount"
Write-Host "  Copilot license: $copilotCount"
Write-Host "  Unlicensed     : $($totalUsers - $licensedCount)"
Write-Host "  Departments    : $($deptGroups.Count)"
foreach ($d in $deptGroups) { Write-Host "    $($d.Name): $($d.Count)" }
Write-Host "  Countries      : $($countryGroups.Count)"
foreach ($c in $countryGroups) { Write-Host "    $($c.Name): $($c.Count)" }
Write-Host "  Output         : $OutputPath"
Write-Host ""

Pop-Location
