<#
.SYNOPSIS
Re-signs PowerShell files in the repository with the local fio-sql-bench code-signing certificate.

.DESCRIPTION
Looks up the certificate thumbprint stored under .local\codesigning\thumbprint.txt,
finds the matching certificate with a private key in Cert:\CurrentUser\My, and applies
an Authenticode signature to repo PowerShell files.

.PARAMETER Thumbprint
Optional certificate thumbprint override. If omitted, the helper reads
.local\codesigning\thumbprint.txt relative to the repository root.

.PARAMETER RepoRoot
Optional repository root override. Defaults to the parent of the script directory.

.PARAMETER PassThru
Emits the signature results as objects in addition to console output.

.EXAMPLE
.\scripts\Sign-FioSqlBenchScripts.ps1
#>
[CmdletBinding()]
param(
    [string]$Thumbprint,
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path,
    [switch]$PassThru
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-FioSigningThumbprint {
    param(
        [Parameter(Mandatory)]
        [string]$RepositoryRoot,

        [string]$OverrideThumbprint
    )

    if (-not [string]::IsNullOrWhiteSpace($OverrideThumbprint)) {
        return ([string]$OverrideThumbprint).Trim().Replace(' ', '')
    }

    $thumbprintPath = Join-Path $RepositoryRoot '.local\codesigning\thumbprint.txt'
    if (-not (Test-Path -LiteralPath $thumbprintPath)) {
        throw "Signing thumbprint file '$thumbprintPath' was not found."
    }

    $storedThumbprint = [string](Get-Content -LiteralPath $thumbprintPath -Raw)
    $storedThumbprint = $storedThumbprint.Trim().Replace(' ', '')
    if ([string]::IsNullOrWhiteSpace($storedThumbprint)) {
        throw "Signing thumbprint file '$thumbprintPath' was empty."
    }

    return $storedThumbprint
}

function Get-FioSigningCertificate {
    param(
        [Parameter(Mandatory)]
        [string]$ResolvedThumbprint
    )

    $certificate = Get-ChildItem -Path 'Cert:\CurrentUser\My' |
        Where-Object { $_.Thumbprint -eq $ResolvedThumbprint -and $_.HasPrivateKey } |
        Select-Object -First 1

    if ($null -eq $certificate) {
        throw "No code-signing certificate with thumbprint '$ResolvedThumbprint' and a private key was found in Cert:\CurrentUser\My."
    }

    return $certificate
}

function Get-FioPowerShellFilesToSign {
    param(
        [Parameter(Mandatory)]
        [string]$RepositoryRoot
    )

    $excludedPrefixes = @(
        [System.IO.Path]::GetFullPath((Join-Path $RepositoryRoot '.git')),
        [System.IO.Path]::GetFullPath((Join-Path $RepositoryRoot '.local')),
        [System.IO.Path]::GetFullPath((Join-Path $RepositoryRoot 'results')),
        [System.IO.Path]::GetFullPath((Join-Path $RepositoryRoot 'scratch')),
        [System.IO.Path]::GetFullPath((Join-Path $RepositoryRoot 'tmp')),
        [System.IO.Path]::GetFullPath((Join-Path $RepositoryRoot 'tools\fio'))
    )

    $files = Get-ChildItem -Path $RepositoryRoot -Recurse -File -Include *.ps1,*.psm1,*.psd1 |
        Where-Object {
            $fullPath = [System.IO.Path]::GetFullPath($_.FullName)
            foreach ($prefix in $excludedPrefixes) {
                if ($fullPath.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
                    return $false
                }
            }

            return $true
        } |
        Sort-Object FullName

    return @($files)
}

$resolvedRepoRoot = [System.IO.Path]::GetFullPath($RepoRoot)
$resolvedThumbprint = Get-FioSigningThumbprint -RepositoryRoot $resolvedRepoRoot -OverrideThumbprint $Thumbprint
$certificate = Get-FioSigningCertificate -ResolvedThumbprint $resolvedThumbprint
$filesToSign = @(Get-FioPowerShellFilesToSign -RepositoryRoot $resolvedRepoRoot)

if ($filesToSign.Count -eq 0) {
    throw "No PowerShell files were found to sign under '$resolvedRepoRoot'."
}

Write-Host ('Signing {0} PowerShell file(s) with {1}' -f $filesToSign.Count, $certificate.Subject) -ForegroundColor Cyan

$results = foreach ($file in $filesToSign) {
    $signature = Set-AuthenticodeSignature -FilePath $file.FullName -Certificate $certificate -HashAlgorithm SHA256
    [pscustomobject]@{
        Path = $file.FullName
        Status = $signature.Status.ToString()
        StatusMessage = $signature.StatusMessage
    }
}

$invalidResults = @($results | Where-Object { $_.Status -ne 'Valid' })

foreach ($result in $results) {
    $color = if ($result.Status -eq 'Valid') { 'Green' } else { 'Yellow' }
    Write-Host ('[{0}] {1}' -f $result.Status, $result.Path) -ForegroundColor $color
}

if ($invalidResults.Count -gt 0) {
    throw ('One or more files did not verify as Valid after signing: {0}' -f (($invalidResults | ForEach-Object { $_.Path }) -join ', '))
}

if ($PassThru) {
    $results
}
# SIG # Begin signature block
# MIIHRQYJKoZIhvcNAQcCoIIHNjCCBzICAQExDzANBglghkgBZQMEAgEFADB5Bgor
# BgEEAYI3AgEEoGswaTA0BgorBgEEAYI3AgEeMCYCAwEAAAQQH8w7YFlLCE63JNLG
# KX7zUQIBAAIBAAIBAAIBAAIBADAxMA0GCWCGSAFlAwQCAQUABCB2tjofx3QT1q2S
# JTfYVM5rvqZccUkaDyJzP5onToGt8aCCBCowggQmMIICjqADAgECAhA0c4FfL4g3
# sEi8dFSS4VmYMA0GCSqGSIb3DQEBCwUAMCsxKTAnBgNVBAMMIGZpby1zcWwtYmVu
# Y2ggTG9jYWwgQ29kZSBTaWduaW5nMB4XDTI2MDQxNzE4NTIxM1oXDTI5MDQxNzE5
# MDIxMFowKzEpMCcGA1UEAwwgZmlvLXNxbC1iZW5jaCBMb2NhbCBDb2RlIFNpZ25p
# bmcwggGiMA0GCSqGSIb3DQEBAQUAA4IBjwAwggGKAoIBgQCT6tn6JPXQIpYAAjyj
# 0UXsZz8x4rdmQkxe0EHEaPDCe1GxBmCyxWrKYFZLjTp8P+wDF9wYD1IiVqTy8ng5
# BkQFN7iRM8PIW+r/4cmbsN/qdTMF8ndmUxEwH1Aq05LiRb8w0sdjWE8IiCcYiIdw
# wXCvvHQ835UOJeEEFv6+bwoq7w+OsgOm9i1Tk31oxymCHWH4byw5bb3y71YPHWeV
# 36yDNQsD6xrixmmT/iH8tsRNOU7tyTx+wMSuak6JhQxeOtEg5vw9g7zpGpO2D3q7
# gAigzpAOV2ZaiSRt0uzmlwhOqpRk4htV/NE++01qnIt07sw3sTSrcflIMsmgyGHf
# yhnaNIbqvfj4rXM/iMPktlEcV6QqZlLlpk0FuXDF30NvmVCviV9BfX3rUHxOunjz
# vTr64+voOk9vVROuUuotwEMWxm3HhXghdJuXx0441k+Tbx47cBt3EvVa0Ai9tGC9
# H0wX4qBJSPngtwTjpxCUoNy/2Hu3TX13d+X2l61DuyxH0pUCAwEAAaNGMEQwDgYD
# VR0PAQH/BAQDAgeAMBMGA1UdJQQMMAoGCCsGAQUFBwMDMB0GA1UdDgQWBBSaKobD
# KUiFgxuLD/kwn4hfxO/IRDANBgkqhkiG9w0BAQsFAAOCAYEAPCk1dbvqrmd6UKsU
# /Gihil84uSHtJlmS9hrcaohguidkAlV5kli1tNBCf7qjyy6Sd8ncPbU0W5aXCF+t
# 7bVhaJHdmzLg32CsLrjDR5ELMUHGT1oJTd5a2GsQyzYozjMuaeLJYWtaFGr3diEV
# RqusvETU1jnjoKMNzH0f5MfGD3dIDcR60pMcLHdsPf4rpfo+7Xy46Fuxmy6FISdm
# u7/pqFIyfRUGFHDfI4VEk9IdrDbtEhwA6PZsxcg3EZtCU7RFYjWCigjx909I/V50
# vvnei2vDNWBrudfALzlmGfkImNH1Qr0YB+73/H3i7dT46F/51OWo/Mp6l33DwnIM
# VgBGRQ241ZLxjdrF9nWCr2YF+DrNuY5TPCGAgJ2os+KLGQTUwnOel+O0H5ZEgTcX
# 4vHEeeiDFQCsALME+AGkvn1FjoP45Afo6i6ekkF/Uu0v6cHzLrZKOKhdTvjp1ICN
# rcvutsFPyH8+zfuQ52bzadVEi4tTfppQaiVT1/D80ED8OQGgMYICcTCCAm0CAQEw
# PzArMSkwJwYDVQQDDCBmaW8tc3FsLWJlbmNoIExvY2FsIENvZGUgU2lnbmluZwIQ
# NHOBXy+IN7BIvHRUkuFZmDANBglghkgBZQMEAgEFAKCBhDAYBgorBgEEAYI3AgEM
# MQowCKACgAChAoAAMBkGCSqGSIb3DQEJAzEMBgorBgEEAYI3AgEEMBwGCisGAQQB
# gjcCAQsxDjAMBgorBgEEAYI3AgEVMC8GCSqGSIb3DQEJBDEiBCDpp3ymCypAc646
# Wx/pQ+cUTkW0cUYdygMIJbIoEAtoqDANBgkqhkiG9w0BAQEFAASCAYAb566gD13/
# r/41m5jiFk4KFUJMekowL4/9fYxEjh90x+2EJOayIakuT4+Qg1WKPSxLp598myCH
# cya6lKlxPF0CY4uKfY5nz9EeKb4nUgN0r+/2Tf7rA0e7m8br+c77ALTzjOE1VEsj
# lcirVBQHEdcBNshux6fg2blVNht6a7p2faSpJwIQxTcpP/r6K6jtpqA6A635FkqB
# Igzc8A28jGxMi/I02HGXDtxCPYkXpZPc+uJfr7k3RePOY5YwIky95uqNNhlxElOV
# c6lm5NE0DlautFeu2Gi063O3xUG2xwIIraud70Qgcatd2yrd0nUlqoAScw17Kt38
# pqz7KJfDV00riUCd7fd8icuS4A1XbsHNyv/Grocve/Cy7OVYVnME2xDIk5fzgYAz
# mlL45qUeqlFcul0KDLuAo2DXco1+STcqUf2tbJ3Ssh5BI6byUNA1Yyuiqp+/z75C
# RZVqy81HEJRzW0fjO3t+vJUOwyjbRSYRD7B0e6phCgGEeq6fS+LAw5k=
# SIG # End signature block
