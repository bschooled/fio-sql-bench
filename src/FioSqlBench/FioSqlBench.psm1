Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Remove-FioNullPadding {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [string]$Value
    )

    if ($null -eq $Value) {
        return $null
    }

    return $Value.Replace([string][char]0, '').Trim()
}

function Get-FioObjectPropertyValue {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$Object,

        [Parameter(Mandatory)]
        [string]$PropertyName
    )

    if ($null -eq $Object) {
        return $null
    }

    $property = $Object.PSObject.Properties[$PropertyName]
    if ($null -eq $property) {
        return $null
    }

    return $property.Value
}

function Get-FioMappedSmbInfo {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    if ($Path.StartsWith('\\')) {
        return $null
    }

    $root = [System.IO.Path]::GetPathRoot($Path)
    if ([string]::IsNullOrWhiteSpace($root) -or $root -notmatch '^[A-Za-z]:\\$') {
        return $null
    }

    $driveName = $root.Substring(0, 1)
    $psDrive = $null
    try {
        $psDrive = Get-PSDrive -Name $driveName -PSProvider FileSystem -ErrorAction Stop
    }
    catch {
        return $null
    }

    $displayRoot = Remove-FioNullPadding -Value $psDrive.DisplayRoot
    if ([string]::IsNullOrWhiteSpace($displayRoot) -or -not $displayRoot.StartsWith('\\')) {
        return $null
    }

    $remoteRoot = $displayRoot.TrimEnd('\\')
    $relativePath = $Path.Substring($root.Length).TrimStart('\\')
    $remotePath = if ([string]::IsNullOrWhiteSpace($relativePath)) {
        $remoteRoot
    }
    else {
        Join-Path -Path $remoteRoot -ChildPath $relativePath
    }

    [pscustomobject]@{
        DriveName = $driveName
        DriveRoot = $root.TrimEnd('\\')
        RemoteRoot = $remoteRoot
        RemotePath = $remotePath
    }
}

function Resolve-FioSqlBenchTarget {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$TargetPath,

        [ValidateSet('Auto', 'Local', 'Smb')]
        [string]$TargetType = 'Auto'
    )

    if ($TargetPath -match '^(\\\\[.?]\\|\\\\[.]\\)') {
        throw 'Raw device paths are not supported. Use a directory on a local volume or SMB share.'
    }

    $createdDirectory = $false
    if (-not (Test-Path -LiteralPath $TargetPath)) {
        # Treat a missing target as an instruction to create a dedicated benchmark
        # folder rather than failing early on a first run.
        New-Item -ItemType Directory -Path $TargetPath -Force | Out-Null
        $createdDirectory = $true
    }

    $item = Get-Item -LiteralPath $TargetPath -ErrorAction Stop
    if (-not $item.PSIsContainer) {
        throw "Target path '$TargetPath' must resolve to a directory."
    }

    $resolvedPath = $item.FullName
    $isUnc = $resolvedPath.StartsWith('\\')
    $mappedSmbInfo = Get-FioMappedSmbInfo -Path $resolvedPath
    $isMappedSmb = $null -ne $mappedSmbInfo

    $resolvedType = switch ($TargetType) {
        'Local' { 'Local' }
        'Smb' { 'Smb' }
        default { if ($isUnc -or $isMappedSmb) { 'Smb' } else { 'Local' } }
    }

    $freeSpaceBytes = $null
    if ($resolvedType -eq 'Local') {
        $root = [System.IO.Path]::GetPathRoot($resolvedPath)
        $drive = [System.IO.DriveInfo]::new($root)
        if ($drive.IsReady) {
            $freeSpaceBytes = [int64]$drive.AvailableFreeSpace
        }
    }

    [pscustomobject]@{
        Path = $resolvedPath
        Type = $resolvedType
        CreatedDirectory = $createdDirectory
        FreeSpaceBytes = $freeSpaceBytes
        IsUnc = $isUnc
        IsMappedSmb = $isMappedSmb
        SmbMetadata = if ($resolvedType -eq 'Smb') { Get-SmbTargetMetadata -Path $resolvedPath -MappedSmbInfo $mappedSmbInfo } else { $null }
    }
}

function Get-SmbTargetMetadata {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Path,

        [pscustomobject]$MappedSmbInfo
    )

    $parsePath = Remove-FioNullPadding -Value $(if ($null -ne $MappedSmbInfo) { $MappedSmbInfo.RemotePath } else { $Path })
    if (-not $parsePath.StartsWith('\\')) {
        return $null
    }

    $trimmed = $parsePath.TrimStart('\\')
    $segments = $trimmed.Split('\')
    if ($segments.Count -lt 2) {
        return $null
    }

    $server = $segments[0]
    $share = $segments[1]

    $connections = @()
    try {
        $connections = @(Get-SmbConnection -ErrorAction Stop | Where-Object {
            $_.ServerName -eq $server -and $_.ShareName -eq $share
        })
    }
    catch {
        $connections = @()
    }

    $connection = $connections | Select-Object -First 1
    $multichannelConnections = @()
    try {
        $multichannelConnections = @(Get-SmbMultichannelConnection -ErrorAction Stop | Where-Object {
            $_.ServerName -eq $server
        })
    }
    catch {
        $multichannelConnections = @()
    }

    $selectedChannels = @($multichannelConnections | Where-Object {
        $selected = Get-FioObjectPropertyValue -Object $_ -PropertyName 'Selected'
        $null -eq $selected -or [bool]$selected
    })

    $rdmaChannels = @($selectedChannels | Where-Object {
        $clientRdma = Get-FioObjectPropertyValue -Object $_ -PropertyName 'ClientRdmaCapable'
        if ($null -eq $clientRdma) {
            $clientRdma = Get-FioObjectPropertyValue -Object $_ -PropertyName 'ClientRDMA Capable'
        }

        $serverRdma = Get-FioObjectPropertyValue -Object $_ -PropertyName 'ServerRdmaCapable'
        if ($null -eq $serverRdma) {
            $serverRdma = Get-FioObjectPropertyValue -Object $_ -PropertyName 'ServerRDMA Capable'
        }

        [bool]$clientRdma -and [bool]$serverRdma
    })

    [pscustomobject]@{
        ServerName = $server
        ShareName = $share
        RemotePath = $parsePath
        MappedDrive = if ($null -ne $MappedSmbInfo) { $MappedSmbInfo.DriveRoot } else { $null }
        Credential = if ($connection) { $connection.UserName } else { $null }
        Dialect = if ($connection) { $connection.Dialect } else { $null }
        NumOpens = if ($connection) { $connection.NumOpens } else { $null }
        EncryptData = if ($connection) { Get-FioObjectPropertyValue -Object $connection -PropertyName 'EncryptData' } else { $null }
        ContinuouslyAvailable = if ($connection) { Get-FioObjectPropertyValue -Object $connection -PropertyName 'ContinuouslyAvailable' } else { $null }
        ConnectionCount = $connections.Count
        MultichannelPathCount = $selectedChannels.Count
        RdmaPathCount = $rdmaChannels.Count
    }
}

function Get-FioSqlBenchProfileDefaults {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [ValidateSet('Data', 'Log', 'Tempdb', 'BackupRestore', 'DbccScan')]
        [string]$Profile
    )

    switch ($Profile) {
        'Data' {
            return [ordered]@{
                ProfileName = 'Data'
                FileSizeGB = 32
                RuntimeSec = 60
                RampSec = 10
                Iterations = 1
                QueueDepth = 32
                NumJobs = 4
                BlockSize = '8k'
                ReadMix = 70
                Fsync = 0
                DirectLocal = 1
                DirectSmb = 1
                ReadWrite = 'randrw'
            }
        }
        'Log' {
            return [ordered]@{
                ProfileName = 'Log'
                FileSizeGB = 8
                RuntimeSec = 60
                RampSec = 10
                Iterations = 1
                QueueDepth = 1
                NumJobs = 1
                BlockSize = '64k'
                ReadMix = $null
                Fsync = 1
                DirectLocal = 1
                DirectSmb = 1
                ReadWrite = 'write'
            }
        }
        'Tempdb' {
            return [ordered]@{
                ProfileName = 'Tempdb'
                FileSizeGB = 16
                RuntimeSec = 60
                RampSec = 10
                Iterations = 1
                QueueDepth = 32
                NumJobs = 8
                BlockSize = '8k'
                ReadMix = 50
                Fsync = 0
                DirectLocal = 1
                DirectSmb = 1
                ReadWrite = 'randrw'
            }
        }
        'BackupRestore' {
            return [ordered]@{
                ProfileName = 'BackupRestore'
                FileSizeGB = 64
                RuntimeSec = 90
                RampSec = 10
                Iterations = 1
                QueueDepth = 8
                NumJobs = 2
                BlockSize = '1m'
                ReadMix = 50
                Fsync = 0
                DirectLocal = 1
                DirectSmb = 1
                ReadWrite = 'rw'
            }
        }
        'DbccScan' {
            return [ordered]@{
                ProfileName = 'DbccScan'
                FileSizeGB = 32
                RuntimeSec = 90
                RampSec = 10
                Iterations = 1
                QueueDepth = 8
                NumJobs = 2
                BlockSize = '256k'
                ReadMix = $null
                Fsync = 0
                DirectLocal = 1
                DirectSmb = 1
                ReadWrite = 'read'
            }
        }
    }
}

function Merge-FioSqlBenchSettings {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$TargetInfo,

        [Parameter(Mandatory)]
        [System.Collections.IDictionary]$ProfileDefaults,

        [Nullable[decimal]]$FileSizeGB,
        [Nullable[int]]$RuntimeSec,
        [Nullable[int]]$RampSec,
        [Nullable[int]]$Iterations,
        [Nullable[int]]$QueueDepth,
        [Nullable[int]]$NumJobs,
        [string]$BlockSize,
        [Nullable[int]]$ReadMix,
        [Nullable[int]]$Fsync,

        [ValidateSet('Auto', 'On', 'Off')]
        [string]$Direct = 'Auto'
    )

    $settings = [ordered]@{
        FileSizeGB = if ($null -ne $FileSizeGB) { [decimal]$FileSizeGB } else { [decimal]$ProfileDefaults.FileSizeGB }
        RuntimeSec = if ($null -ne $RuntimeSec) { [int]$RuntimeSec } else { [int]$ProfileDefaults.RuntimeSec }
        RampSec = if ($null -ne $RampSec) { [int]$RampSec } else { [int]$ProfileDefaults.RampSec }
        Iterations = if ($null -ne $Iterations) { [int]$Iterations } else { [int]$ProfileDefaults.Iterations }
        QueueDepth = if ($null -ne $QueueDepth) { [int]$QueueDepth } else { [int]$ProfileDefaults.QueueDepth }
        NumJobs = if ($null -ne $NumJobs) { [int]$NumJobs } else { [int]$ProfileDefaults.NumJobs }
        BlockSize = if ($BlockSize) { $BlockSize } else { [string]$ProfileDefaults.BlockSize }
        ReadMix = if ($null -ne $ReadMix) { [int]$ReadMix } else { $ProfileDefaults.ReadMix }
        Fsync = if ($null -ne $Fsync) { [int]$Fsync } else { [int]$ProfileDefaults.Fsync }
        ReadWrite = [string]$ProfileDefaults.ReadWrite
    }

    switch ($Direct) {
        'On' { $settings.Direct = 1 }
        'Off' { $settings.Direct = 0 }
        default {
            $settings.Direct = if ($TargetInfo.Type -eq 'Smb') { [int]$ProfileDefaults.DirectSmb } else { [int]$ProfileDefaults.DirectLocal }
        }
    }

    if ($settings.FileSizeGB -le 0) {
        throw 'FileSizeGB must be greater than 0.'
    }

    if ($settings.RuntimeSec -le 0) {
        throw 'RuntimeSec must be greater than 0.'
    }

    if ($settings.QueueDepth -le 0 -or $settings.NumJobs -le 0 -or $settings.Iterations -le 0) {
        throw 'QueueDepth, NumJobs, and Iterations must all be greater than 0.'
    }

    if ($null -ne $settings.ReadMix -and ($settings.ReadMix -lt 0 -or $settings.ReadMix -gt 100)) {
        throw 'ReadMix must be between 0 and 100.'
    }

    $blockSizeBytes = ConvertFrom-FioSizeStringToBytes -Size $settings.BlockSize
    $totalFileBytes = [int64][math]::Floor([decimal]1GB * $settings.FileSizeGB)
    if ($null -ne $TargetInfo.FreeSpaceBytes) {
        $requiredBytes = [int64]($totalFileBytes * 1.10)
        if ($requiredBytes -gt $TargetInfo.FreeSpaceBytes) {
            throw "Not enough free space on target volume. Required about $requiredBytes bytes, available $($TargetInfo.FreeSpaceBytes)."
        }
    }

    $settings.TotalFileBytes = $totalFileBytes
    $perJobBytes = [int64][math]::Floor($totalFileBytes / $settings.NumJobs)
    $minimumPerJobBytes = [int64][math]::Max(1MB, $blockSizeBytes)
    if ($perJobBytes -lt $minimumPerJobBytes) {
        $perJobBytes = $minimumPerJobBytes
    }

    $settings.BlockSizeBytes = $blockSizeBytes
    $settings.FileSizePerJobBytes = Get-FioAlignedByteCount -ByteCount $perJobBytes -Alignment $blockSizeBytes
    [pscustomobject]$settings
}

function ConvertFrom-FioSizeStringToBytes {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Size
    )

    $trimmed = $Size.Trim()
    $match = [regex]::Match($trimmed, '^(?<value>\d+(?:\.\d+)?)(?<unit>[kKmMgGtT]?)$')
    if (-not $match.Success) {
        throw "Unsupported fio size format '$Size'."
    }

    $value = [double]$match.Groups['value'].Value
    $multiplier = switch ($match.Groups['unit'].Value.ToLowerInvariant()) {
        'k' { 1KB }
        'm' { 1MB }
        'g' { 1GB }
        't' { 1TB }
        default { 1 }
    }

    [int64][math]::Round($value * $multiplier)
}

function Get-FioAlignedByteCount {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [int64]$ByteCount,

        [Parameter(Mandatory)]
        [int64]$Alignment
    )

    if ($Alignment -le 0) {
        throw 'Alignment must be greater than 0.'
    }

    $remainder = $ByteCount % $Alignment
    if ($remainder -eq 0) {
        return $ByteCount
    }

    [int64]($ByteCount - $remainder)
}

function New-FioSqlBenchRunContext {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$TargetInfo,

        [Parameter(Mandatory)]
        [string]$OutputRoot,

        [string]$RunLabel,

        [Parameter(Mandatory)]
        [string]$Profile,

        [Parameter(Mandatory)]
        [pscustomobject]$Settings
    )

    $timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $label = if ($RunLabel) { $RunLabel } else { $Profile.ToLowerInvariant() }
    $sanitizedLabel = ($label -replace '[^a-zA-Z0-9_-]', '-')
    $runId = "$timestamp-$sanitizedLabel"

    [pscustomobject]@{
        RunId = $runId
        ResultDirectory = Join-Path -Path $OutputRoot -ChildPath $runId
        TargetRunDirectory = Join-Path -Path $TargetInfo.Path -ChildPath $runId
        TimestampUtc = [DateTime]::UtcNow.ToString('o')
    }
}

function New-FioSqlBenchJobContent {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Settings,

        [Parameter(Mandatory)]
        [pscustomobject]$RunContext,

        [Parameter(Mandatory)]
        [ValidateSet('Prep', 'Bench', 'Combined')]
        [string]$Phase,

        [switch]$EnableLogs
    )

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add('[global]')
    $lines.Add('ioengine=windowsaio')
    $lines.Add('thread=1')
    $lines.Add('group_reporting=1')
    $lines.Add("direct=$($Settings.Direct)")
    $lines.Add("bs=$($Settings.BlockSize)")
    $lines.Add("filesize=$($Settings.FileSizePerJobBytes)")
    $lines.Add("size=$($Settings.FileSizePerJobBytes)")
    $lines.Add('norandommap=1')
    $lines.Add('randrepeat=0')
    $lines.Add('create_on_open=1')
    $lines.Add('invalidate=1')
    $lines.Add('refill_buffers=1')
    $lines.Add('percentile_list=50:95:99:99.9')

    if ($Phase -ne 'Prep') {
        $lines.Add('time_based=1')
        $lines.Add("runtime=$($Settings.RuntimeSec)")
        $lines.Add("ramp_time=$($Settings.RampSec)")
        $lines.Add("iodepth=$($Settings.QueueDepth)")
    }

    if ($EnableLogs -and $Phase -ne 'Prep') {
        $logBase = ConvertTo-FioJobPath -Path (Join-Path -Path $RunContext.ResultDirectory -ChildPath 'fio')
        $lines.Add("write_bw_log=$logBase")
        $lines.Add("write_lat_log=$logBase")
        $lines.Add('per_job_logs=1')
    }

    if ($Phase -in @('Prep', 'Combined')) {
        for ($jobIndex = 1; $jobIndex -le $Settings.NumJobs; $jobIndex++) {
            $filePath = ConvertTo-FioJobPath -Path (Join-Path -Path $RunContext.TargetRunDirectory -ChildPath ('testfile.{0:D2}.dat' -f $jobIndex))

            $lines.Add('')
            $lines.Add('[prep-{0:D2}]' -f $jobIndex)
            $lines.Add("filename=$filePath")
            $lines.Add('rw=write')
            $lines.Add("bs=$($Settings.BlockSize)")
            $lines.Add('iodepth=1')
            $lines.Add('time_based=0')
            $lines.Add('end_fsync=1')
        }
    }

    if ($Phase -in @('Bench', 'Combined')) {
        for ($jobIndex = 1; $jobIndex -le $Settings.NumJobs; $jobIndex++) {
            $filePath = ConvertTo-FioJobPath -Path (Join-Path -Path $RunContext.TargetRunDirectory -ChildPath ('testfile.{0:D2}.dat' -f $jobIndex))

            $lines.Add('')
            $lines.Add('[bench-{0:D2}]' -f $jobIndex)
            $lines.Add("filename=$filePath")
            if ($Phase -eq 'Combined' -and $jobIndex -eq 1) {
                $lines.Add('stonewall=1')
            }
            $lines.Add("rw=$($Settings.ReadWrite)")
            if ($null -ne $Settings.ReadMix) {
                $lines.Add("rwmixread=$($Settings.ReadMix)")
            }
            if ($Settings.Fsync -gt 0) {
                $lines.Add("fsync=$($Settings.Fsync)")
            }
        }
    }

    $lines -join [Environment]::NewLine
}

function Get-FioBenchFilePaths {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Settings,

        [Parameter(Mandatory)]
        [pscustomobject]$RunContext
    )

    $paths = for ($jobIndex = 1; $jobIndex -le $Settings.NumJobs; $jobIndex++) {
        Join-Path -Path $RunContext.TargetRunDirectory -ChildPath ('testfile.{0:D2}.dat' -f $jobIndex)
    }

    @($paths)
}

function Test-FioPreparedFiles {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string[]]$Paths,

        [Parameter(Mandatory)]
        [int64]$ExpectedLengthBytes
    )

    $results = foreach ($path in $Paths) {
        $exists = Test-Path -LiteralPath $path
        $item = if ($exists) { Get-Item -LiteralPath $path -ErrorAction Stop } else { $null }
        [pscustomobject]@{
            Path = $path
            Exists = $exists
            Length = if ($item) { [int64]$item.Length } else { $null }
            ExpectedLength = $ExpectedLengthBytes
            IsValid = $exists -and $item.Length -ge $ExpectedLengthBytes
        }
    }

    [pscustomobject]@{
        AllValid = @($results | Where-Object { -not $_.IsValid }).Count -eq 0
        Files = @($results)
    }
}

function ConvertTo-FioJobPath {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    if ($Path -match '^[A-Za-z]:') {
        return ($Path -replace '^([A-Za-z]):', '$1\:')
    }

    return $Path
}

function Resolve-FioBinary {
    [CmdletBinding()]
    param(
        [string]$ExplicitPath
    )

    # Windows installs are often present without PATH being updated, so resolution
    # walks from the explicit override to PATH, installed-program metadata, and
    # finally a few conventional Program Files locations.
    $candidates = New-Object System.Collections.Generic.List[string]
    if ($ExplicitPath) {
        $candidates.Add($ExplicitPath)
    }

    $command = Get-Command -Name 'fio.exe' -ErrorAction SilentlyContinue
    if ($command) {
        $candidates.Add($command.Source)
    }

    $candidates.Add('C:\Program Files\fio\fio.exe')
    $candidates.Add('C:\Program Files\fio\bin\fio.exe')
    $candidates.Add('C:\Program Files (x86)\fio\fio.exe')
    $candidates.Add('C:\Program Files (x86)\fio\bin\fio.exe')

    foreach ($discoveredPath in Get-FioInstalledProgramCandidates) {
        $candidates.Add($discoveredPath)
    }

    foreach ($candidate in $candidates | Select-Object -Unique) {
        if (-not (Test-Path -LiteralPath $candidate)) {
            continue
        }

        $versionOutput = & $candidate --version 2>&1 | Select-Object -First 1
        return [pscustomobject]@{
            Path = (Resolve-Path -LiteralPath $candidate).Path
            Version = [string]$versionOutput
        }
    }

    throw 'Unable to find fio.exe. Install fio, add it to PATH, or pass -FioPath explicitly.'
}

function Get-FioInstalledProgramCandidates {
    [CmdletBinding()]
    param()

    $paths = New-Object System.Collections.Generic.List[string]

    # MSI-based installs may expose only partial metadata, so collect several
    # candidate file paths from the uninstall registry and validate them later.
    foreach ($entry in Get-FioInstalledProgramEntry) {
        foreach ($candidate in Get-FioCandidatePathsFromEntry -Entry $entry) {
            $paths.Add($candidate)
        }
    }

    foreach ($searchRoot in @($env:ProgramFiles, ${env:ProgramFiles(x86)}) | Where-Object { $_ -and (Test-Path -LiteralPath $_) }) {
        foreach ($directory in Get-ChildItem -LiteralPath $searchRoot -Directory -Filter 'fio*' -ErrorAction SilentlyContinue) {
            $paths.Add((Join-Path -Path $directory.FullName -ChildPath 'fio.exe'))
            $paths.Add((Join-Path -Path $directory.FullName -ChildPath 'bin\fio.exe'))
        }
    }

    $paths | Select-Object -Unique
}

function Get-FioInstalledProgramEntry {
    [CmdletBinding()]
    param()

    $registryPaths = @(
        'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*',
        'HKLM:\SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\*',
        'HKCU:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*'
    )

    Get-ItemProperty -Path $registryPaths -ErrorAction SilentlyContinue |
        Where-Object {
            $displayNameProperty = $_.PSObject.Properties['DisplayName']
            if (-not $displayNameProperty -or [string]::IsNullOrWhiteSpace([string]$displayNameProperty.Value)) {
                return $false
            }

            $displayName = [string]$displayNameProperty.Value
            ($displayName -match '(^|\s)fio(\s|$)') -or
            ($displayName -match 'Flexible I/O Tester')
        }
}

function Get-FioCandidatePathsFromEntry {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Entry
    )

    $paths = New-Object System.Collections.Generic.List[string]

    foreach ($propertyName in 'InstallLocation', 'InstallSource') {
        $property = $Entry.PSObject.Properties[$propertyName]
        $value = if ($property) { [string]$property.Value } else { $null }
        if (-not [string]::IsNullOrWhiteSpace($value)) {
            $paths.Add((Join-Path -Path $value -ChildPath 'fio.exe'))
            $paths.Add((Join-Path -Path $value -ChildPath 'bin\fio.exe'))
        }
    }

    foreach ($propertyName in 'DisplayIcon', 'QuietUninstallString') {
        $property = $Entry.PSObject.Properties[$propertyName]
        $value = if ($property) { [string]$property.Value } else { $null }
        if ([string]::IsNullOrWhiteSpace($value)) {
            continue
        }

        $exePath = Select-FioExecutablePathFromString -Value $value
        if ($exePath) {
            $paths.Add($exePath)
        }
    }

    $paths | Select-Object -Unique
}

function Select-FioExecutablePathFromString {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Value
    )

    foreach ($pattern in @('"([^"]*fio\.exe)"', '([^\s]*fio\.exe)')) {
        $match = [regex]::Match($Value, $pattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
        if ($match.Success) {
            return $match.Groups[1].Value
        }
    }

    return $null
}

function Invoke-FioSqlBenchRun {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$FioPath,

        [Parameter(Mandatory)]
        [string]$JobFilePath,

        [Parameter(Mandatory)]
        [string]$OutputJsonPath,

        [Parameter(Mandatory)]
        [string]$ConsoleLogPath
    )

    $arguments = @(
        '--output-format=json',
        "--output=$OutputJsonPath",
        $JobFilePath
    )

    $consoleOutput = & $FioPath @arguments 2>&1
    $exitCode = $LASTEXITCODE
    $consoleOutput | Set-Content -Path $ConsoleLogPath -Encoding utf8

    if ($exitCode -ne 0) {
        throw "fio exited with code $exitCode. See $ConsoleLogPath for details."
    }

    if (-not (Test-Path -LiteralPath $OutputJsonPath)) {
        throw 'fio did not produce the expected JSON output file.'
    }

    [pscustomobject]@{
        ExitCode = $exitCode
        OutputJsonPath = $OutputJsonPath
        ConsoleLogPath = $ConsoleLogPath
    }
}

function ConvertFrom-FioJsonToSummary {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$JsonPath,

        [Parameter(Mandatory)]
        [pscustomobject]$RunContext,

        [Parameter(Mandatory)]
        [pscustomobject]$Settings,

        [Parameter(Mandatory)]
        [int]$Iteration,

        [Parameter(Mandatory)]
        [string]$FioVersion,

        [Parameter(Mandatory)]
        [pscustomobject]$TargetInfo
    )

    $fio = Get-Content -LiteralPath $JsonPath -Raw | ConvertFrom-Json -Depth 100
    $jobs = @($fio.jobs | Where-Object { $_.jobname -like 'bench*' })
    if ($jobs.Count -eq 0) {
        $jobs = @($fio.jobs)
    }

    $read = Merge-FioOperationStats -Jobs $jobs -OperationName 'read'
    $write = Merge-FioOperationStats -Jobs $jobs -OperationName 'write'

    [pscustomobject]@{
        RunId = $RunContext.RunId
        TimestampUtc = $RunContext.TimestampUtc
        Iteration = $Iteration
        TargetPath = $TargetInfo.Path
        TargetType = $TargetInfo.Type
        SmbMetadata = $TargetInfo.SmbMetadata
        FioVersion = $FioVersion
        RuntimeSec = $Settings.RuntimeSec
        RampSec = $Settings.RampSec
        FileSizeGB = $Settings.FileSizeGB
        BlockSize = $Settings.BlockSize
        QueueDepth = $Settings.QueueDepth
        NumJobs = $Settings.NumJobs
        Direct = $Settings.Direct
        ReadWrite = $Settings.ReadWrite
        ReadMix = $Settings.ReadMix
        Fsync = $Settings.Fsync
        Read = $read
        Write = $write
    }
}

function Merge-FioOperationStats {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object[]]$Jobs,

        [Parameter(Mandatory)]
        [ValidateSet('read', 'write')]
        [string]$OperationName
    )

    $opStats = foreach ($job in $Jobs) {
        $operation = $job.$OperationName
        if ($null -eq $operation) {
            continue
        }

        $latencyMetric = Get-FioLatencyMetric -Operation $operation
        $percentiles = if ($null -ne $latencyMetric) { Get-FioLatencyPercentilesUs -Operation $operation } else { $null }

        [pscustomobject]@{
            Iops = [double]$operation.iops
            BwBytes = [double]$operation.bw_bytes
            IoBytes = [double]$operation.io_bytes
            RuntimeMs = [double]$operation.runtime
            TotalIos = [double]$operation.total_ios
            BwMinKBps = [double]$operation.bw_min
            BwMaxKBps = [double]$operation.bw_max
            BwMeanKBps = [double]$operation.bw_mean
            BwStdDevKBps = [double]$operation.bw_dev
            BwSamples = [double]$operation.bw_samples
            IopsMin = [double]$operation.iops_min
            IopsMax = [double]$operation.iops_max
            IopsMean = [double]$operation.iops_mean
            IopsStdDev = [double]$operation.iops_stddev
            IopsSamples = [double]$operation.iops_samples
            LatencySampleCount = if ($null -ne $latencyMetric) { [double]$latencyMetric.SampleCount } else { 0 }
            MeanLatencyUs = if ($null -ne $latencyMetric) { [math]::Round(($latencyMetric.Mean / $latencyMetric.Divisor), 2) } else { $null }
            Percentiles = $percentiles
        }
    }

    if (-not $opStats) {
        return [pscustomobject]@{
            Iops = 0
            BandwidthMBps = 0
            IoMB = 0
            RuntimeMs = 0
            TotalIos = 0
            LatencySampleCount = 0
            MeanLatencyUs = $null
            P50LatencyUs = $null
            P95LatencyUs = $null
            P99LatencyUs = $null
            P999LatencyUs = $null
            WorstP99LatencyUs = $null
            WorstP999LatencyUs = $null
            BandwidthMinMBps = $null
            BandwidthMaxMBps = $null
            BandwidthMeanMBps = $null
            BandwidthStdDevMBps = $null
            BandwidthCvPercent = $null
            IopsMin = $null
            IopsMax = $null
            IopsMean = $null
            IopsStdDev = $null
            IopsCvPercent = $null
        }
    }

    $validLatency = @($opStats | Where-Object { $null -ne $_.MeanLatencyUs })
    $percentileStats = @($opStats | Where-Object { $null -ne $_.Percentiles })
    $weightedLatencyStats = @($validLatency | Where-Object { $_.LatencySampleCount -gt 0 })
    $weightedBwStats = @($opStats | Where-Object { $_.BwSamples -gt 0 })
    $weightedIopsStats = @($opStats | Where-Object { $_.IopsSamples -gt 0 })

    $bandwidthMeanMBps = Get-FioWeightedAverage -Items $weightedBwStats -ValueProperty 'BwMeanKBps' -WeightProperty 'BwSamples' -Divisor 1024.0
    $bandwidthStdDevMBps = Get-FioWeightedAverage -Items $weightedBwStats -ValueProperty 'BwStdDevKBps' -WeightProperty 'BwSamples' -Divisor 1024.0
    $iopsMean = Get-FioWeightedAverage -Items $weightedIopsStats -ValueProperty 'IopsMean' -WeightProperty 'IopsSamples'
    $iopsStdDev = Get-FioWeightedAverage -Items $weightedIopsStats -ValueProperty 'IopsStdDev' -WeightProperty 'IopsSamples'

    [pscustomobject]@{
        Iops = [math]::Round((($opStats | Measure-Object -Property Iops -Sum).Sum), 2)
        BandwidthMBps = [math]::Round((((($opStats | Measure-Object -Property BwBytes -Sum).Sum) / 1MB)), 2)
        IoMB = [math]::Round((((($opStats | Measure-Object -Property IoBytes -Sum).Sum) / 1MB)), 2)
        RuntimeMs = [math]::Round((($opStats | Measure-Object -Property RuntimeMs -Maximum).Maximum), 2)
        TotalIos = [math]::Round((($opStats | Measure-Object -Property TotalIos -Sum).Sum), 2)
        LatencySampleCount = [math]::Round((($opStats | Measure-Object -Property LatencySampleCount -Sum).Sum), 2)
        MeanLatencyUs = Get-FioWeightedAverage -Items $weightedLatencyStats -ValueProperty 'MeanLatencyUs' -WeightProperty 'LatencySampleCount'
        P50LatencyUs = Get-AggregatedPercentile -Stats $percentileStats -Name 'P50' -Mode 'WeightedAverage'
        P95LatencyUs = Get-AggregatedPercentile -Stats $percentileStats -Name 'P95' -Mode 'WeightedAverage'
        P99LatencyUs = Get-AggregatedPercentile -Stats $percentileStats -Name 'P99' -Mode 'WeightedAverage'
        P999LatencyUs = Get-AggregatedPercentile -Stats $percentileStats -Name 'P999' -Mode 'WeightedAverage'
        WorstP99LatencyUs = Get-AggregatedPercentile -Stats $percentileStats -Name 'P99' -Mode 'Maximum'
        WorstP999LatencyUs = Get-AggregatedPercentile -Stats $percentileStats -Name 'P999' -Mode 'Maximum'
        BandwidthMinMBps = [math]::Round((((($opStats | Measure-Object -Property BwMinKBps -Minimum).Minimum) / 1024.0)), 2)
        BandwidthMaxMBps = [math]::Round((((($opStats | Measure-Object -Property BwMaxKBps -Maximum).Maximum) / 1024.0)), 2)
        BandwidthMeanMBps = $bandwidthMeanMBps
        BandwidthStdDevMBps = $bandwidthStdDevMBps
        BandwidthCvPercent = Get-FioCoefficientOfVariation -Mean $bandwidthMeanMBps -StdDev $bandwidthStdDevMBps
        IopsMin = [math]::Round((($opStats | Measure-Object -Property IopsMin -Minimum).Minimum), 2)
        IopsMax = [math]::Round((($opStats | Measure-Object -Property IopsMax -Maximum).Maximum), 2)
        IopsMean = $iopsMean
        IopsStdDev = $iopsStdDev
        IopsCvPercent = Get-FioCoefficientOfVariation -Mean $iopsMean -StdDev $iopsStdDev
    }
}

function Get-AggregatedPercentile {
    [CmdletBinding()]
    param(
        [object[]]$Stats,

        [Parameter(Mandatory)]
        [string]$Name,

        [ValidateSet('Maximum', 'WeightedAverage')]
        [string]$Mode = 'Maximum'
    )

    if ($null -eq $Stats -or $Stats.Count -eq 0) {
        return $null
    }

    $values = @($Stats | ForEach-Object { $_.Percentiles.$Name } | Where-Object { $null -ne $_ })
    if ($values.Count -eq 0) {
        return $null
    }

    if ($Mode -eq 'WeightedAverage') {
        return Get-FioWeightedAverage -Items @($Stats | Where-Object { $null -ne $_.Percentiles.$Name -and $_.LatencySampleCount -gt 0 }) -ValueProperty ("Percentiles.$Name") -WeightProperty 'LatencySampleCount'
    }

    [math]::Round((($values | Measure-Object -Maximum).Maximum), 2)
}

function Get-FioWeightedAverage {
    [CmdletBinding()]
    param(
        [object[]]$Items,

        [Parameter(Mandatory)]
        [string]$ValueProperty,

        [Parameter(Mandatory)]
        [string]$WeightProperty,

        [double]$Divisor = 1.0
    )

    if ($null -eq $Items -or $Items.Count -eq 0) {
        return $null
    }

    $weightedSum = 0.0
    $weightTotal = 0.0
    foreach ($item in $Items) {
        $value = Get-FioNestedPropertyValue -Object $item -Path $ValueProperty
        $weight = Get-FioNestedPropertyValue -Object $item -Path $WeightProperty
        if ($null -eq $value -or $null -eq $weight) {
            continue
        }

        $numericWeight = [double]$weight
        if ($numericWeight -le 0) {
            continue
        }

        $weightedSum += ([double]$value * $numericWeight)
        $weightTotal += $numericWeight
    }

    if ($weightTotal -le 0) {
        return $null
    }

    return [math]::Round((($weightedSum / $weightTotal) / $Divisor), 2)
}

function Get-FioNestedPropertyValue {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object]$Object,

        [Parameter(Mandatory)]
        [string]$Path
    )

    $current = $Object
    foreach ($segment in $Path.Split('.')) {
        if ($null -eq $current) {
            return $null
        }

        $property = $current.PSObject.Properties[$segment]
        if (-not $property) {
            return $null
        }

        $current = $property.Value
    }

    return $current
}

function Get-FioCoefficientOfVariation {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [double]$Mean,

        [AllowNull()]
        [double]$StdDev
    )

    if ($null -eq $Mean -or $null -eq $StdDev -or $Mean -eq 0) {
        return $null
    }

    return [math]::Round((($StdDev / $Mean) * 100.0), 2)
}

function Get-FioMeanLatencyUs {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Operation
    )

    $metric = Get-FioLatencyMetric -Operation $Operation
    if ($null -eq $metric) {
        return $null
    }

    [math]::Round(($metric.Mean / $metric.Divisor), 2)
}

function Get-FioLatencyPercentilesUs {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Operation
    )

    $metric = Get-FioLatencyMetric -Operation $Operation
    if ($null -eq $metric) {
        return $null
    }

    $percentileProperty = $metric.Source.PSObject.Properties['percentile']
    if (-not $percentileProperty -or $null -eq $percentileProperty.Value) {
        return $null
    }

    $percentiles = $percentileProperty.Value

    [pscustomobject]@{
        P50 = Convert-FioPercentileToUs -Percentiles $percentiles -Lookup @('50.000000', '50.000000', '50.0', '50') -Divisor $metric.Divisor
        P95 = Convert-FioPercentileToUs -Percentiles $percentiles -Lookup @('95.000000', '95.0', '95') -Divisor $metric.Divisor
        P99 = Convert-FioPercentileToUs -Percentiles $percentiles -Lookup @('99.000000', '99.0', '99') -Divisor $metric.Divisor
        P999 = Convert-FioPercentileToUs -Percentiles $percentiles -Lookup @('99.900000', '99.9') -Divisor $metric.Divisor
    }
}

function Get-FioLatencyMetric {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Operation
    )

    foreach ($candidate in @(
        @{ Name = 'clat_ns'; Divisor = 1000.0 },
        @{ Name = 'clat_us'; Divisor = 1.0 },
        @{ Name = 'lat_ns'; Divisor = 1000.0 },
        @{ Name = 'lat_us'; Divisor = 1.0 }
    )) {
        $property = $Operation.PSObject.Properties[$candidate.Name]
        if (-not $property -or $null -eq $property.Value) {
            continue
        }

        $meanProperty = $property.Value.PSObject.Properties['mean']
        if (-not $meanProperty -or $null -eq $meanProperty.Value) {
            continue
        }

        return [pscustomobject]@{
            Source = $property.Value
            Mean = [double]$meanProperty.Value
            Divisor = $candidate.Divisor
            SampleCount = if ($property.Value.PSObject.Properties['N']) { [double]$property.Value.N } else { [double]$Operation.total_ios }
        }
    }

    return $null
}

function Convert-FioPercentileToUs {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Percentiles,

        [Parameter(Mandatory)]
        [string[]]$Lookup,

        [Parameter(Mandatory)]
        [double]$Divisor
    )

    foreach ($candidate in $Lookup) {
        $property = $Percentiles.PSObject.Properties[$candidate]
        if ($property) {
            return [math]::Round(([double]$property.Value / $Divisor), 2)
        }
    }

    return $null
}

function Export-FioSqlBenchCsv {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object[]]$Summaries,

        [Parameter(Mandatory)]
        [string]$Path
    )

    $rows = foreach ($summary in $Summaries) {
        [pscustomobject]@{
            RunId = $summary.RunId
            TimestampUtc = $summary.TimestampUtc
            Iteration = $summary.Iteration
            TargetPath = $summary.TargetPath
            TargetType = $summary.TargetType
            SmbServerName = if ($summary.SmbMetadata) { $summary.SmbMetadata.ServerName } else { $null }
            SmbShareName = if ($summary.SmbMetadata) { $summary.SmbMetadata.ShareName } else { $null }
            SmbDialect = if ($summary.SmbMetadata) { $summary.SmbMetadata.Dialect } else { $null }
            SmbContinuouslyAvailable = if ($summary.SmbMetadata) { $summary.SmbMetadata.ContinuouslyAvailable } else { $null }
            SmbEncryptData = if ($summary.SmbMetadata) { $summary.SmbMetadata.EncryptData } else { $null }
            SmbMultichannelPathCount = if ($summary.SmbMetadata) { $summary.SmbMetadata.MultichannelPathCount } else { $null }
            SmbRdmaPathCount = if ($summary.SmbMetadata) { $summary.SmbMetadata.RdmaPathCount } else { $null }
            FioVersion = $summary.FioVersion
            RuntimeSec = $summary.RuntimeSec
            FileSizeGB = $summary.FileSizeGB
            BlockSize = $summary.BlockSize
            QueueDepth = $summary.QueueDepth
            NumJobs = $summary.NumJobs
            Direct = $summary.Direct
            ReadWrite = $summary.ReadWrite
            ReadMix = $summary.ReadMix
            Fsync = $summary.Fsync
            ReadIops = $summary.Read.Iops
            ReadBandwidthMBps = $summary.Read.BandwidthMBps
            ReadMeanLatencyUs = $summary.Read.MeanLatencyUs
            ReadP50LatencyUs = $summary.Read.P50LatencyUs
            ReadP95LatencyUs = $summary.Read.P95LatencyUs
            ReadP99LatencyUs = $summary.Read.P99LatencyUs
            ReadP999LatencyUs = $summary.Read.P999LatencyUs
            ReadWorstP99LatencyUs = $summary.Read.WorstP99LatencyUs
            ReadWorstP999LatencyUs = $summary.Read.WorstP999LatencyUs
            ReadBandwidthCvPercent = $summary.Read.BandwidthCvPercent
            WriteIops = $summary.Write.Iops
            WriteBandwidthMBps = $summary.Write.BandwidthMBps
            WriteMeanLatencyUs = $summary.Write.MeanLatencyUs
            WriteP50LatencyUs = $summary.Write.P50LatencyUs
            WriteP95LatencyUs = $summary.Write.P95LatencyUs
            WriteP99LatencyUs = $summary.Write.P99LatencyUs
            WriteP999LatencyUs = $summary.Write.P999LatencyUs
            WriteWorstP99LatencyUs = $summary.Write.WorstP99LatencyUs
            WriteWorstP999LatencyUs = $summary.Write.WorstP999LatencyUs
            WriteBandwidthCvPercent = $summary.Write.BandwidthCvPercent
        }
    }

    $rows | Export-Csv -Path $Path -NoTypeInformation -Encoding utf8
}

function Get-FioHistoricalAverageValue {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object[]]$Items,

        [Parameter(Mandatory)]
        [scriptblock]$ValueScript
    )

    $values = @(
        foreach ($item in $Items) {
            try {
                $value = & $ValueScript $item
            }
            catch {
                $value = $null
            }

            if ($null -ne $value) {
                [double]$value
            }
        }
    )

    if (-not $values -or $values.Count -eq 0) {
        return $null
    }

    return [math]::Round((($values | Measure-Object -Average).Average), 2)
}

function Get-FioHistoricalTriplet {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object[]]$Values
    )

    $numeric = @($Values | Where-Object { $null -ne $_ } | ForEach-Object { [double]$_ })
    if ($numeric.Count -eq 0) {
        return [pscustomobject]@{
            Min = $null
            Avg = $null
            Max = $null
        }
    }

    return [pscustomobject]@{
        Min = [math]::Round((($numeric | Measure-Object -Minimum).Minimum), 2)
        Avg = [math]::Round((($numeric | Measure-Object -Average).Average), 2)
        Max = [math]::Round((($numeric | Measure-Object -Maximum).Maximum), 2)
    }
}

function Get-FioHistoryRunAggregate {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$RootSummary,

        [Parameter(Mandatory)]
        [object[]]$Iterations,

        [Parameter(Mandatory)]
        [string]$SourceSummaryPath
    )

    $first = $Iterations[0]
    $profileName = if ($RootSummary.PSObject.Properties['Profile'] -and -not [string]::IsNullOrWhiteSpace([string]$RootSummary.Profile)) {
        [string]$RootSummary.Profile
    }
    else {
        'Unknown'
    }

    $resultDirectory = if ($RootSummary.PSObject.Properties['ResultDirectory'] -and -not [string]::IsNullOrWhiteSpace([string]$RootSummary.ResultDirectory)) {
        [string]$RootSummary.ResultDirectory
    }
    else {
        Split-Path -Path $SourceSummaryPath -Parent
    }

    $timestampValues = @(
        $Iterations |
            ForEach-Object {
                if ($_.PSObject.Properties['TimestampUtc'] -and -not [string]::IsNullOrWhiteSpace([string]$_.TimestampUtc)) {
                    try {
                        [DateTime]$_.TimestampUtc
                    }
                    catch {
                        $null
                    }
                }
            } |
            Where-Object { $null -ne $_ }
    )

    $timestampUtc = if ($timestampValues.Count -gt 0) {
        ($timestampValues | Sort-Object | Select-Object -First 1).ToString('o')
    }
    elseif ($RootSummary.PSObject.Properties['TimestampUtc'] -and -not [string]::IsNullOrWhiteSpace([string]$RootSummary.TimestampUtc)) {
        [string]$RootSummary.TimestampUtc
    }
    else {
        $null
    }

    $read = [pscustomobject]@{
        Iops = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Read.Iops }
        BandwidthMBps = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Read.BandwidthMBps }
        IoMB = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Read.IoMB }
        MeanLatencyUs = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Read.MeanLatencyUs }
        P50LatencyUs = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Read.P50LatencyUs }
        P95LatencyUs = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Read.P95LatencyUs }
        P99LatencyUs = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Read.P99LatencyUs }
        P999LatencyUs = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Read.P999LatencyUs }
        WorstP99LatencyUs = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Read.WorstP99LatencyUs }
        WorstP999LatencyUs = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Read.WorstP999LatencyUs }
        BandwidthCvPercent = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Read.BandwidthCvPercent }
    }

    $write = [pscustomobject]@{
        Iops = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Write.Iops }
        BandwidthMBps = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Write.BandwidthMBps }
        IoMB = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Write.IoMB }
        MeanLatencyUs = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Write.MeanLatencyUs }
        P50LatencyUs = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Write.P50LatencyUs }
        P95LatencyUs = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Write.P95LatencyUs }
        P99LatencyUs = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Write.P99LatencyUs }
        P999LatencyUs = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Write.P999LatencyUs }
        WorstP99LatencyUs = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Write.WorstP99LatencyUs }
        WorstP999LatencyUs = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Write.WorstP999LatencyUs }
        BandwidthCvPercent = Get-FioHistoricalAverageValue -Items $Iterations -ValueScript { param($item) $item.Write.BandwidthCvPercent }
    }

    return [pscustomobject]@{
        RunId = if ($RootSummary.PSObject.Properties['RunId']) { [string]$RootSummary.RunId } else { [string]$first.RunId }
        Profile = $profileName
        TimestampUtc = $timestampUtc
        ResultDirectory = $resultDirectory
        SourceSummaryPath = $SourceSummaryPath
        IterationCount = $Iterations.Count
        TargetPath = [string]$first.TargetPath
        TargetType = [string]$first.TargetType
        SmbMetadata = if ($first.PSObject.Properties['SmbMetadata']) { $first.SmbMetadata } else { $null }
        FioVersion = if ($first.PSObject.Properties['FioVersion']) { [string]$first.FioVersion } else { $null }
        RuntimeSec = if ($first.PSObject.Properties['RuntimeSec']) { [int]$first.RuntimeSec } else { $null }
        RampSec = if ($first.PSObject.Properties['RampSec']) { [int]$first.RampSec } else { $null }
        FileSizeGB = if ($first.PSObject.Properties['FileSizeGB']) { [decimal]$first.FileSizeGB } else { $null }
        BlockSize = if ($first.PSObject.Properties['BlockSize']) { [string]$first.BlockSize } else { $null }
        QueueDepth = if ($first.PSObject.Properties['QueueDepth']) { [int]$first.QueueDepth } else { $null }
        NumJobs = if ($first.PSObject.Properties['NumJobs']) { [int]$first.NumJobs } else { $null }
        Direct = if ($first.PSObject.Properties['Direct']) { [int]$first.Direct } else { $null }
        ReadWrite = if ($first.PSObject.Properties['ReadWrite']) { [string]$first.ReadWrite } else { $null }
        ReadMix = if ($first.PSObject.Properties['ReadMix']) { $first.ReadMix } else { $null }
        Fsync = if ($first.PSObject.Properties['Fsync']) { [int]$first.Fsync } else { $null }
        Read = $read
        Write = $write
        Iterations = $Iterations
    }
}

function Import-FioSqlBenchHistory {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$ResultsRoot,

        [ValidateSet('Any', 'Data', 'Log', 'Tempdb', 'BackupRestore', 'DbccScan')]
        [string]$Profile = 'Any',

        [ValidateSet('Any', 'Local', 'Smb')]
        [string]$TargetType = 'Any',

        [string]$TargetPathLike,

        [Nullable[int]]$Newest
    )

    if (-not (Test-Path -LiteralPath $ResultsRoot)) {
        throw "Results root '$ResultsRoot' does not exist."
    }

    $summaryFiles = @(Get-ChildItem -LiteralPath $ResultsRoot -Filter 'summary.json' -Recurse -File | Sort-Object FullName)
    $runs = foreach ($file in $summaryFiles) {
        $rootSummary = Get-Content -LiteralPath $file.FullName -Raw | ConvertFrom-Json -Depth 100
        $iterations = if ($rootSummary.PSObject.Properties['Iterations']) {
            @($rootSummary.Iterations)
        }
        elseif ($rootSummary.PSObject.Properties['Iteration']) {
            @($rootSummary)
        }
        else {
            @()
        }

        if ($iterations.Count -eq 0) {
            continue
        }

        $run = Get-FioHistoryRunAggregate -RootSummary $rootSummary -Iterations $iterations -SourceSummaryPath $file.FullName
        if ($Profile -ne 'Any' -and $run.Profile -ine $Profile) {
            continue
        }

        if ($TargetType -ne 'Any' -and $run.TargetType -ine $TargetType) {
            continue
        }

        if (-not [string]::IsNullOrWhiteSpace($TargetPathLike) -and $run.TargetPath -notlike $TargetPathLike) {
            continue
        }

        $run
    }

    $ordered = @($runs | Sort-Object TimestampUtc)
    if ($null -ne $Newest -and $Newest -gt 0) {
        return @($ordered | Sort-Object TimestampUtc -Descending | Select-Object -First $Newest | Sort-Object TimestampUtc)
    }

    return $ordered
}

function Get-FioHistoricalRollup {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object[]]$Runs
    )

    if ($Runs.Count -eq 0) {
        return @()
    }

    $groups = $Runs | Group-Object -Property {
        '{0}|{1}|{2}' -f $_.Profile, $_.TargetType, $_.TargetPath
    }

    return @(
        foreach ($group in $groups) {
            $sample = $group.Group[0]
            $orderedRuns = @($group.Group | Sort-Object TimestampUtc)
            [pscustomobject]@{
                Profile = $sample.Profile
                TargetType = $sample.TargetType
                TargetPath = $sample.TargetPath
                RunCount = $group.Count
                FirstTimestampUtc = $orderedRuns[0].TimestampUtc
                LastTimestampUtc = $orderedRuns[-1].TimestampUtc
                ReadIops = Get-FioHistoricalTriplet -Values @($orderedRuns | ForEach-Object { $_.Read.Iops })
                ReadBandwidthMBps = Get-FioHistoricalTriplet -Values @($orderedRuns | ForEach-Object { $_.Read.BandwidthMBps })
                ReadMeanLatencyMs = Get-FioHistoricalTriplet -Values @($orderedRuns | ForEach-Object { if ($null -ne $_.Read.MeanLatencyUs) { [math]::Round(($_.Read.MeanLatencyUs / 1000.0), 2) } })
                ReadP99LatencyMs = Get-FioHistoricalTriplet -Values @($orderedRuns | ForEach-Object { if ($null -ne $_.Read.P99LatencyUs) { [math]::Round(($_.Read.P99LatencyUs / 1000.0), 2) } })
                WriteIops = Get-FioHistoricalTriplet -Values @($orderedRuns | ForEach-Object { $_.Write.Iops })
                WriteBandwidthMBps = Get-FioHistoricalTriplet -Values @($orderedRuns | ForEach-Object { $_.Write.BandwidthMBps })
                WriteMeanLatencyMs = Get-FioHistoricalTriplet -Values @($orderedRuns | ForEach-Object { if ($null -ne $_.Write.MeanLatencyUs) { [math]::Round(($_.Write.MeanLatencyUs / 1000.0), 2) } })
                WriteP99LatencyMs = Get-FioHistoricalTriplet -Values @($orderedRuns | ForEach-Object { if ($null -ne $_.Write.P99LatencyUs) { [math]::Round(($_.Write.P99LatencyUs / 1000.0), 2) } })
            }
        }
    )
}

function Export-FioSqlBenchHistoricalCsv {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object[]]$Runs,

        [Parameter(Mandatory)]
        [string]$Path
    )

    $rows = foreach ($run in $Runs) {
        [pscustomobject]@{
            RunId = $run.RunId
            Profile = $run.Profile
            TimestampUtc = $run.TimestampUtc
            ResultDirectory = $run.ResultDirectory
            TargetPath = $run.TargetPath
            TargetType = $run.TargetType
            SmbServerName = if ($run.SmbMetadata) { $run.SmbMetadata.ServerName } else { $null }
            SmbShareName = if ($run.SmbMetadata) { $run.SmbMetadata.ShareName } else { $null }
            SmbDialect = if ($run.SmbMetadata) { $run.SmbMetadata.Dialect } else { $null }
            SmbContinuouslyAvailable = if ($run.SmbMetadata) { $run.SmbMetadata.ContinuouslyAvailable } else { $null }
            SmbEncryptData = if ($run.SmbMetadata) { $run.SmbMetadata.EncryptData } else { $null }
            SmbMultichannelPathCount = if ($run.SmbMetadata) { $run.SmbMetadata.MultichannelPathCount } else { $null }
            SmbRdmaPathCount = if ($run.SmbMetadata) { $run.SmbMetadata.RdmaPathCount } else { $null }
            IterationCount = $run.IterationCount
            FioVersion = $run.FioVersion
            RuntimeSec = $run.RuntimeSec
            RampSec = $run.RampSec
            FileSizeGB = $run.FileSizeGB
            BlockSize = $run.BlockSize
            QueueDepth = $run.QueueDepth
            NumJobs = $run.NumJobs
            Direct = $run.Direct
            ReadWrite = $run.ReadWrite
            ReadMix = $run.ReadMix
            Fsync = $run.Fsync
            ReadIops = $run.Read.Iops
            ReadBandwidthMBps = $run.Read.BandwidthMBps
            ReadMeanLatencyUs = $run.Read.MeanLatencyUs
            ReadP95LatencyUs = $run.Read.P95LatencyUs
            ReadP99LatencyUs = $run.Read.P99LatencyUs
            ReadP999LatencyUs = $run.Read.P999LatencyUs
            ReadWorstP99LatencyUs = $run.Read.WorstP99LatencyUs
            ReadWorstP999LatencyUs = $run.Read.WorstP999LatencyUs
            ReadBandwidthCvPercent = $run.Read.BandwidthCvPercent
            WriteIops = $run.Write.Iops
            WriteBandwidthMBps = $run.Write.BandwidthMBps
            WriteMeanLatencyUs = $run.Write.MeanLatencyUs
            WriteP95LatencyUs = $run.Write.P95LatencyUs
            WriteP99LatencyUs = $run.Write.P99LatencyUs
            WriteP999LatencyUs = $run.Write.P999LatencyUs
            WriteWorstP99LatencyUs = $run.Write.WorstP99LatencyUs
            WriteWorstP999LatencyUs = $run.Write.WorstP999LatencyUs
            WriteBandwidthCvPercent = $run.Write.BandwidthCvPercent
        }
    }

    $rows | Export-Csv -Path $Path -NoTypeInformation -Encoding utf8
}

function ConvertTo-FioHtmlEncoded {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$Value
    )

    return [System.Net.WebUtility]::HtmlEncode([string]$Value)
}

function Format-FioHtmlMetric {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [double]$Value,

        [string]$Suffix = '',

        [int]$Decimals = 2
    )

    if ($null -eq $Value) {
        return '-'
    }

    return ('{0:N' + $Decimals + '}{1}') -f $Value, $Suffix
}

function Format-FioHtmlTimestamp {
        [CmdletBinding()]
        param(
                [AllowNull()]
                [string]$TimestampUtc
        )

        if ([string]::IsNullOrWhiteSpace($TimestampUtc)) {
                return '-'
        }

        try {
                return ([DateTimeOffset]::Parse($TimestampUtc)).ToLocalTime().ToString('yyyy-MM-dd HH:mm')
        }
        catch {
                return $TimestampUtc
        }
}

function Format-FioHtmlDelta {
        [CmdletBinding()]
        param(
                [AllowNull()]
                [double]$Current,

                [AllowNull()]
                [double]$Previous,

                [string]$Suffix = '',

                [switch]$LowerIsBetter
        )

        if ($null -eq $Current -or $null -eq $Previous) {
                return [pscustomobject]@{
                        Text = 'Baseline'
                        Class = 'delta-neutral'
                }
        }

        $delta = [math]::Round(($Current - $Previous), 2)
        $percent = if ($Previous -eq 0) { $null } else { [math]::Round((($delta / $Previous) * 100.0), 1) }
        $improved = if ($LowerIsBetter) { $delta -lt 0 } else { $delta -gt 0 }
        $class = if ($delta -eq 0) { 'delta-neutral' } elseif ($improved) { 'delta-good' } else { 'delta-bad' }
        $sign = if ($delta -gt 0) { '+' } else { '' }
        $percentText = if ($null -eq $percent) { '' } else { ' ({0}{1:N1}%)' -f $sign, $percent }

        return [pscustomobject]@{
                Text = ('{0}{1:N2}{2}{3}' -f $sign, $delta, $Suffix, $percentText)
                Class = $class
        }
}

    function ConvertTo-FioHtmlSettingBadges {
        [CmdletBinding()]
        param(
            [Parameter(Mandatory)]
            [object[]]$Settings,

            [hashtable]$PreviousValues
        )

        $badges = New-Object System.Collections.Generic.List[string]
        foreach ($setting in $Settings) {
            if ($null -eq $setting -or [string]::IsNullOrWhiteSpace([string]$setting.Value)) {
                continue
            }

            $label = if ($setting.PSObject.Properties['Label']) { [string]$setting.Label } else { [string]$setting.Key }
            $value = [string]$setting.Value
            $className = 'setting-badge'
            $title = '{0}={1}' -f $label, $value

            if ($null -ne $PreviousValues -and $PreviousValues.ContainsKey([string]$setting.Key)) {
                $previousValue = [string]$PreviousValues[[string]$setting.Key]
                if ($previousValue -ne $value) {
                    $className += ' setting-badge-changed'
                    $title = '{0} changed from {1} to {2}' -f $label, $previousValue, $value
                }
            }

            $badges.Add(@"
    <span class='$className' title='$([System.Net.WebUtility]::HtmlEncode($title))'>
      <span class='setting-key'>$([System.Net.WebUtility]::HtmlEncode($label))</span>
      <span class='setting-value'>$([System.Net.WebUtility]::HtmlEncode($value))</span>
    </span>
    "@)
        }

        if ($badges.Count -eq 0) {
            return "<span class='setting-badge setting-badge-empty'>No settings captured</span>"
        }

        return ($badges -join [Environment]::NewLine)
    }

function New-FioHtmlProfileComparisonSection {
        [CmdletBinding()]
        param(
                [Parameter(Mandatory)]
                [string]$Profile,

                [Parameter(Mandatory)]
                [object[]]$Runs
        )

        $orderedRuns = @($Runs | Sort-Object TimestampUtc)
        $recentRuns = @($orderedRuns | Select-Object -Last 6)
        $maxRead = @($recentRuns | ForEach-Object { $_.Read.BandwidthMBps } | Where-Object { $null -ne $_ } | Measure-Object -Maximum).Maximum
        $maxWrite = @($recentRuns | ForEach-Object { $_.Write.BandwidthMBps } | Where-Object { $null -ne $_ } | Measure-Object -Maximum).Maximum
        if ($null -eq $maxRead -or $maxRead -le 0) { $maxRead = 1 }
        if ($null -eq $maxWrite -or $maxWrite -le 0) { $maxWrite = 1 }

        $rows = New-Object System.Collections.Generic.List[string]
        for ($index = 0; $index -lt $recentRuns.Count; $index++) {
                $run = $recentRuns[$index]
                $previous = if ($index -gt 0) { $recentRuns[$index - 1] } else { $null }
                $readP99Ms = if ($null -ne $run.Read.P99LatencyUs) { [math]::Round(($run.Read.P99LatencyUs / 1000.0), 2) } else { $null }
                $writeP99Ms = if ($null -ne $run.Write.P99LatencyUs) { [math]::Round(($run.Write.P99LatencyUs / 1000.0), 2) } else { $null }
                $previousReadP99Ms = if ($null -ne $previous -and $null -ne $previous.Read.P99LatencyUs) { [math]::Round(($previous.Read.P99LatencyUs / 1000.0), 2) } else { $null }
                $previousWriteP99Ms = if ($null -ne $previous -and $null -ne $previous.Write.P99LatencyUs) { [math]::Round(($previous.Write.P99LatencyUs / 1000.0), 2) } else { $null }
                $readDelta = Format-FioHtmlDelta -Current $run.Read.BandwidthMBps -Previous $(if ($null -ne $previous) { $previous.Read.BandwidthMBps } else { $null }) -Suffix ' MB/s'
                $writeDelta = Format-FioHtmlDelta -Current $run.Write.BandwidthMBps -Previous $(if ($null -ne $previous) { $previous.Write.BandwidthMBps } else { $null }) -Suffix ' MB/s'
                $readLatencyDelta = Format-FioHtmlDelta -Current $readP99Ms -Previous $previousReadP99Ms -Suffix ' ms' -LowerIsBetter
                $writeLatencyDelta = Format-FioHtmlDelta -Current $writeP99Ms -Previous $previousWriteP99Ms -Suffix ' ms' -LowerIsBetter
                $readWidth = [math]::Round((($run.Read.BandwidthMBps / $maxRead) * 100.0), 2)
                $writeWidth = [math]::Round((($run.Write.BandwidthMBps / $maxWrite) * 100.0), 2)
                $runSettings = @(
                    [pscustomobject]@{ Key = 'BlockSize'; Label = 'bs'; Value = $run.BlockSize }
                    [pscustomobject]@{ Key = 'QueueDepth'; Label = 'qd'; Value = if ($null -ne $run.QueueDepth) { [string]$run.QueueDepth } else { $null } }
                    [pscustomobject]@{ Key = 'NumJobs'; Label = 'jobs'; Value = if ($null -ne $run.NumJobs) { [string]$run.NumJobs } else { $null } }
                    [pscustomobject]@{ Key = 'FileSizeGB'; Label = 'size'; Value = if ($null -ne $run.FileSizeGB) { '{0} GB' -f ([math]::Round([double]$run.FileSizeGB, 2)) } else { $null } }
                    [pscustomobject]@{ Key = 'ReadWrite'; Label = 'rw'; Value = $run.ReadWrite }
                    [pscustomobject]@{ Key = 'ReadMix'; Label = 'mix'; Value = if ($null -ne $run.ReadMix) { '{0}/{1}' -f $run.ReadMix, (100 - [int]$run.ReadMix) } else { $null } }
                    [pscustomobject]@{ Key = 'Direct'; Label = 'direct'; Value = if ($null -ne $run.Direct) { [string]$run.Direct } else { $null } }
                    [pscustomobject]@{ Key = 'Fsync'; Label = 'fsync'; Value = if ($null -ne $run.Fsync -and $run.Fsync -gt 0) { [string]$run.Fsync } else { $null } }
                    [pscustomobject]@{ Key = 'RuntimeSec'; Label = 'runtime'; Value = if ($null -ne $run.RuntimeSec) { '{0}s' -f $run.RuntimeSec } else { $null } }
                )
                $runTargetSettings = @(
                    [pscustomobject]@{ Key = 'IterationCount'; Label = 'iters'; Value = if ($null -ne $run.IterationCount) { [string]$run.IterationCount } else { $null } }
                    [pscustomobject]@{ Key = 'FioVersion'; Label = 'fio'; Value = $run.FioVersion }
                    [pscustomobject]@{ Key = 'TargetType'; Label = 'type'; Value = $run.TargetType }
                )
                $previousRunSettings = $null
                $previousTargetSettings = $null
                if ($null -ne $previous) {
                    $previousRunSettings = @{}
                    foreach ($setting in @(
                        [pscustomobject]@{ Key = 'BlockSize'; Value = $previous.BlockSize }
                        [pscustomobject]@{ Key = 'QueueDepth'; Value = if ($null -ne $previous.QueueDepth) { [string]$previous.QueueDepth } else { $null } }
                        [pscustomobject]@{ Key = 'NumJobs'; Value = if ($null -ne $previous.NumJobs) { [string]$previous.NumJobs } else { $null } }
                        [pscustomobject]@{ Key = 'FileSizeGB'; Value = if ($null -ne $previous.FileSizeGB) { '{0} GB' -f ([math]::Round([double]$previous.FileSizeGB, 2)) } else { $null } }
                        [pscustomobject]@{ Key = 'ReadWrite'; Value = $previous.ReadWrite }
                        [pscustomobject]@{ Key = 'ReadMix'; Value = if ($null -ne $previous.ReadMix) { '{0}/{1}' -f $previous.ReadMix, (100 - [int]$previous.ReadMix) } else { $null } }
                        [pscustomobject]@{ Key = 'Direct'; Value = if ($null -ne $previous.Direct) { [string]$previous.Direct } else { $null } }
                        [pscustomobject]@{ Key = 'Fsync'; Value = if ($null -ne $previous.Fsync -and $previous.Fsync -gt 0) { [string]$previous.Fsync } else { $null } }
                        [pscustomobject]@{ Key = 'RuntimeSec'; Value = if ($null -ne $previous.RuntimeSec) { '{0}s' -f $previous.RuntimeSec } else { $null } }
                    )) {
                        $previousRunSettings[[string]$setting.Key] = $setting.Value
                    }

                    $previousTargetSettings = @{}
                    foreach ($setting in @(
                        [pscustomobject]@{ Key = 'IterationCount'; Value = if ($null -ne $previous.IterationCount) { [string]$previous.IterationCount } else { $null } }
                        [pscustomobject]@{ Key = 'FioVersion'; Value = $previous.FioVersion }
                        [pscustomobject]@{ Key = 'TargetType'; Value = $previous.TargetType }
                    )) {
                        $previousTargetSettings[[string]$setting.Key] = $setting.Value
                    }
                }
                $settingsBadgeHtml = ConvertTo-FioHtmlSettingBadges -Settings $runSettings -PreviousValues $previousRunSettings
                $targetBadgeHtml = ConvertTo-FioHtmlSettingBadges -Settings $runTargetSettings -PreviousValues $previousTargetSettings

                $rows.Add(@"
<tr>
    <td>
        <div class='run-date'>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlTimestamp -TimestampUtc $run.TimestampUtc)))</div>
        <div class='subtle'>$([System.Net.WebUtility]::HtmlEncode([string]$run.RunId))</div>
            <div class='settings-badges'>
                $settingsBadgeHtml
            </div>
    </td>
    <td>
        <div>$([System.Net.WebUtility]::HtmlEncode([string]$run.TargetType))</div>
        <div class='subtle'>$([System.Net.WebUtility]::HtmlEncode([string]$run.TargetPath))</div>
            <div class='settings-badges'>
                $targetBadgeHtml
            </div>
    </td>
    <td>
        <div class='metric-cell'>
            <span>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $run.Read.BandwidthMBps -Suffix ' MB/s')))</span>
            <div class='mini-track'><div class='mini-fill throughput-read' style='width: ${readWidth}%'></div></div>
        </div>
        <div class='delta $($readDelta.Class)'>$([System.Net.WebUtility]::HtmlEncode($readDelta.Text))</div>
    </td>
    <td>
        <div class='metric-cell'>
            <span>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $run.Write.BandwidthMBps -Suffix ' MB/s')))</span>
            <div class='mini-track'><div class='mini-fill throughput-write' style='width: ${writeWidth}%'></div></div>
        </div>
        <div class='delta $($writeDelta.Class)'>$([System.Net.WebUtility]::HtmlEncode($writeDelta.Text))</div>
    </td>
    <td>
        <div>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $readP99Ms -Suffix ' ms')))</div>
        <div class='delta $($readLatencyDelta.Class)'>$([System.Net.WebUtility]::HtmlEncode($readLatencyDelta.Text))</div>
    </td>
    <td>
        <div>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $writeP99Ms -Suffix ' ms')))</div>
        <div class='delta $($writeLatencyDelta.Class)'>$([System.Net.WebUtility]::HtmlEncode($writeLatencyDelta.Text))</div>
    </td>
</tr>
"@)
        }

        return @"
<section class='table-card profile-card'>
    <div class='section-heading'>
        <div>
            <h2>$([System.Net.WebUtility]::HtmlEncode($Profile))</h2>
            <p>Recent runs for this workload profile, with each row compared against the previous run in the same profile.</p>
        </div>
        <div class='pill'>$($recentRuns.Count) runs shown</div>
    </div>
    <table>
        <thead>
            <tr>
                <th>Run Time</th>
                <th>Target</th>
                <th>Read Throughput</th>
                <th>Write Throughput</th>
                <th>Read P99</th>
                <th>Write P99</th>
            </tr>
        </thead>
        <tbody>
            $($rows -join [Environment]::NewLine)
        </tbody>
    </table>
</section>
"@
}

function New-FioHtmlBarChartSection {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Title,

        [Parameter(Mandatory)]
        [object[]]$Runs,

        [Parameter(Mandatory)]
        [scriptblock]$ValueScript,

        [Parameter(Mandatory)]
        [scriptblock]$LabelScript,

        [Parameter(Mandatory)]
        [scriptblock]$FormatScript,

        [Parameter(Mandatory)]
        [string]$BarClass
    )

    $points = @(
        foreach ($run in $Runs) {
            $value = & $ValueScript $run
            [pscustomobject]@{
                Label = & $LabelScript $run
                Value = if ($null -ne $value) { [double]$value } else { $null }
                Display = & $FormatScript $value
            }
        }
    )

    $maxValue = @($points | ForEach-Object { $_.Value } | Where-Object { $null -ne $_ } | Measure-Object -Maximum).Maximum
    if ($null -eq $maxValue -or $maxValue -le 0) {
        $maxValue = 1
    }

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("<section class='chart-card'><h2>$([System.Net.WebUtility]::HtmlEncode($Title))</h2>")
    foreach ($point in $points) {
        $width = if ($null -eq $point.Value) { 0 } else { [math]::Round(($point.Value / $maxValue) * 100, 2) }
        $lines.Add(@"
<div class='bar-row'>
  <div class='bar-label'>$([System.Net.WebUtility]::HtmlEncode([string]$point.Label))</div>
  <div class='bar-track'><div class='bar-fill $BarClass' style='width: ${width}%'></div></div>
  <div class='bar-value'>$([System.Net.WebUtility]::HtmlEncode([string]$point.Display))</div>
</div>
"@)
    }
    $lines.Add('</section>')
    return ($lines -join [Environment]::NewLine)
}

function Export-FioSqlBenchHtmlReport {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object[]]$Runs,

        [Parameter(Mandatory)]
        [string]$Path,

        [string]$Title = 'fio SQL Bench Report',

        [string]$ResultsRoot,

        [object[]]$Rollups
    )

        function local:ConvertToHtmlSettingBadges {
                param(
                        [object[]]$Settings,
                        [hashtable]$PreviousValues
                )

                $badges = New-Object System.Collections.Generic.List[string]
                $normalBadges = New-Object System.Collections.Generic.List[string]
                $changedBadges = New-Object System.Collections.Generic.List[string]

                foreach ($setting in $Settings) {
                        if ($null -eq $setting -or [string]::IsNullOrWhiteSpace([string]$setting.Value)) {
                                continue
                        }

                        $label = if ($setting.PSObject.Properties['Label']) { [string]$setting.Label } else { [string]$setting.Key }
                        $value = [string]$setting.Value
                        $className = 'setting-badge'
                        $title = '{0}={1}' -f $label, $value
                        $isChanged = $false

                        if ($null -ne $PreviousValues -and $PreviousValues.ContainsKey([string]$setting.Key)) {
                                $previousValue = [string]$PreviousValues[[string]$setting.Key]
                                if ($previousValue -ne $value) {
                                        $className += ' setting-badge-changed'
                                        $title = '{0} changed from {1} to {2}' -f $label, $previousValue, $value
                                        $isChanged = $true
                                }
                        }

                        $badgeHtml = @"
<span class='$className' title='$([System.Net.WebUtility]::HtmlEncode($title))'>
    <span class='setting-key'>$([System.Net.WebUtility]::HtmlEncode($label))</span>
    <span class='setting-value'>$([System.Net.WebUtility]::HtmlEncode($value))</span>
</span>
"@

                        if ($isChanged) {
                                $changedBadges.Add($badgeHtml)
                        }
                        else {
                                $normalBadges.Add($badgeHtml)
                        }
                }

                foreach ($badge in $changedBadges) {
                        $badges.Add($badge)
                }
                foreach ($badge in $normalBadges) {
                        $badges.Add($badge)
                }

                if ($badges.Count -eq 0) {
                        return "<span class='setting-badge setting-badge-empty'>No settings captured</span>"
                }

                return ($badges -join [Environment]::NewLine)
        }

        function local:GetSettingsChangeSummary {
                param(
                        [object[]]$Settings,
                        [hashtable]$PreviousValues
                )

                if ($null -eq $PreviousValues) {
                        return 'Baseline settings'
                }

                $changes = New-Object System.Collections.Generic.List[string]
                foreach ($setting in $Settings) {
                        if ($null -eq $setting -or [string]::IsNullOrWhiteSpace([string]$setting.Value)) {
                                continue
                        }

                        $key = [string]$setting.Key
                        if (-not $PreviousValues.ContainsKey($key)) {
                                continue
                        }

                        $previousValue = [string]$PreviousValues[$key]
                        $currentValue = [string]$setting.Value
                        if ($previousValue -ne $currentValue) {
                                $label = if ($setting.PSObject.Properties['Label']) { [string]$setting.Label } else { $key }
                                $changes.Add(('{0}: {1} -> {2}' -f $label, $previousValue, $currentValue))
                        }
                }

                if ($changes.Count -eq 0) {
                        return 'Settings unchanged'
                }

                return ('Settings changed: ' + ($changes -join ', '))
        }

        function local:NewProfileComparisonSection {
                param(
                        [string]$Profile,
                        [object[]]$ProfileRuns
                )

                $orderedProfileRuns = @($ProfileRuns | Sort-Object TimestampUtc)
                $recentProfileRuns = @($orderedProfileRuns | Select-Object -Last 6)
                $maxRead = @($recentProfileRuns | ForEach-Object { $_.Read.BandwidthMBps } | Where-Object { $null -ne $_ } | Measure-Object -Maximum).Maximum
                $maxWrite = @($recentProfileRuns | ForEach-Object { $_.Write.BandwidthMBps } | Where-Object { $null -ne $_ } | Measure-Object -Maximum).Maximum
                if ($null -eq $maxRead -or $maxRead -le 0) { $maxRead = 1 }
                if ($null -eq $maxWrite -or $maxWrite -le 0) { $maxWrite = 1 }

                $rows = New-Object System.Collections.Generic.List[string]
                for ($index = 0; $index -lt $recentProfileRuns.Count; $index++) {
                        $run = $recentProfileRuns[$index]
                        $previous = if ($index -gt 0) { $recentProfileRuns[$index - 1] } else { $null }
                        $readP99Ms = if ($null -ne $run.Read.P99LatencyUs) { [math]::Round(($run.Read.P99LatencyUs / 1000.0), 2) } else { $null }
                        $writeP99Ms = if ($null -ne $run.Write.P99LatencyUs) { [math]::Round(($run.Write.P99LatencyUs / 1000.0), 2) } else { $null }
                        $previousReadP99Ms = if ($null -ne $previous -and $null -ne $previous.Read.P99LatencyUs) { [math]::Round(($previous.Read.P99LatencyUs / 1000.0), 2) } else { $null }
                        $previousWriteP99Ms = if ($null -ne $previous -and $null -ne $previous.Write.P99LatencyUs) { [math]::Round(($previous.Write.P99LatencyUs / 1000.0), 2) } else { $null }
                        $readDelta = Format-FioHtmlDelta -Current $run.Read.BandwidthMBps -Previous $(if ($null -ne $previous) { $previous.Read.BandwidthMBps } else { $null }) -Suffix ' MB/s'
                        $writeDelta = Format-FioHtmlDelta -Current $run.Write.BandwidthMBps -Previous $(if ($null -ne $previous) { $previous.Write.BandwidthMBps } else { $null }) -Suffix ' MB/s'
                        $readLatencyDelta = Format-FioHtmlDelta -Current $readP99Ms -Previous $previousReadP99Ms -Suffix ' ms' -LowerIsBetter
                        $writeLatencyDelta = Format-FioHtmlDelta -Current $writeP99Ms -Previous $previousWriteP99Ms -Suffix ' ms' -LowerIsBetter
                        $readWidth = [math]::Round((($run.Read.BandwidthMBps / $maxRead) * 100.0), 2)
                        $writeWidth = [math]::Round((($run.Write.BandwidthMBps / $maxWrite) * 100.0), 2)

                        $runSettings = @(
                                [pscustomobject]@{ Key = 'BlockSize'; Label = 'bs'; Value = $run.BlockSize }
                                [pscustomobject]@{ Key = 'QueueDepth'; Label = 'qd'; Value = if ($null -ne $run.QueueDepth) { [string]$run.QueueDepth } else { $null } }
                                [pscustomobject]@{ Key = 'NumJobs'; Label = 'jobs'; Value = if ($null -ne $run.NumJobs) { [string]$run.NumJobs } else { $null } }
                                [pscustomobject]@{ Key = 'FileSizeGB'; Label = 'size'; Value = if ($null -ne $run.FileSizeGB) { '{0} GB' -f ([math]::Round([double]$run.FileSizeGB, 2)) } else { $null } }
                                [pscustomobject]@{ Key = 'ReadWrite'; Label = 'rw'; Value = $run.ReadWrite }
                                [pscustomobject]@{ Key = 'ReadMix'; Label = 'mix'; Value = if ($null -ne $run.ReadMix) { '{0}/{1}' -f $run.ReadMix, (100 - [int]$run.ReadMix) } else { $null } }
                                [pscustomobject]@{ Key = 'Direct'; Label = 'direct'; Value = if ($null -ne $run.Direct) { [string]$run.Direct } else { $null } }
                                [pscustomobject]@{ Key = 'Fsync'; Label = 'fsync'; Value = if ($null -ne $run.Fsync -and $run.Fsync -gt 0) { [string]$run.Fsync } else { $null } }
                                [pscustomobject]@{ Key = 'RuntimeSec'; Label = 'runtime'; Value = if ($null -ne $run.RuntimeSec) { '{0}s' -f $run.RuntimeSec } else { $null } }
                        )
                        $targetSettings = @(
                                [pscustomobject]@{ Key = 'IterationCount'; Label = 'iters'; Value = if ($null -ne $run.IterationCount) { [string]$run.IterationCount } else { $null } }
                                [pscustomobject]@{ Key = 'FioVersion'; Label = 'fio'; Value = $run.FioVersion }
                                [pscustomobject]@{ Key = 'TargetType'; Label = 'type'; Value = $run.TargetType }
                            [pscustomobject]@{ Key = 'SmbServerName'; Label = 'server'; Value = if ($run.SmbMetadata) { $run.SmbMetadata.ServerName } else { $null } }
                            [pscustomobject]@{ Key = 'SmbShareName'; Label = 'share'; Value = if ($run.SmbMetadata) { $run.SmbMetadata.ShareName } else { $null } }
                            [pscustomobject]@{ Key = 'SmbDialect'; Label = 'dialect'; Value = if ($run.SmbMetadata) { $run.SmbMetadata.Dialect } else { $null } }
                            [pscustomobject]@{ Key = 'SmbContinuouslyAvailable'; Label = 'ca'; Value = if ($run.SmbMetadata -and $null -ne $run.SmbMetadata.ContinuouslyAvailable) { [string]$run.SmbMetadata.ContinuouslyAvailable } else { $null } }
                            [pscustomobject]@{ Key = 'SmbEncryptData'; Label = 'encrypt'; Value = if ($run.SmbMetadata -and $null -ne $run.SmbMetadata.EncryptData) { [string]$run.SmbMetadata.EncryptData } else { $null } }
                            [pscustomobject]@{ Key = 'SmbMultichannelPathCount'; Label = 'channels'; Value = if ($run.SmbMetadata -and $null -ne $run.SmbMetadata.MultichannelPathCount) { [string]$run.SmbMetadata.MultichannelPathCount } else { $null } }
                            [pscustomobject]@{ Key = 'SmbRdmaPathCount'; Label = 'rdma'; Value = if ($run.SmbMetadata -and $null -ne $run.SmbMetadata.RdmaPathCount) { [string]$run.SmbMetadata.RdmaPathCount } else { $null } }
                        )

                        $previousRunSettings = $null
                        $previousTargetSettings = $null
                        if ($null -ne $previous) {
                                $previousRunSettings = @{
                                        BlockSize = $previous.BlockSize
                                        QueueDepth = if ($null -ne $previous.QueueDepth) { [string]$previous.QueueDepth } else { $null }
                                        NumJobs = if ($null -ne $previous.NumJobs) { [string]$previous.NumJobs } else { $null }
                                        FileSizeGB = if ($null -ne $previous.FileSizeGB) { '{0} GB' -f ([math]::Round([double]$previous.FileSizeGB, 2)) } else { $null }
                                        ReadWrite = $previous.ReadWrite
                                        ReadMix = if ($null -ne $previous.ReadMix) { '{0}/{1}' -f $previous.ReadMix, (100 - [int]$previous.ReadMix) } else { $null }
                                        Direct = if ($null -ne $previous.Direct) { [string]$previous.Direct } else { $null }
                                        Fsync = if ($null -ne $previous.Fsync -and $previous.Fsync -gt 0) { [string]$previous.Fsync } else { $null }
                                        RuntimeSec = if ($null -ne $previous.RuntimeSec) { '{0}s' -f $previous.RuntimeSec } else { $null }
                                }
                                $previousTargetSettings = @{
                                        IterationCount = if ($null -ne $previous.IterationCount) { [string]$previous.IterationCount } else { $null }
                                        FioVersion = $previous.FioVersion
                                        TargetType = $previous.TargetType
                                    SmbServerName = if ($previous.SmbMetadata) { $previous.SmbMetadata.ServerName } else { $null }
                                    SmbShareName = if ($previous.SmbMetadata) { $previous.SmbMetadata.ShareName } else { $null }
                                    SmbDialect = if ($previous.SmbMetadata) { $previous.SmbMetadata.Dialect } else { $null }
                                    SmbContinuouslyAvailable = if ($previous.SmbMetadata -and $null -ne $previous.SmbMetadata.ContinuouslyAvailable) { [string]$previous.SmbMetadata.ContinuouslyAvailable } else { $null }
                                    SmbEncryptData = if ($previous.SmbMetadata -and $null -ne $previous.SmbMetadata.EncryptData) { [string]$previous.SmbMetadata.EncryptData } else { $null }
                                    SmbMultichannelPathCount = if ($previous.SmbMetadata -and $null -ne $previous.SmbMetadata.MultichannelPathCount) { [string]$previous.SmbMetadata.MultichannelPathCount } else { $null }
                                    SmbRdmaPathCount = if ($previous.SmbMetadata -and $null -ne $previous.SmbMetadata.RdmaPathCount) { [string]$previous.SmbMetadata.RdmaPathCount } else { $null }
                                }
                        }

                        $settingsBadgeHtml = ConvertToHtmlSettingBadges -Settings $runSettings -PreviousValues $previousRunSettings
                        $targetBadgeHtml = ConvertToHtmlSettingBadges -Settings $targetSettings -PreviousValues $previousTargetSettings
                        $runChangeSummary = GetSettingsChangeSummary -Settings $runSettings -PreviousValues $previousRunSettings
                        $targetChangeSummary = GetSettingsChangeSummary -Settings $targetSettings -PreviousValues $previousTargetSettings

                        $rows.Add(@"
<tr>
    <td>
        <div class='run-date'>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlTimestamp -TimestampUtc $run.TimestampUtc)))</div>
        <div class='subtle'>$([System.Net.WebUtility]::HtmlEncode([string]$run.RunId))</div>
        <div class='settings-summary'>$([System.Net.WebUtility]::HtmlEncode($runChangeSummary))</div>
        <div class='settings-badges'>
            $settingsBadgeHtml
        </div>
    </td>
    <td>
        <div>$([System.Net.WebUtility]::HtmlEncode([string]$run.TargetType))</div>
        <div class='subtle'>$([System.Net.WebUtility]::HtmlEncode([string]$run.TargetPath))</div>
        <div class='settings-summary'>$([System.Net.WebUtility]::HtmlEncode($targetChangeSummary))</div>
        <div class='settings-badges'>
            $targetBadgeHtml
        </div>
    </td>
    <td>
        <div class='metric-cell'>
            <span>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $run.Read.BandwidthMBps -Suffix ' MB/s')))</span>
            <div class='mini-track'><div class='mini-fill throughput-read' style='width: ${readWidth}%'></div></div>
        </div>
        <div class='delta $($readDelta.Class)'>$([System.Net.WebUtility]::HtmlEncode($readDelta.Text))</div>
    </td>
    <td>
        <div class='metric-cell'>
            <span>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $run.Write.BandwidthMBps -Suffix ' MB/s')))</span>
            <div class='mini-track'><div class='mini-fill throughput-write' style='width: ${writeWidth}%'></div></div>
        </div>
        <div class='delta $($writeDelta.Class)'>$([System.Net.WebUtility]::HtmlEncode($writeDelta.Text))</div>
    </td>
    <td>
        <div>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $readP99Ms -Suffix ' ms')))</div>
        <div class='delta $($readLatencyDelta.Class)'>$([System.Net.WebUtility]::HtmlEncode($readLatencyDelta.Text))</div>
    </td>
    <td>
        <div>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $writeP99Ms -Suffix ' ms')))</div>
        <div class='delta $($writeLatencyDelta.Class)'>$([System.Net.WebUtility]::HtmlEncode($writeLatencyDelta.Text))</div>
    </td>
</tr>
"@)
                }

                return @"
<section class='table-card profile-card'>
    <div class='section-heading'>
        <div>
            <h2>$([System.Net.WebUtility]::HtmlEncode($Profile))</h2>
            <p>Recent runs for this workload profile, with each row compared against the previous run in the same profile.</p>
        </div>
        <div class='pill'>$($recentProfileRuns.Count) runs shown</div>
    </div>
    <table>
        <thead>
            <tr>
                <th>Run Time</th>
                <th>Target</th>
                <th>Read Throughput</th>
                <th>Write Throughput</th>
                <th>Read P99</th>
                <th>Write P99</th>
            </tr>
        </thead>
        <tbody>
            $($rows -join [Environment]::NewLine)
        </tbody>
    </table>
</section>
"@
        }

    if ($Runs.Count -eq 0) {
        throw 'Cannot build an HTML report without any runs.'
    }

    $orderedRuns = @($Runs | Sort-Object TimestampUtc)
    $profileCount = @($orderedRuns.Profile | Sort-Object -Unique).Count
    $smbRuns = @($orderedRuns | Where-Object { $_.TargetType -eq 'Smb' }).Count
    $localRuns = @($orderedRuns | Where-Object { $_.TargetType -eq 'Local' }).Count
    $rollupRows = if ($null -ne $Rollups) { @($Rollups) } else { @(Get-FioHistoricalRollup -Runs $orderedRuns) }
    $profileSections = @(
        foreach ($profileGroup in ($orderedRuns | Group-Object Profile | Sort-Object Name)) {
            NewProfileComparisonSection -Profile $profileGroup.Name -ProfileRuns @($profileGroup.Group)
        }
    )

    $runTableRows = New-Object System.Collections.Generic.List[string]
    foreach ($run in ($orderedRuns | Sort-Object TimestampUtc -Descending)) {
        $readMeanMs = if ($null -ne $run.Read.MeanLatencyUs) { [math]::Round(($run.Read.MeanLatencyUs / 1000.0), 2) } else { $null }
        $writeMeanMs = if ($null -ne $run.Write.MeanLatencyUs) { [math]::Round(($run.Write.MeanLatencyUs / 1000.0), 2) } else { $null }
        $runTableRows.Add(@"
<tr>
    <td>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlTimestamp -TimestampUtc $run.TimestampUtc)))</td>
  <td>$([System.Net.WebUtility]::HtmlEncode([string]$run.Profile))</td>
  <td>$([System.Net.WebUtility]::HtmlEncode([string]$run.TargetType))</td>
  <td>$([System.Net.WebUtility]::HtmlEncode([string]$run.TargetPath))</td>
  <td>$([System.Net.WebUtility]::HtmlEncode([string]$run.IterationCount))</td>
  <td>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $run.Read.BandwidthMBps -Suffix ' MB/s')))</td>
  <td>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $run.Write.BandwidthMBps -Suffix ' MB/s')))</td>
  <td>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $readMeanMs -Suffix ' ms')))</td>
  <td>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $writeMeanMs -Suffix ' ms')))</td>
  <td>$([System.Net.WebUtility]::HtmlEncode([string]$run.ResultDirectory))</td>
</tr>
"@)
    }

    $rollupTableRows = New-Object System.Collections.Generic.List[string]
    foreach ($rollup in ($rollupRows | Sort-Object Profile, TargetType, TargetPath)) {
        $rollupTableRows.Add(@"
<tr>
  <td>$([System.Net.WebUtility]::HtmlEncode([string]$rollup.Profile))</td>
  <td>$([System.Net.WebUtility]::HtmlEncode([string]$rollup.TargetType))</td>
  <td>$([System.Net.WebUtility]::HtmlEncode([string]$rollup.TargetPath))</td>
  <td>$([System.Net.WebUtility]::HtmlEncode([string]$rollup.RunCount))</td>
  <td>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $rollup.ReadBandwidthMBps.Avg -Suffix ' MB/s')))</td>
  <td>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $rollup.WriteBandwidthMBps.Avg -Suffix ' MB/s')))</td>
  <td>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $rollup.ReadMeanLatencyMs.Avg -Suffix ' ms')))</td>
  <td>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $rollup.WriteMeanLatencyMs.Avg -Suffix ' ms')))</td>
    <td>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlTimestamp -TimestampUtc $rollup.LastTimestampUtc)))</td>
</tr>
"@)
    }

    $html = @"
<!DOCTYPE html>
<html lang='en'>
<head>
  <meta charset='utf-8' />
  <meta name='viewport' content='width=device-width, initial-scale=1' />
  <title>$([System.Net.WebUtility]::HtmlEncode($Title))</title>
  <style>
    :root {
      --bg: #f4f1e8;
      --panel: #fffdf8;
      --ink: #1e2a2f;
      --muted: #65737a;
      --line: #d9d0bd;
      --accent: #0f766e;
      --accent-2: #b45309;
      --accent-3: #1d4ed8;
      --shadow: rgba(30, 42, 47, 0.08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: 'Segoe UI', Tahoma, sans-serif;
      color: var(--ink);
      background: linear-gradient(180deg, #efe7d6 0%, var(--bg) 220px);
    }
    main { max-width: 1400px; margin: 0 auto; padding: 32px 24px 48px; }
    h1, h2 { margin: 0 0 12px; }
    p { margin: 0; color: var(--muted); }
    .hero, .card, .chart-card, .table-card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: 0 12px 32px var(--shadow);
    }
    .hero { padding: 24px; margin-bottom: 20px; }
        .hero-grid, .metric-grid, .chart-grid { display: grid; gap: 16px; }
    .hero-grid { grid-template-columns: 2fr 1fr; align-items: end; }
    .metric-grid { grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); margin-top: 20px; }
    .chart-grid { grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); margin: 20px 0; }
    .card { padding: 18px; }
    .metric-label { font-size: 12px; letter-spacing: 0.08em; text-transform: uppercase; color: var(--muted); }
    .metric-value { font-size: 28px; font-weight: 700; margin-top: 6px; }
    .meta { display: flex; flex-wrap: wrap; gap: 16px; font-size: 13px; margin-top: 14px; color: var(--muted); }
    .chart-card, .table-card { padding: 18px; }
        .profile-grid { display: grid; gap: 18px; margin: 20px 0; }
        .profile-card { overflow-x: auto; }
        .section-heading {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 12px;
            margin-bottom: 8px;
        }
        .pill {
            border: 1px solid var(--line);
            border-radius: 999px;
            padding: 6px 10px;
            font-size: 12px;
            color: var(--muted);
            white-space: nowrap;
        }
    .bar-row {
      display: grid;
      grid-template-columns: minmax(140px, 1.2fr) 3fr minmax(90px, 0.8fr);
      gap: 12px;
      align-items: center;
      margin-top: 12px;
    }
    .bar-label, .bar-value { font-size: 13px; }
    .bar-track {
      height: 14px;
      background: #ece6d8;
      border-radius: 999px;
      overflow: hidden;
    }
    .bar-fill {
      height: 100%;
      border-radius: 999px;
      background: var(--accent);
    }
        .mini-track {
            height: 8px;
            background: #ece6d8;
            border-radius: 999px;
            overflow: hidden;
            min-width: 84px;
        }
        .mini-fill {
            height: 100%;
            border-radius: 999px;
        }
        .metric-cell {
            display: grid;
            grid-template-columns: auto 1fr;
            gap: 10px;
            align-items: center;
        }
        .delta {
            margin-top: 4px;
            font-size: 12px;
            font-weight: 600;
        }
        .delta-good { color: #0f766e; }
        .delta-bad { color: #b42318; }
        .delta-neutral { color: var(--muted); }
        .run-date { font-weight: 600; }
        .subtle {
            font-size: 12px;
            color: var(--muted);
            margin-top: 3px;
            word-break: break-word;
        }
        .settings-badges {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
            margin-top: 8px;
        }
        .settings-summary {
            margin-top: 8px;
            font-size: 11px;
            color: #7c5e10;
            background: #fff7e6;
            border: 1px solid #f1d39b;
            border-radius: 10px;
            padding: 6px 8px;
            line-height: 1.35;
        }
        .setting-badge {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            border: 1px solid var(--line);
            border-radius: 999px;
            padding: 3px 8px;
            font-size: 11px;
            line-height: 1.3;
            color: var(--muted);
            background: #faf6ed;
        }
        .setting-badge-changed {
            border-color: #d97706;
            background: #fff3dc;
            color: #9a3412;
        }
        .setting-badge-empty {
            background: #f5f5f4;
        }
        .setting-key {
            text-transform: uppercase;
            letter-spacing: 0.04em;
            font-weight: 700;
            font-size: 10px;
        }
        .setting-value {
            font-weight: 600;
        }
    .throughput-read { background: linear-gradient(90deg, #0f766e, #14b8a6); }
    .throughput-write { background: linear-gradient(90deg, #b45309, #f59e0b); }
    .latency-read { background: linear-gradient(90deg, #1d4ed8, #60a5fa); }
    .latency-write { background: linear-gradient(90deg, #7c3aed, #c084fc); }
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 12px;
      font-size: 13px;
    }
    th, td {
      text-align: left;
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }
    th { color: var(--muted); font-weight: 600; }
    @media (max-width: 900px) {
      .hero-grid { grid-template-columns: 1fr; }
      .bar-row { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <main>
    <section class='hero'>
      <div class='hero-grid'>
        <div>
          <h1>$([System.Net.WebUtility]::HtmlEncode($Title))</h1>
                    <p>Self-contained HTML report for fio SQL-style benchmarks, with recent comparisons grouped by workload profile and deltas against the previous run in each group.</p>
          <div class='meta'>
            <span>Generated: $([System.Net.WebUtility]::HtmlEncode(([DateTime]::UtcNow.ToString('u'))))</span>
            <span>Results root: $([System.Net.WebUtility]::HtmlEncode([string]$ResultsRoot))</span>
          </div>
        </div>
        <div class='metric-grid'>
          <div class='card'><div class='metric-label'>Runs</div><div class='metric-value'>$($orderedRuns.Count)</div></div>
          <div class='card'><div class='metric-label'>Profiles</div><div class='metric-value'>$profileCount</div></div>
          <div class='card'><div class='metric-label'>SMB Runs</div><div class='metric-value'>$smbRuns</div></div>
          <div class='card'><div class='metric-label'>Local Runs</div><div class='metric-value'>$localRuns</div></div>
        </div>
      </div>
    </section>
        <div class='profile-grid'>
            $($profileSections -join [Environment]::NewLine)
        </div>
    <section class='table-card'>
      <h2>Historical Rollups</h2>
      <table>
        <thead>
          <tr>
            <th>Profile</th>
            <th>Target Type</th>
            <th>Target Path</th>
            <th>Runs</th>
            <th>Read MB/s Avg</th>
            <th>Write MB/s Avg</th>
            <th>Read Mean ms Avg</th>
            <th>Write Mean ms Avg</th>
            <th>Last Run</th>
          </tr>
        </thead>
        <tbody>
          $($rollupTableRows -join [Environment]::NewLine)
        </tbody>
      </table>
    </section>
    <section class='table-card' style='margin-top: 20px;'>
      <h2>Runs</h2>
      <table>
        <thead>
          <tr>
            <th>Timestamp</th>
            <th>Profile</th>
            <th>Target Type</th>
            <th>Target Path</th>
            <th>Iters</th>
            <th>Read MB/s</th>
            <th>Write MB/s</th>
            <th>Read Mean ms</th>
            <th>Write Mean ms</th>
            <th>Result Directory</th>
          </tr>
        </thead>
        <tbody>
          $($runTableRows -join [Environment]::NewLine)
        </tbody>
      </table>
    </section>
  </main>
</body>
</html>
"@

    $html | Set-Content -Path $Path -Encoding utf8
}

Export-ModuleMember -Function Resolve-FioSqlBenchTarget, Get-FioSqlBenchProfileDefaults, Merge-FioSqlBenchSettings, New-FioSqlBenchRunContext, New-FioSqlBenchJobContent, Get-FioBenchFilePaths, Test-FioPreparedFiles, Resolve-FioBinary, Invoke-FioSqlBenchRun, ConvertFrom-FioJsonToSummary, Export-FioSqlBenchCsv, Import-FioSqlBenchHistory, Get-FioHistoricalRollup, Export-FioSqlBenchHistoricalCsv, Export-FioSqlBenchHtmlReport, New-FioHtmlProfileComparisonSection, ConvertTo-FioHtmlSettingBadges
