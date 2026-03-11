Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

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

    if ([string]::IsNullOrWhiteSpace($psDrive.DisplayRoot) -or -not $psDrive.DisplayRoot.StartsWith('\\')) {
        return $null
    }

    $remoteRoot = $psDrive.DisplayRoot.TrimEnd('\\')
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

    $parsePath = if ($null -ne $MappedSmbInfo) { $MappedSmbInfo.RemotePath } else { $Path }
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

    $connection = $null
    try {
        $connection = Get-SmbConnection -ErrorAction Stop | Where-Object {
            $_.ServerName -eq $server -and $_.ShareName -eq $share
        } | Select-Object -First 1
    }
    catch {
        $connection = $null
    }

    [pscustomobject]@{
        ServerName = $server
        ShareName = $share
        RemotePath = $parsePath
        MappedDrive = if ($null -ne $MappedSmbInfo) { $MappedSmbInfo.DriveRoot } else { $null }
        Credential = if ($connection) { $connection.UserName } else { $null }
        Dialect = if ($connection) { $connection.Dialect } else { $null }
        NumOpens = if ($connection) { $connection.NumOpens } else { $null }
    }
}

function Get-FioSqlBenchProfileDefaults {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [ValidateSet('Data', 'Log', 'Tempdb')]
        [string]$Profile
    )

    switch ($Profile) {
        'Data' {
            return [ordered]@{
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
                DirectSmb = 0
                ReadWrite = 'randrw'
            }
        }
        'Log' {
            return [ordered]@{
                FileSizeGB = 8
                RuntimeSec = 60
                RampSec = 10
                Iterations = 1
                QueueDepth = 1
                NumJobs = 1
                BlockSize = '64k'
                ReadMix = $null
                Fsync = 1
                DirectLocal = 0
                DirectSmb = 0
                ReadWrite = 'write'
            }
        }
        'Tempdb' {
            return [ordered]@{
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
                DirectSmb = 0
                ReadWrite = 'randrw'
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

        [pscustomobject]@{
            Iops = [double]$operation.iops
            BwBytes = [double]$operation.bw_bytes
            IoBytes = [double]$operation.io_bytes
            MeanLatencyUs = Get-FioMeanLatencyUs -Operation $operation
            Percentiles = Get-FioLatencyPercentilesUs -Operation $operation
        }
    }

    if (-not $opStats) {
        return [pscustomobject]@{
            Iops = 0
            BandwidthMBps = 0
            IoMB = 0
            MeanLatencyUs = $null
            P50LatencyUs = $null
            P95LatencyUs = $null
            P99LatencyUs = $null
            P999LatencyUs = $null
        }
    }

    $validLatency = @($opStats | Where-Object { $null -ne $_.MeanLatencyUs })
    $percentileStats = @($opStats | Where-Object { $null -ne $_.Percentiles })

    [pscustomobject]@{
        Iops = [math]::Round((($opStats | Measure-Object -Property Iops -Sum).Sum), 2)
        BandwidthMBps = [math]::Round((((($opStats | Measure-Object -Property BwBytes -Sum).Sum) / 1MB)), 2)
        IoMB = [math]::Round((((($opStats | Measure-Object -Property IoBytes -Sum).Sum) / 1MB)), 2)
        MeanLatencyUs = if ($validLatency.Count -gt 0) { [math]::Round((($validLatency | Measure-Object -Property MeanLatencyUs -Average).Average), 2) } else { $null }
        P50LatencyUs = Get-AggregatedPercentile -Stats $percentileStats -Name 'P50'
        P95LatencyUs = Get-AggregatedPercentile -Stats $percentileStats -Name 'P95'
        P99LatencyUs = Get-AggregatedPercentile -Stats $percentileStats -Name 'P99'
        P999LatencyUs = Get-AggregatedPercentile -Stats $percentileStats -Name 'P999'
    }
}

function Get-AggregatedPercentile {
    [CmdletBinding()]
    param(
        [object[]]$Stats,

        [Parameter(Mandatory)]
        [string]$Name
    )

    if ($null -eq $Stats -or $Stats.Count -eq 0) {
        return $null
    }

    $values = @($Stats | ForEach-Object { $_.Percentiles.$Name } | Where-Object { $null -ne $_ })
    if ($values.Count -eq 0) {
        return $null
    }

    [math]::Round((($values | Measure-Object -Maximum).Maximum), 2)
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
            WriteIops = $summary.Write.Iops
            WriteBandwidthMBps = $summary.Write.BandwidthMBps
            WriteMeanLatencyUs = $summary.Write.MeanLatencyUs
            WriteP50LatencyUs = $summary.Write.P50LatencyUs
            WriteP95LatencyUs = $summary.Write.P95LatencyUs
            WriteP99LatencyUs = $summary.Write.P99LatencyUs
            WriteP999LatencyUs = $summary.Write.P999LatencyUs
        }
    }

    $rows | Export-Csv -Path $Path -NoTypeInformation -Encoding utf8
}

Export-ModuleMember -Function Resolve-FioSqlBenchTarget, Get-FioSqlBenchProfileDefaults, Merge-FioSqlBenchSettings, New-FioSqlBenchRunContext, New-FioSqlBenchJobContent, Get-FioBenchFilePaths, Test-FioPreparedFiles, Resolve-FioBinary, Invoke-FioSqlBenchRun, ConvertFrom-FioJsonToSummary, Export-FioSqlBenchCsv
