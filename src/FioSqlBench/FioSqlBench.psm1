Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$script:FioDiagnosticsWindowMs = 1000
$script:FioDiagnosticsStabilityWindowSamples = 5
$script:FioDiagnosticsWarmupTrimSamples = 5
$script:FioDiagnosticsSmoothingWindowSamples = 5
$script:FioConvertFromJsonSupportsDepth = $null

function ConvertFrom-FioJsonDocument {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$InputObject
    )

    if ($null -eq $script:FioConvertFromJsonSupportsDepth) {
        $convertFromJsonCommand = Get-Command -Name 'ConvertFrom-Json' -ErrorAction Stop
        $script:FioConvertFromJsonSupportsDepth = $convertFromJsonCommand.Parameters.ContainsKey('Depth')
    }

    if ($script:FioConvertFromJsonSupportsDepth) {
        return ($InputObject | ConvertFrom-Json -Depth 100)
    }

    return ($InputObject | ConvertFrom-Json)
}

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
    [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSAvoidAssignmentToAutomaticVariable', 'Profile', Justification = 'The public function intentionally exposes -Profile.')]
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [ValidateSet('Data', 'Log', 'Tempdb', 'BackupRestore', 'DbccScan', 'MaxThroughput', 'MaxIOPs', 'All')]
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
        'MaxThroughput' {
            return [ordered]@{
                ProfileName = 'MaxThroughput'
                FileSizeGB = 64
                RuntimeSec = 120
                RampSec = 10
                Iterations = 1
                QueueDepth = 32
                NumJobs = 4
                BlockSize = '1m'
                ReadMix = $null
                Fsync = 0
                DirectLocal = 1
                DirectSmb = 1
                ReadWrite = 'read+write'
            }
        }
        'MaxIOPs' {
            return [ordered]@{
                ProfileName = 'MaxIOPs'
                FileSizeGB = 32
                RuntimeSec = 120
                RampSec = 10
                Iterations = 1
                QueueDepth = 64
                NumJobs = 8
                BlockSize = '4k'
                ReadMix = $null
                Fsync = 0
                DirectLocal = 1
                DirectSmb = 1
                ReadWrite = 'randread+randwrite'
            }
        }
        'All' {
            return [ordered]@{
                ProfileName = 'All'
                ChildProfiles = @('MaxThroughput', 'Data', 'DbccScan', 'BackupRestore', 'MaxIOPs', 'Tempdb', 'Log')
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
        ProfileName = [string]$ProfileDefaults.ProfileName
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
    $settings.CpuAffinity = Get-FioCpuAffinityPlan -Settings ([pscustomobject]$settings)
    [pscustomobject]$settings
}

function Get-FioCpuTopology {
    [CmdletBinding()]
    param()

    $logicalCpuCount = [int][Environment]::ProcessorCount
    $coreCount = $logicalCpuCount
    $socketCount = 1

    try {
        $processors = @(Get-CimInstance -ClassName Win32_Processor -ErrorAction Stop)
        if ($processors.Count -gt 0) {
            $socketCount = $processors.Count

            $coreMeasure = ($processors | Measure-Object -Property NumberOfCores -Sum).Sum
            if ($null -ne $coreMeasure -and [int]$coreMeasure -gt 0) {
                $coreCount = [int]$coreMeasure
            }

            $logicalMeasure = ($processors | Measure-Object -Property NumberOfLogicalProcessors -Sum).Sum
            if ($null -ne $logicalMeasure -and [int]$logicalMeasure -gt 0) {
                $logicalCpuCount = [int]$logicalMeasure
            }
        }
    }
    catch {
    }

    [pscustomobject]@{
        LogicalCpuCount = [int][math]::Max($logicalCpuCount, 1)
        CoreCount = [int][math]::Max($coreCount, 1)
        SocketCount = [int][math]::Max($socketCount, 1)
    }
}

function Get-FioCpuAffinityPlan {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Settings
    )

    $topology = Get-FioCpuTopology
    $profileName = [string]$Settings.ProfileName
    $supportsSplitAffinity = @('Data', 'Tempdb', 'MaxThroughput', 'MaxIOPs') -contains $profileName

    if ($Settings.NumJobs -lt 2) {
        return [pscustomobject]@{
            Enabled = $false
            Policy = $null
            CpuIds = @()
            CpuList = $null
            Description = 'CPU affinity was skipped because the profile runs a single fio worker.'
            Topology = $topology
        }
    }

    if (-not $supportsSplitAffinity) {
        return [pscustomobject]@{
            Enabled = $false
            Policy = $null
            CpuIds = @()
            CpuList = $null
            Description = 'CPU affinity was skipped because this profile is latency-focused or stream-focused rather than worker-scaling focused.'
            Topology = $topology
        }
    }

    if ($topology.LogicalCpuCount -lt 4) {
        return [pscustomobject]@{
            Enabled = $false
            Policy = $null
            CpuIds = @()
            CpuList = $null
            Description = 'CPU affinity was skipped because the machine exposes fewer than four logical CPUs.'
            Topology = $topology
        }
    }

    $affinitizedCpuCount = [int][math]::Min($topology.LogicalCpuCount, [int]$Settings.NumJobs)
    if ($affinitizedCpuCount -lt 2) {
        return [pscustomobject]@{
            Enabled = $false
            Policy = $null
            CpuIds = @()
            CpuList = $null
            Description = 'CPU affinity was skipped because fewer than two logical CPUs would be assigned.'
            Topology = $topology
        }
    }

    $cpuIds = @(0..($affinitizedCpuCount - 1))

    return [pscustomobject]@{
        Enabled = $true
        Policy = 'split'
        CpuIds = $cpuIds
        CpuList = ($cpuIds -join ',')
        Description = 'Split affinity pins multi-job fio workers across the first {0} logical CPUs ({1} logical / {2} cores / {3} sockets detected).' -f $affinitizedCpuCount, $topology.LogicalCpuCount, $topology.CoreCount, $topology.SocketCount
        Topology = $topology
    }
}

function Get-FioCpuAffinitySummary {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Settings
    )

    if (-not $Settings.PSObject.Properties['CpuAffinity'] -or $null -eq $Settings.CpuAffinity) {
        return $null
    }

    [pscustomobject]@{
        Enabled = [bool]$Settings.CpuAffinity.Enabled
        Policy = if ($Settings.CpuAffinity.Policy) { [string]$Settings.CpuAffinity.Policy } else { $null }
        CpuList = if ($Settings.CpuAffinity.CpuList) { [string]$Settings.CpuAffinity.CpuList } else { $null }
        Description = [string]$Settings.CpuAffinity.Description
        LogicalCpuCount = [int]$Settings.CpuAffinity.Topology.LogicalCpuCount
        CoreCount = [int]$Settings.CpuAffinity.Topology.CoreCount
        SocketCount = [int]$Settings.CpuAffinity.Topology.SocketCount
    }
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

function Get-FioPreparationBlockSizeBytes {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Settings
    )

    [int64][math]::Max([int64]1MB, [int64]$Settings.BlockSizeBytes)
}

function Get-FioPreparationQueueDepth {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Settings
    )

    [int][math]::Min([int][math]::Max($Settings.QueueDepth, 1), 8)
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
        [string]$WorkloadProfile,

        [Parameter(Mandatory)]
        [pscustomobject]$Settings
    )

    $timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $label = if ($RunLabel) { $RunLabel } else { $WorkloadProfile.ToLowerInvariant() }
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

    $prepBlockSizeBytes = Get-FioPreparationBlockSizeBytes -Settings $Settings
    $prepQueueDepth = Get-FioPreparationQueueDepth -Settings $Settings

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add('[global]')
    $lines.Add('ioengine=windowsaio')
    $lines.Add('thread=1')
    $lines.Add('group_reporting=1')
    $lines.Add("direct=$($Settings.Direct)")
    $lines.Add(("bs={0}" -f $(if ($Phase -eq 'Prep') { $prepBlockSizeBytes } else { $Settings.BlockSize })))
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

        if ($Settings.PSObject.Properties['CpuAffinity'] -and $Settings.CpuAffinity -and $Settings.CpuAffinity.Enabled -and -not [string]::IsNullOrWhiteSpace([string]$Settings.CpuAffinity.CpuList)) {
            $lines.Add("cpus_allowed=$($Settings.CpuAffinity.CpuList)")
            $lines.Add("cpus_allowed_policy=$($Settings.CpuAffinity.Policy)")
        }
    }

    if ($EnableLogs -and $Phase -ne 'Prep') {
        $logBase = 'fio'
        $lines.Add("write_bw_log=$logBase")
        $lines.Add("write_iops_log=$logBase")
        $lines.Add("write_lat_log=$logBase")
        $lines.Add("log_avg_msec=$script:FioDiagnosticsWindowMs")
        $lines.Add('log_window_value=both')
        $lines.Add('per_job_logs=1')
    }

    if ($Phase -in @('Prep', 'Combined')) {
        for ($jobIndex = 1; $jobIndex -le $Settings.NumJobs; $jobIndex++) {
            $filePath = ConvertTo-FioJobPath -Path (Join-Path -Path $RunContext.TargetRunDirectory -ChildPath ('testfile.{0:D2}.dat' -f $jobIndex))

            $lines.Add('')
            $lines.Add('[prep-{0:D2}]' -f $jobIndex)
            $lines.Add("filename=$filePath")
            $lines.Add('rw=write')
            $lines.Add(("bs={0}" -f $prepBlockSizeBytes))
            $lines.Add(("iodepth={0}" -f $prepQueueDepth))
            $lines.Add('time_based=0')
            $lines.Add('end_fsync=1')
        }
    }

    if ($Phase -in @('Bench', 'Combined')) {
        if (@('MaxIOPs', 'MaxThroughput') -contains $Settings.ProfileName) {
            $operations = switch ($Settings.ProfileName) {
                'MaxIOPs' {
                    @(
                        @{ Label = 'read'; Rw = 'randread'; UseStonewall = $false },
                        @{ Label = 'write'; Rw = 'randwrite'; UseStonewall = $true }
                    )
                }
                'MaxThroughput' {
                    @(
                        @{ Label = 'read'; Rw = 'read'; UseStonewall = $false },
                        @{ Label = 'write'; Rw = 'write'; UseStonewall = $true }
                    )
                }
            }

            foreach ($operation in $operations) {
                for ($jobIndex = 1; $jobIndex -le $Settings.NumJobs; $jobIndex++) {
                    $filePath = ConvertTo-FioJobPath -Path (Join-Path -Path $RunContext.TargetRunDirectory -ChildPath ('testfile.{0:D2}.dat' -f $jobIndex))

                    $lines.Add('')
                    $lines.Add(("[bench-{0}-{1}]" -f $operation.Label, ('{0:D2}' -f $jobIndex)))
                    $lines.Add("filename=$filePath")
                    if ($operation.UseStonewall -and $jobIndex -eq 1) {
                        $lines.Add('stonewall=1')
                    }
                    elseif ($Phase -eq 'Combined' -and -not $operation.UseStonewall -and $jobIndex -eq 1) {
                        $lines.Add('stonewall=1')
                    }
                    $lines.Add("rw=$($operation.Rw)")
                    if ($Settings.Fsync -gt 0) {
                        $lines.Add("fsync=$($Settings.Fsync)")
                    }
                }
            }
        }
        else {
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

    # Prefer a repo-local fio binary when present, then fall back to explicit
    # overrides and machine-level installation discovery.
    $candidates = New-Object System.Collections.Generic.List[string]
    if ($ExplicitPath) {
        $candidates.Add($ExplicitPath)
    }

    foreach ($repoLocalPath in Get-FioRepoBinaryCandidates) {
        $candidates.Add($repoLocalPath)
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

    throw 'Unable to find fio.exe. Add a local copy under tools/fio, install fio, add it to PATH, or pass -FioPath explicitly.'
}

function Get-FioRepoBinaryCandidates {
    [CmdletBinding()]
    param()

    $repoRoot = Split-Path -Path (Split-Path -Path $PSScriptRoot -Parent) -Parent
    $paths = New-Object System.Collections.Generic.List[string]
    foreach ($relativePath in @(
        'tools\fio\windows-x64\fio.exe',
        'tools\fio\fio.exe',
        'vendor\fio\windows-x64\fio.exe',
        'vendor\fio\fio.exe'
    )) {
        $paths.Add((Join-Path -Path $repoRoot -ChildPath $relativePath))
    }

    $paths | Select-Object -Unique
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

    $workingDirectory = Split-Path -Path $OutputJsonPath -Parent
    Push-Location -LiteralPath $workingDirectory
    try {
        $consoleOutput = & $FioPath @arguments 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }

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

    $fio = ConvertFrom-FioJsonDocument -InputObject (Get-Content -LiteralPath $JsonPath -Raw)
    $jobs = @($fio.jobs | Where-Object { $_.jobname -like 'bench*' })
    if ($jobs.Count -eq 0) {
        $jobs = @($fio.jobs)
    }

    $read = Merge-FioOperationStats -Jobs $jobs -OperationName 'read'
    $write = Merge-FioOperationStats -Jobs $jobs -OperationName 'write'
    $diagnostics = Get-FioDiagnosticsSummary -ResultDirectory $RunContext.ResultDirectory -WarmupTrimSamples $script:FioDiagnosticsWarmupTrimSamples -SmoothingWindowSamples $script:FioDiagnosticsSmoothingWindowSamples

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
        CpuAffinity = Get-FioCpuAffinitySummary -Settings $Settings
        AggregateBandwidthMBps = [math]::Round(([double]$read.BandwidthMBps + [double]$write.BandwidthMBps), 2)
        AggregateIops = [math]::Round(([double]$read.Iops + [double]$write.Iops), 2)
        Read = $read
        Write = $write
        Diagnostics = $diagnostics
    }
}

function Get-FioStandardDeviation {
    [CmdletBinding()]
    param(
        [AllowEmptyCollection()]
        [double[]]$Values
    )

    if ($null -eq $Values -or $Values.Count -eq 0) {
        return $null
    }

    $average = ($Values | Measure-Object -Average).Average
    if ($null -eq $average) {
        return $null
    }

    if ($Values.Count -eq 1) {
        return 0.0
    }

    $variance = (($Values | ForEach-Object { [math]::Pow(([double]$_ - [double]$average), 2) } | Measure-Object -Average).Average)
    if ($null -eq $variance) {
        return $null
    }

    return [math]::Sqrt([double]$variance)
}

function Get-FioCoefficientOfVariationFromValues {
    [CmdletBinding()]
    param(
        [AllowEmptyCollection()]
        [double[]]$Values
    )

    if ($null -eq $Values -or $Values.Count -eq 0) {
        return $null
    }

    $mean = ($Values | Measure-Object -Average).Average
    if ($null -eq $mean) {
        return $null
    }

    if ([double]$mean -eq 0) {
        $minValue = ($Values | Measure-Object -Minimum).Minimum
        $maxValue = ($Values | Measure-Object -Maximum).Maximum
        if ($minValue -eq 0 -and $maxValue -eq 0) {
            return 0.0
        }

        return $null
    }

    return Get-FioCoefficientOfVariation -Mean ([double]$mean) -StdDev (Get-FioStandardDeviation -Values $Values)
}

function New-FioDiagnosticsBandwidthStabilitySeries {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object]$Series,

        [int]$WindowSize = $script:FioDiagnosticsStabilityWindowSamples
    )

    $orderedPoints = @($Series.Points | Sort-Object TimeMs)
    if ($orderedPoints.Count -eq 0) {
        return $null
    }

    $rollingPoints = New-Object System.Collections.Generic.List[object]
    for ($index = 0; $index -lt $orderedPoints.Count; $index++) {
        $startIndex = [math]::Max(0, ($index - $WindowSize + 1))
        $windowValues = New-Object System.Collections.Generic.List[double]
        for ($windowIndex = $startIndex; $windowIndex -le $index; $windowIndex++) {
            $windowValues.Add([double]$orderedPoints[$windowIndex].Value)
        }

        $rollingCv = Get-FioCoefficientOfVariationFromValues -Values $windowValues.ToArray()
        if ($null -eq $rollingCv) {
            $rollingCv = 0.0
        }

        $rollingPoints.Add([pscustomobject]@{
            TimeMs = [int]$orderedPoints[$index].TimeMs
            Value = [double]$rollingCv
        })
    }

    $rollingValues = @($rollingPoints | ForEach-Object { [double]$_.Value })
    [pscustomobject]@{
        Key = 'bandwidthstability-{0}' -f ([string]$Series.Direction).ToLowerInvariant()
        Label = '{0} BW Stability' -f [string]$Series.Direction
        Metric = 'bandwidthStability'
        Direction = [string]$Series.Direction
        Unit = 'CV %'
        PreferredDecimals = 2
        Points = $rollingPoints.ToArray()
        Summary = [pscustomobject]@{
            Min = if ($rollingValues.Count -gt 0) { ($rollingValues | Measure-Object -Minimum).Minimum } else { 0 }
            Max = if ($rollingValues.Count -gt 0) { ($rollingValues | Measure-Object -Maximum).Maximum } else { 0 }
            Avg = if ($rollingValues.Count -gt 0) { ($rollingValues | Measure-Object -Average).Average } else { 0 }
            SampleCount = $rollingValues.Count
            CvPercent = if ($Series.Summary -and $Series.Summary.PSObject.Properties['CvPercent']) { $Series.Summary.CvPercent } else { Get-FioCoefficientOfVariationFromValues -Values @($orderedPoints | ForEach-Object { [double]$_.Value }) }
        }
    }
}

function Get-FioDiagnosticsDisplayTransform {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Metric
    )

    switch ($Metric) {
        'bandwidth' {
            return [pscustomobject]@{ TrimLeadingSamples = $script:FioDiagnosticsWarmupTrimSamples; SmoothingWindowSize = $script:FioDiagnosticsSmoothingWindowSamples }
        }
        'iops' {
            return [pscustomobject]@{ TrimLeadingSamples = $script:FioDiagnosticsWarmupTrimSamples; SmoothingWindowSize = $script:FioDiagnosticsSmoothingWindowSamples }
        }
        'latencyAvg' {
            return [pscustomobject]@{ TrimLeadingSamples = $script:FioDiagnosticsWarmupTrimSamples; SmoothingWindowSize = 3 }
        }
        'latencyMax' {
            return [pscustomobject]@{ TrimLeadingSamples = $script:FioDiagnosticsWarmupTrimSamples; SmoothingWindowSize = 3 }
        }
        'perfmonLatency' {
            return [pscustomobject]@{ TrimLeadingSamples = $script:FioDiagnosticsWarmupTrimSamples; SmoothingWindowSize = 3 }
        }
        default {
            return [pscustomobject]@{ TrimLeadingSamples = $script:FioDiagnosticsWarmupTrimSamples; SmoothingWindowSize = 1 }
        }
    }
}

function Get-FioDiagnosticsSmoothedPoints {
    [CmdletBinding()]
    param(
        [AllowEmptyCollection()]
        [object[]]$Points,

        [int]$TrimLeadingSamples = $script:FioDiagnosticsWarmupTrimSamples,

        [int]$SmoothingWindowSize = 1
    )

    $orderedPoints = @($Points | Sort-Object TimeMs)
    if ($orderedPoints.Count -eq 0) {
        return @()
    }

    $effectiveTrim = [math]::Max(0, [math]::Min($TrimLeadingSamples, [math]::Max(0, $orderedPoints.Count - 1)))
    $trimmedPoints = @($orderedPoints | Select-Object -Skip $effectiveTrim)
    if ($trimmedPoints.Count -eq 0) {
        $trimmedPoints = @($orderedPoints)
    }

    $effectiveWindow = [math]::Max(1, $SmoothingWindowSize)
    if ($effectiveWindow -eq 1) {
        return @(
            foreach ($point in $trimmedPoints) {
                [pscustomobject]@{
                    TimeMs = [int]$point.TimeMs
                    Value = [double]$point.Value
                }
            }
        )
    }

    $smoothedPoints = New-Object System.Collections.Generic.List[object]
    for ($index = 0; $index -lt $trimmedPoints.Count; $index++) {
        $startIndex = [math]::Max(0, ($index - $effectiveWindow + 1))
        $windowValues = @(
            for ($windowIndex = $startIndex; $windowIndex -le $index; $windowIndex++) {
                [double]$trimmedPoints[$windowIndex].Value
            }
        )

        $smoothedPoints.Add([pscustomobject]@{
            TimeMs = [int]$trimmedPoints[$index].TimeMs
            Value = [double](($windowValues | Measure-Object -Average).Average)
        })
    }

    return $smoothedPoints.ToArray()
}

function Get-FioBandwidthStabilityAssessment {
    [CmdletBinding()]
    param(
        [AllowNull()][double]$CvPercent
    )

    if ($null -eq $CvPercent) {
        return [pscustomobject]@{
            Label = 'No data'
            Class = 'stability-neutral'
        }
    }

    if ($CvPercent -le 12) {
        return [pscustomobject]@{
            Label = 'Stable'
            Class = 'stability-good'
        }
    }

    if ($CvPercent -le 25) {
        return [pscustomobject]@{
            Label = 'Moderate'
            Class = 'stability-warn'
        }
    }

    return [pscustomobject]@{
        Label = 'Bursty'
        Class = 'stability-bad'
    }
}

function Get-FioDiagnosticsBandwidthStabilityModels {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$Diagnostics
    )

    if ($null -eq $Diagnostics -or -not $Diagnostics.Available -or $null -eq $Diagnostics.Series) {
        return @()
    }

    $bandwidthSeries = @($Diagnostics.Series | Where-Object { $_.Metric -eq 'bandwidth' } | Sort-Object Direction)
    $models = foreach ($series in $bandwidthSeries) {
        $cvPercent = if ($series.Summary -and $series.Summary.PSObject.Properties['CvPercent']) { $series.Summary.CvPercent } else { $null }
        $assessment = Get-FioBandwidthStabilityAssessment -CvPercent $cvPercent
        [pscustomobject]@{
            Direction = [string]$series.Direction
            DisplayLabel = '{0} BW' -f [string]$series.Direction
            DisplayValue = if ($null -ne $cvPercent) { '{0:N2}% CV' -f [double]$cvPercent } else { 'n/a' }
            AssessmentLabel = [string]$assessment.Label
            Class = [string]$assessment.Class
        }
    }

    @($models)
}

function Read-FioPerfmonDiagnosticsCsv {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    $rows = @(
        foreach ($row in @(Import-Csv -Path $Path -ErrorAction SilentlyContinue)) {
        if ($null -eq $row) {
            continue
        }

        $timeMs = $null
        $value = $null
        try {
            $timeMs = [int]$row.TimeMs
            $value = [double]$row.Value
        }
        catch {
            continue
        }

            [pscustomobject]@{
            TimeMs = $timeMs
            Metric = [string]$row.Metric
            Direction = [string]$row.Direction
            Unit = [string]$row.Unit
            Instance = [string]$row.Instance
            Value = $value
            }
        }
    )

    @($rows)
}

function Get-FioDiagnosticsThrottlingModel {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$Diagnostics
    )

    if ($null -eq $Diagnostics -or -not $Diagnostics.Available -or $null -eq $Diagnostics.Series) {
        return [pscustomobject]@{
            Available = $false
            Active = $false
            Label = 'No throttle evidence'
            Detail = 'No SMB perf counter diagnostics were available.'
        }
    }

    $queueSeries = @($Diagnostics.Series | Where-Object { $_.Metric -eq 'perfmonQueue' })
    $latencySeries = @($Diagnostics.Series | Where-Object { $_.Metric -eq 'perfmonLatency' })
    $stallSeries = @($Diagnostics.Series | Where-Object { $_.Metric -eq 'perfmonCreditStalls' })

    if (($queueSeries.Count + $latencySeries.Count + $stallSeries.Count) -eq 0) {
        return [pscustomobject]@{
            Available = $false
            Active = $false
            Label = 'No throttle evidence'
            Detail = 'No SMB perf counter diagnostics were available.'
        }
    }

    $queueMax = @($queueSeries | ForEach-Object { if ($_.Summary) { [double]$_.Summary.Max } }) | Measure-Object -Maximum
    $latencyMax = @($latencySeries | ForEach-Object { if ($_.Summary) { [double]$_.Summary.Max } }) | Measure-Object -Maximum
    $stallMax = @($stallSeries | ForEach-Object { if ($_.Summary) { [double]$_.Summary.Max } }) | Measure-Object -Maximum

    $queueValue = if ($null -ne $queueMax.Maximum) { [double]$queueMax.Maximum } else { 0.0 }
    $latencyValue = if ($null -ne $latencyMax.Maximum) { [double]$latencyMax.Maximum } else { 0.0 }
    $stallValue = if ($null -ne $stallMax.Maximum) { [double]$stallMax.Maximum } else { 0.0 }
    $active = (($queueValue -ge 1.0) -and ($latencyValue -ge 15.0)) -or ($stallValue -gt 0.0)

    $detail = if ($active) {
        'SMB client perf counters show queue growth, latency inflation, or credit stalls consistent with backend saturation or throttling.'
    }
    else {
        'SMB client perf counters did not show strong signs of sustained queueing or credit stalls.'
    }

    [pscustomobject]@{
        Available = $true
        Active = $active
        Label = if ($active) { 'Saturation signs' } else { 'No clear saturation' }
        Detail = $detail
        QueueDepthMax = [math]::Round($queueValue, 2)
        ClientLatencyMaxMs = [math]::Round($latencyValue, 2)
        CreditStallsMaxPerSec = [math]::Round($stallValue, 2)
    }
}

function Get-FioAggregateBandwidthMBps {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object]$Run
    )

    $readBandwidth = if ($Run.PSObject.Properties['Read'] -and $Run.Read.PSObject.Properties['BandwidthMBps']) { [double]$Run.Read.BandwidthMBps } else { 0.0 }
    $writeBandwidth = if ($Run.PSObject.Properties['Write'] -and $Run.Write.PSObject.Properties['BandwidthMBps']) { [double]$Run.Write.BandwidthMBps } else { 0.0 }
    [math]::Round(($readBandwidth + $writeBandwidth), 2)
}

function Get-FioDiagnosticsSummary {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$ResultDirectory,

        [int]$WarmupTrimSamples = $script:FioDiagnosticsWarmupTrimSamples,

        [int]$SmoothingWindowSamples = $script:FioDiagnosticsSmoothingWindowSamples
    )

    $raw = Get-FioDiagnosticsData -ResultDirectory $ResultDirectory
    if (-not $raw.Available) {
        return [pscustomobject]@{
            Requested = $false
            Available = $false
            Status = [string]$raw.Status
            Message = [string]$raw.Message
            LogWindowMs = $script:FioDiagnosticsWindowMs
            WarmupTrimSamples = $WarmupTrimSamples
            SmoothingWindowSamples = $SmoothingWindowSamples
            TimeRangeMs = $null
            MissingMetrics = @($raw.MissingMetrics)
            PresentMetrics = @($raw.PresentMetrics)
            Series = @()
            SourceFiles = @($raw.SourceFiles)
            Throttling = Get-FioDiagnosticsThrottlingModel -Diagnostics $null
        }
    }

    $series = @(
        foreach ($item in $raw.Series) {
            [pscustomobject]@{
                Key = $item.Key
                Label = $item.Label
                Metric = $item.Metric
                Direction = $item.Direction
                Unit = $item.Unit
                PreferredDecimals = $item.PreferredDecimals
                Points = @(
                    foreach ($point in $item.Points) {
                        [pscustomobject]@{
                            TimeMs = [int]$point.TimeMs
                            Value = [math]::Round([double]$point.Value, 4)
                        }
                    }
                )
                Summary = [pscustomobject]@{
                    Min = [math]::Round([double]$item.Summary.Min, 4)
                    Max = [math]::Round([double]$item.Summary.Max, 4)
                    Avg = [math]::Round([double]$item.Summary.Avg, 4)
                    SampleCount = [int]$item.Summary.SampleCount
                    CvPercent = if ($item.Summary.PSObject.Properties['CvPercent'] -and $null -ne $item.Summary.CvPercent) { [math]::Round([double]$item.Summary.CvPercent, 4) } else { $null }
                }
            }
        }
    )

    $diagnostics = [pscustomobject]@{
        Requested = $false
        Available = $true
        Status = [string]$raw.Status
        Message = [string]$raw.Message
        LogWindowMs = $raw.LogWindowMs
        WarmupTrimSamples = $WarmupTrimSamples
        SmoothingWindowSamples = $SmoothingWindowSamples
        TimeRangeMs = $raw.TimeRangeMs
        MissingMetrics = @($raw.MissingMetrics)
        PresentMetrics = @($raw.PresentMetrics)
        Series = $series
        SourceFiles = @($raw.SourceFiles)
    }

    $diagnostics | Add-Member -NotePropertyName Throttling -NotePropertyValue (Get-FioDiagnosticsThrottlingModel -Diagnostics $diagnostics)
    return $diagnostics
}

function Get-FioDiagnosticsExpectations {
    [CmdletBinding()]
    param()

    return @(
        [pscustomobject]@{ Key = 'bandwidth'; Label = 'Throughput' }
        [pscustomobject]@{ Key = 'bandwidthStability'; Label = 'BW Stability' }
        [pscustomobject]@{ Key = 'iops'; Label = 'IOPS' }
        [pscustomobject]@{ Key = 'latencyAvg'; Label = 'Avg Latency' }
        [pscustomobject]@{ Key = 'latencyMax'; Label = 'Max Latency' }
        [pscustomobject]@{ Key = 'perfmonLatency'; Label = 'SMB Client Latency' }
        [pscustomobject]@{ Key = 'perfmonQueue'; Label = 'SMB Queue' }
        [pscustomobject]@{ Key = 'perfmonCreditStalls'; Label = 'SMB Credit Stalls' }
    )
}

function Get-FioDiagnosticsData {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$ResultDirectory
    )

    $expectedMetrics = @(Get-FioDiagnosticsExpectations)

    if (-not (Test-Path -LiteralPath $ResultDirectory)) {
        return [pscustomobject]@{
            Available = $false
            Status = 'Unavailable'
            Message = 'The result directory was not found, so diagnostics could not be evaluated.'
            LogWindowMs = $script:FioDiagnosticsWindowMs
            TimeRangeMs = $null
            MissingMetrics = @($expectedMetrics.Label)
            PresentMetrics = @()
            Series = @()
            SourceFiles = @()
        }
    }

    $seriesMap = [ordered]@{}
    $sourceFiles = New-Object System.Collections.Generic.List[string]
    $patternMap = @(
        @{ Pattern = 'fio_bw*.log'; Metric = 'bandwidth'; Unit = 'MB/s'; Divisor = 1024.0; PreferredDecimals = 2; DirectionLabels = @{ '0' = 'Read'; '1' = 'Write'; '2' = 'Trim' } },
        @{ Pattern = 'fio_iops*.log'; Metric = 'iops'; Unit = 'IOPS'; Divisor = 1.0; PreferredDecimals = 0; DirectionLabels = @{ '0' = 'Read'; '1' = 'Write'; '2' = 'Trim' } },
        @{ Pattern = 'fio_clat*.log'; Metric = 'latencyAvg'; Unit = 'ms'; Divisor = 1000000.0; PreferredDecimals = 2; DirectionLabels = @{ '0' = 'Read'; '1' = 'Write'; '2' = 'Trim' }; ValueIndex = 1; LabelSuffix = 'Avg Latency' },
        @{ Pattern = 'fio_clat*.log'; Metric = 'latencyMax'; Unit = 'ms'; Divisor = 1000000.0; PreferredDecimals = 2; DirectionLabels = @{ '0' = 'Read'; '1' = 'Write'; '2' = 'Trim' }; ValueIndex = 2; LabelSuffix = 'Max Latency' }
    )

    foreach ($pattern in $patternMap) {
        $files = @(Get-ChildItem -LiteralPath $ResultDirectory -Filter $pattern.Pattern -File -ErrorAction SilentlyContinue)
        foreach ($file in $files) {
            $sourceFiles.Add($file.FullName)
            $rows = Read-FioDiagnosticsLogFile -Path $file.FullName -ValueIndex $(if ($pattern.ContainsKey('ValueIndex')) { [int]$pattern.ValueIndex } else { 1 }) -Divisor $pattern.Divisor
            foreach ($row in $rows) {
                $directionLabel = if ($pattern.DirectionLabels.ContainsKey([string]$row.Direction)) { [string]$pattern.DirectionLabels[[string]$row.Direction] } else { ('Direction {0}' -f [string]$row.Direction) }
                if ($directionLabel -eq 'Trim') {
                    continue
                }

                $seriesKey = '{0}-{1}' -f $pattern.Metric, $directionLabel.ToLowerInvariant()
                if (-not $seriesMap.Contains($seriesKey)) {
                    $label = if ($pattern.ContainsKey('LabelSuffix')) {
                        '{0} {1}' -f $directionLabel, $pattern.LabelSuffix
                    }
                    elseif ($pattern.Metric -eq 'bandwidth') {
                        '{0} Throughput' -f $directionLabel
                    }
                    elseif ($pattern.Metric -eq 'iops') {
                        '{0} IOPS' -f $directionLabel
                    }
                    else {
                        '{0} {1}' -f $directionLabel, $pattern.Metric
                    }

                    $seriesMap[$seriesKey] = [ordered]@{
                        Key = $seriesKey
                        Label = $label
                        Metric = $pattern.Metric
                        Direction = $directionLabel
                        Unit = $pattern.Unit
                        PreferredDecimals = $pattern.PreferredDecimals
                        PointsByTime = @{}
                    }
                }

                $pointBucket = $seriesMap[$seriesKey].PointsByTime
                $bucketKey = [string]$row.TimeMs
                if (-not $pointBucket.ContainsKey($bucketKey)) {
                    $pointBucket[$bucketKey] = 0.0
                }
                $pointBucket[$bucketKey] = [double]$pointBucket[$bucketKey] + [double]$row.Value
            }
        }
    }

    foreach ($file in @(Get-ChildItem -LiteralPath $ResultDirectory -Filter '*-perfmon-smb.csv' -File -ErrorAction SilentlyContinue)) {
        $sourceFiles.Add($file.FullName)
        foreach ($row in @(Read-FioPerfmonDiagnosticsCsv -Path $file.FullName)) {
            $seriesKey = '{0}-{1}' -f [string]$row.Metric, ([string]$row.Direction).ToLowerInvariant()
            if (-not $seriesMap.Contains($seriesKey)) {
                $label = switch ([string]$row.Metric) {
                    'perfmonBandwidth' { '{0} SMB Throughput' -f [string]$row.Direction }
                    'perfmonLatency' { '{0} SMB Client Latency' -f [string]$row.Direction }
                    'perfmonQueue' { 'SMB Client Queue' }
                    'perfmonCreditStalls' { 'SMB Credit Stalls' }
                    default { '{0} {1}' -f [string]$row.Direction, [string]$row.Metric }
                }

                $seriesMap[$seriesKey] = [ordered]@{
                    Key = $seriesKey
                    Label = $label
                    Metric = [string]$row.Metric
                    Direction = [string]$row.Direction
                    Unit = [string]$row.Unit
                    PreferredDecimals = if (@('perfmonQueue', 'perfmonCreditStalls') -contains [string]$row.Metric) { 2 } else { 2 }
                    PointsByTime = @{}
                }
            }

            $pointBucket = $seriesMap[$seriesKey].PointsByTime
            $bucketKey = [string]$row.TimeMs
            if (-not $pointBucket.ContainsKey($bucketKey)) {
                $pointBucket[$bucketKey] = 0.0
            }

            if (@('perfmonQueue', 'perfmonCreditStalls') -contains [string]$row.Metric) {
                $pointBucket[$bucketKey] = [math]::Max([double]$pointBucket[$bucketKey], [double]$row.Value)
            }
            else {
                $pointBucket[$bucketKey] = [double]$pointBucket[$bucketKey] + [double]$row.Value
            }
        }
    }

    if ($seriesMap.Count -eq 0) {
        $status = if ($sourceFiles.Count -gt 0) { 'Unreadable' } else { 'MissingLogs' }
        $message = if ($sourceFiles.Count -gt 0) {
            'fio diagnostic log files were found, but no chartable samples could be parsed from them.'
        }
        else {
            'No fio diagnostic log files were found in the result directory.'
        }

        return [pscustomobject]@{
            Available = $false
            Status = $status
            Message = $message
            LogWindowMs = $script:FioDiagnosticsWindowMs
            TimeRangeMs = $null
            MissingMetrics = @($expectedMetrics.Label)
            PresentMetrics = @()
            Series = @()
            SourceFiles = @($sourceFiles | Select-Object -Unique)
        }
    }

    $series = New-Object System.Collections.Generic.List[object]
    $allTimes = New-Object System.Collections.Generic.List[int]
    foreach ($entry in $seriesMap.GetEnumerator()) {
        $rawPoints = @(
            foreach ($timeKey in ($entry.Value.PointsByTime.Keys | ForEach-Object { [int]$_ } | Sort-Object)) {
                $allTimes.Add($timeKey)
                [pscustomobject]@{
                    TimeMs = $timeKey
                    Value = [double]$entry.Value.PointsByTime[[string]$timeKey]
                }
            }
        )

        $displayTransform = Get-FioDiagnosticsDisplayTransform -Metric ([string]$entry.Value.Metric)
        $points = @(Get-FioDiagnosticsSmoothedPoints -Points $rawPoints -TrimLeadingSamples $displayTransform.TrimLeadingSamples -SmoothingWindowSize $displayTransform.SmoothingWindowSize)
        $values = @($points | ForEach-Object { $_.Value })
        $series.Add([pscustomobject]@{
            Key = $entry.Value.Key
            Label = $entry.Value.Label
            Metric = $entry.Value.Metric
            Direction = $entry.Value.Direction
            Unit = $entry.Value.Unit
            PreferredDecimals = $entry.Value.PreferredDecimals
            Points = $points
            Summary = [pscustomobject]@{
                Min = if ($values.Count -gt 0) { ($values | Measure-Object -Minimum).Minimum } else { 0 }
                Max = if ($values.Count -gt 0) { ($values | Measure-Object -Maximum).Maximum } else { 0 }
                Avg = if ($values.Count -gt 0) { ($values | Measure-Object -Average).Average } else { 0 }
                SampleCount = $values.Count
                CvPercent = Get-FioCoefficientOfVariationFromValues -Values @($values | ForEach-Object { [double]$_ })
                RawSampleCount = $rawPoints.Count
                TrimmedSamples = [math]::Max(0, ($rawPoints.Count - $points.Count))
                SmoothingWindowSamples = $displayTransform.SmoothingWindowSize
            }
        })
    }

    foreach ($bandwidthSeries in @($series | Where-Object { $_.Metric -eq 'bandwidth' })) {
        $stabilitySeries = New-FioDiagnosticsBandwidthStabilitySeries -Series $bandwidthSeries
        if ($null -ne $stabilitySeries) {
            $series.Add($stabilitySeries)
        }
    }

    $orderedSeries = @($series | Sort-Object Metric, Direction)
    $orderedTimes = @($allTimes | Sort-Object -Unique)
    $presentMetricKeys = @($orderedSeries | ForEach-Object { [string]$_.Metric } | Select-Object -Unique)
    $presentMetrics = @(
        foreach ($metric in $expectedMetrics) {
            if ($presentMetricKeys -contains $metric.Key) {
                $metric.Label
            }
        }
    )
    $missingMetrics = @(
        foreach ($metric in $expectedMetrics) {
            if ($presentMetricKeys -notcontains $metric.Key) {
                $metric.Label
            }
        }
    )
    $status = if ($missingMetrics.Count -gt 0) { 'Partial' } else { 'Available' }
    $message = if ($missingMetrics.Count -gt 0) {
        'Some diagnostics were captured, but not every telemetry stream was available. Displayed charts use steady-state, smoothed samples.'
    }
    else {
        'Windowed throughput, BW stability, IOPS, and latency diagnostics were captured successfully. Displayed charts use steady-state, smoothed samples.'
    }

    [pscustomobject]@{
        Available = $true
        Status = $status
        Message = $message
        LogWindowMs = $script:FioDiagnosticsWindowMs
        TimeRangeMs = if ($orderedTimes.Count -gt 0) { ($orderedTimes | Measure-Object -Maximum).Maximum } else { $null }
        MissingMetrics = $missingMetrics
        PresentMetrics = $presentMetrics
        Series = $orderedSeries
        SourceFiles = @($sourceFiles | Select-Object -Unique | Sort-Object)
    }
}

function Read-FioDiagnosticsLogFile {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Path,

        [Parameter(Mandatory)]
        [int]$ValueIndex,

        [Parameter(Mandatory)]
        [double]$Divisor
    )

    $rows = New-Object System.Collections.Generic.List[object]
    foreach ($line in Get-Content -LiteralPath $Path -ErrorAction Stop) {
        if ([string]::IsNullOrWhiteSpace($line)) {
            continue
        }

        $trimmed = $line.Trim()
        if ($trimmed.StartsWith('#') -or $trimmed.StartsWith(';')) {
            continue
        }

        $columns = $trimmed.Split(',')
        if ($columns.Count -le $ValueIndex) {
            continue
        }

        try {
            $timeMs = [int]::Parse($columns[0], [System.Globalization.CultureInfo]::InvariantCulture)
            $value = [double]::Parse($columns[$ValueIndex], [System.Globalization.CultureInfo]::InvariantCulture)
        }
        catch {
            continue
        }

        $directionIndex = if ($columns.Count -ge 6) { 3 } elseif ($columns.Count -ge 3) { 2 } else { -1 }
        try {
            if ($directionIndex -lt 0 -or $columns.Count -le $directionIndex) {
                throw 'Missing direction column.'
            }

            $direction = [int]::Parse($columns[$directionIndex], [System.Globalization.CultureInfo]::InvariantCulture)
        }
        catch {
            $direction = 0
        }

        $rows.Add([pscustomobject]@{
            TimeMs = $timeMs
            Value = ($value / $Divisor)
            Direction = $direction
        })
    }

    return $rows.ToArray()
}

function Export-FioSqlBenchDiagnosticsCsv {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object]$Diagnostics,

        [Parameter(Mandatory)]
        [string]$Path
    )

    $rows = New-Object System.Collections.Generic.List[object]
    if ($null -ne $Diagnostics -and $Diagnostics.Available) {
        foreach ($series in @($Diagnostics.Series)) {
            foreach ($point in @($series.Points)) {
                $rows.Add([pscustomobject]@{
                    Metric = $series.Metric
                    SeriesKey = $series.Key
                    Label = $series.Label
                    Direction = $series.Direction
                    Unit = $series.Unit
                    TimeMs = $point.TimeMs
                    TimeSec = [math]::Round(($point.TimeMs / 1000.0), 3)
                    Value = $point.Value
                })
            }
        }
    }

    $rows | Export-Csv -Path $Path -NoTypeInformation -Encoding utf8
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

    $diagnosticIteration = @($Iterations | Where-Object { $_.PSObject.Properties['Diagnostics'] -and $null -ne $_.Diagnostics }) | Select-Object -Last 1
    $rawDiagnostics = if ($null -ne $diagnosticIteration) { $diagnosticIteration.Diagnostics } else { $null }
    $diagnosticsRequested = if ($null -ne $rawDiagnostics -and $rawDiagnostics.PSObject.Properties['Requested']) {
        [bool]$rawDiagnostics.Requested
    }
    elseif ($RootSummary.PSObject.Properties['DiagnosticsEnabled']) {
        [bool]$RootSummary.DiagnosticsEnabled
    }
    else {
        $false
    }

    $normalizedDiagnostics = if ($null -ne $rawDiagnostics) {
        [pscustomobject]@{
            Requested = $diagnosticsRequested
            Available = [bool]$rawDiagnostics.Available
            Status = if ($rawDiagnostics.PSObject.Properties['Status']) { [string]$rawDiagnostics.Status } elseif ($rawDiagnostics.Available) { 'Available' } else { 'Unavailable' }
            Message = if ($rawDiagnostics.PSObject.Properties['Message']) { [string]$rawDiagnostics.Message } elseif ($rawDiagnostics.Available) { 'Windowed diagnostics were captured successfully.' } elseif ($diagnosticsRequested) { 'Diagnostics were requested but no chartable telemetry was retained in this summary.' } else { 'Diagnostics were not requested for this run.' }
            LogWindowMs = if ($rawDiagnostics.PSObject.Properties['LogWindowMs']) { $rawDiagnostics.LogWindowMs } else { $script:FioDiagnosticsWindowMs }
            TimeRangeMs = if ($rawDiagnostics.PSObject.Properties['TimeRangeMs']) { $rawDiagnostics.TimeRangeMs } else { $null }
            MissingMetrics = if ($rawDiagnostics.PSObject.Properties['MissingMetrics']) { @($rawDiagnostics.MissingMetrics) } else { @() }
            PresentMetrics = if ($rawDiagnostics.PSObject.Properties['PresentMetrics']) { @($rawDiagnostics.PresentMetrics) } else { @() }
            Series = if ($rawDiagnostics.PSObject.Properties['Series']) { @($rawDiagnostics.Series) } else { @() }
            SourceFiles = if ($rawDiagnostics.PSObject.Properties['SourceFiles']) { @($rawDiagnostics.SourceFiles) } else { @() }
            Throttling = if ($rawDiagnostics.PSObject.Properties['Throttling']) { $rawDiagnostics.Throttling } else { Get-FioDiagnosticsThrottlingModel -Diagnostics $rawDiagnostics }
        }
    }
    else {
        [pscustomobject]@{
            Requested = $diagnosticsRequested
            Available = $false
            Status = if ($diagnosticsRequested) { 'MissingSummary' } else { 'Disabled' }
            Message = if ($diagnosticsRequested) { 'Diagnostics were requested for this run, but the historical summary does not retain a diagnostics payload.' } else { 'Diagnostics were not requested for this run.' }
            LogWindowMs = $script:FioDiagnosticsWindowMs
            TimeRangeMs = $null
            MissingMetrics = @()
            PresentMetrics = @()
            Series = @()
            SourceFiles = @()
            Throttling = Get-FioDiagnosticsThrottlingModel -Diagnostics $null
        }
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
        CpuAffinity = if ($RootSummary.PSObject.Properties['CpuAffinity']) { $RootSummary.CpuAffinity } elseif ($first.PSObject.Properties['CpuAffinity']) { $first.CpuAffinity } else { $null }
        AggregateBandwidthMBps = [math]::Round(([double]$read.BandwidthMBps + [double]$write.BandwidthMBps), 2)
        AggregateIops = [math]::Round(([double]$read.Iops + [double]$write.Iops), 2)
        Read = $read
        Write = $write
        Diagnostics = $normalizedDiagnostics
        Iterations = $Iterations
    }
}

function Import-FioSqlBenchHistory {
    [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSAvoidAssignmentToAutomaticVariable', 'Profile', Justification = 'The public function intentionally exposes -Profile.')]
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$ResultsRoot,

        [ValidateSet('Any', 'Data', 'Log', 'Tempdb', 'BackupRestore', 'DbccScan', 'MaxThroughput', 'MaxIOPs')]
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
        $rootSummary = ConvertFrom-FioJsonDocument -InputObject (Get-Content -LiteralPath $file.FullName -Raw)
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
                DiagnosticsRequestedRuns = @($orderedRuns | Where-Object { $_.Diagnostics -and $_.Diagnostics.Requested }).Count
                DiagnosticsAvailableRuns = @($orderedRuns | Where-Object { $_.Diagnostics -and $_.Diagnostics.Available }).Count
                DiagnosticsPartialRuns = @($orderedRuns | Where-Object { $_.Diagnostics -and $_.Diagnostics.Status -eq 'Partial' }).Count
                DiagnosticsMissingRuns = @($orderedRuns | Where-Object { $_.Diagnostics -and $_.Diagnostics.Requested -and -not $_.Diagnostics.Available }).Count
                LastDiagnosticsStatus = if ($orderedRuns[-1].Diagnostics) { [string]$orderedRuns[-1].Diagnostics.Status } else { 'Disabled' }
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
            DiagnosticsRequested = if ($run.Diagnostics) { $run.Diagnostics.Requested } else { $false }
            DiagnosticsStatus = if ($run.Diagnostics) { $run.Diagnostics.Status } else { $null }
            DiagnosticsMessage = if ($run.Diagnostics) { $run.Diagnostics.Message } else { $null }
            DiagnosticsSeriesCount = if ($run.Diagnostics -and $run.Diagnostics.Series) { @($run.Diagnostics.Series).Count } else { 0 }
            DiagnosticsSourceFileCount = if ($run.Diagnostics -and $run.Diagnostics.SourceFiles) { @($run.Diagnostics.SourceFiles).Count } else { 0 }
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

function Get-FioDiagnosticsRenderState {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$Diagnostics
    )

    $requested = $false
    if ($null -ne $Diagnostics -and $Diagnostics.PSObject.Properties['Requested']) {
        $requested = [bool]$Diagnostics.Requested
    }

    $available = $false
    if ($null -ne $Diagnostics -and $Diagnostics.PSObject.Properties['Available']) {
        $available = [bool]$Diagnostics.Available
    }

    $status = if ($null -ne $Diagnostics -and $Diagnostics.PSObject.Properties['Status'] -and -not [string]::IsNullOrWhiteSpace([string]$Diagnostics.Status)) {
        [string]$Diagnostics.Status
    }
    elseif ($available) {
        'Available'
    }
    elseif ($requested) {
        'Unavailable'
    }
    else {
        'Disabled'
    }

    $missingCount = if ($null -ne $Diagnostics -and $Diagnostics.PSObject.Properties['MissingMetrics']) { @($Diagnostics.MissingMetrics).Count } else { 0 }
    $presentCount = if ($null -ne $Diagnostics -and $Diagnostics.PSObject.Properties['PresentMetrics']) { @($Diagnostics.PresentMetrics).Count } else { 0 }
    $detail = if ($requested) {
        '{0}/{1} streams' -f $presentCount, ($presentCount + $missingCount)
    }
    else {
        'Not requested'
    }

    $label = switch ($status) {
        'Available' { 'Available' }
        'Partial' { 'Partial' }
        'MissingLogs' { 'Missing logs' }
        'Unreadable' { 'Unreadable' }
        'MissingSummary' { 'Missing summary' }
        'Unavailable' { 'Unavailable' }
        default { 'Disabled' }
    }

    $class = switch ($status) {
        'Available' { 'diagnostic-state-good' }
        'Partial' { 'diagnostic-state-warn' }
        'MissingLogs' { 'diagnostic-state-bad' }
        'Unreadable' { 'diagnostic-state-bad' }
        'MissingSummary' { 'diagnostic-state-bad' }
        'Unavailable' { 'diagnostic-state-bad' }
        default { 'diagnostic-state-neutral' }
    }

    return [pscustomobject]@{
        Requested = $requested
        Available = $available
        Status = $status
        Label = $label
        Detail = $detail
        Class = $class
    }
}

function Convert-FioHtmlUsToMs {
    [CmdletBinding()]
    param(
        [AllowNull()][double]$Microseconds
    )

    if ($null -eq $Microseconds) {
        return $null
    }

    [math]::Round(($Microseconds / 1000.0), 2)
}

function Get-FioHtmlAssessmentDisplay {
    [CmdletBinding()]
    param(
        [string]$Status
    )

    switch ($Status) {
        'Excellent' { return [pscustomobject]@{ Label = 'Excellent'; Class = 'assessment-excellent'; Rank = 0 } }
        'Very good' { return [pscustomobject]@{ Label = 'Good'; Class = 'assessment-good'; Rank = 1 } }
        'Good' { return [pscustomobject]@{ Label = 'Good'; Class = 'assessment-good'; Rank = 2 } }
        'Watch' { return [pscustomobject]@{ Label = 'Poor'; Class = 'assessment-poor'; Rank = 3 } }
        'Poor' { return [pscustomobject]@{ Label = 'Poor'; Class = 'assessment-poor'; Rank = 4 } }
        'Bad' { return [pscustomobject]@{ Label = 'Poor'; Class = 'assessment-poor'; Rank = 5 } }
        'Deplorable' { return [pscustomobject]@{ Label = 'Poor'; Class = 'assessment-poor'; Rank = 6 } }
        default { return [pscustomobject]@{ Label = 'No data'; Class = 'assessment-neutral'; Rank = 7 } }
    }
}

function Get-FioHtmlGenericLatencyAssessment {
    [CmdletBinding()]
    param(
        [AllowNull()][double]$LatencyMs
    )

    if ($null -eq $LatencyMs) {
        return 'No data'
    }

    if ($LatencyMs -lt 2) { return 'Excellent' }
    if ($LatencyMs -le 5) { return 'Very good' }
    if ($LatencyMs -le 15) { return 'Good' }
    if ($LatencyMs -le 100) { return 'Poor' }
    if ($LatencyMs -le 500) { return 'Bad' }
    return 'Deplorable'
}

function Get-FioHtmlSqlProfileAssessment {
    [CmdletBinding()]
    param(
        [string]$WorkloadProfile,
        [string]$TargetType,
        [string]$Operation,
        [AllowNull()][double]$MeanLatencyMs,
        [AllowNull()][double]$P99LatencyMs,
        [AllowNull()][double]$P999LatencyMs,
        [AllowNull()][double]$WorstP99LatencyMs,
        [int]$Direct
    )

    $notes = New-Object System.Collections.Generic.List[string]
    $status = Get-FioHtmlGenericLatencyAssessment -LatencyMs $MeanLatencyMs

    switch ($WorkloadProfile) {
        'MaxIOPs' {
            if ($null -ne $MeanLatencyMs) {
                if ($TargetType -eq 'Smb') {
                    if ($MeanLatencyMs -le 4) { $status = 'Excellent' }
                    elseif ($MeanLatencyMs -le 8) { $status = 'Very good' }
                    elseif ($MeanLatencyMs -le 15) { $status = 'Good' }
                    elseif ($MeanLatencyMs -le 30) { $status = 'Watch' }
                    else { $status = 'Poor' }
                }
                elseif ($MeanLatencyMs -le 2) { $status = 'Excellent' }
                elseif ($MeanLatencyMs -le 5) { $status = 'Very good' }
                elseif ($MeanLatencyMs -le 10) { $status = 'Good' }
                elseif ($MeanLatencyMs -le 20) { $status = 'Watch' }
                else { $status = 'Poor' }

                $notes.Add('MaxIOPs intentionally runs isolated 4K random read and random write phases with aggressive concurrency. Prioritize peak sustained IOPS and stability over OLTP-like mixed-latency targets.')
            }
        }
        'MaxThroughput' {
            if ($null -ne $MeanLatencyMs) {
                if ($TargetType -eq 'Smb') {
                    if ($MeanLatencyMs -le 15) { $status = 'Excellent' }
                    elseif ($MeanLatencyMs -le 30) { $status = 'Very good' }
                    elseif ($MeanLatencyMs -le 50) { $status = 'Good' }
                    elseif ($MeanLatencyMs -le 100) { $status = 'Watch' }
                    else { $status = 'Poor' }
                }
                elseif ($MeanLatencyMs -le 10) { $status = 'Excellent' }
                elseif ($MeanLatencyMs -le 25) { $status = 'Very good' }
                elseif ($MeanLatencyMs -le 40) { $status = 'Good' }
                elseif ($MeanLatencyMs -le 75) { $status = 'Watch' }
                else { $status = 'Poor' }

                $notes.Add('MaxThroughput intentionally runs separate large-block sequential read and write phases with enough concurrency to saturate the path. Throughput and stability matter more here than OLTP-style low latency.')
            }
        }
        'Log' {
            if ($Operation -eq 'Write' -and $null -ne $MeanLatencyMs) {
                if ($TargetType -eq 'Smb') {
                    if ($MeanLatencyMs -le 2) { $status = 'Excellent' }
                    elseif ($MeanLatencyMs -le 6) { $status = 'Very good' }
                    elseif ($MeanLatencyMs -le 10) { $status = 'Good' }
                    elseif ($MeanLatencyMs -le 15) { $status = 'Watch' }
                    else { $status = 'Poor' }
                    $notes.Add('SQL over SMB is supported, but all file I/O traverses the network path. Microsoft recommends ensuring adequate bandwidth and SMB 3 continuous availability.')
                }
                elseif ($MeanLatencyMs -le 1) { $status = 'Excellent' }
                elseif ($MeanLatencyMs -le 5) { $status = 'Very good' }
                elseif ($MeanLatencyMs -le 10) { $status = 'Watch' }
                elseif ($MeanLatencyMs -le 15) { $status = 'Poor' }
                else { $status = 'Bad' }
                $notes.Add('SQL log guidance: well-tuned log writes are typically 1-5 ms, ideally near 1 ms.')

                if ($TargetType -eq 'Local' -and $Direct -eq 0) {
                    $notes.Add('Local log benchmarking is most SQL-like with direct I/O enabled so buffered filesystem cache does not hide commit latency.')
                }
            }
        }
        default {
            if ($null -ne $MeanLatencyMs) {
                if ($TargetType -eq 'Smb') {
                    if ($MeanLatencyMs -le 6) { $status = 'Excellent' }
                    elseif ($MeanLatencyMs -le 12) { $status = 'Very good' }
                    elseif ($MeanLatencyMs -le 15) { $status = 'Good' }
                    elseif ($MeanLatencyMs -le 20) { $status = 'Watch' }
                    else { $status = 'Poor' }
                    $notes.Add('SMB-backed SQL results include network and file-server effects. Microsoft recommends checking bandwidth, SMB Multichannel, and SMB Direct where available.')
                }
                elseif ($MeanLatencyMs -le 5) { $status = 'Excellent' }
                elseif ($MeanLatencyMs -le 10) { $status = 'Very good' }
                elseif ($MeanLatencyMs -le 15) { $status = 'Good' }
                elseif ($MeanLatencyMs -le 20) { $status = 'Watch' }
                else { $status = 'Poor' }
                $notes.Add('SQL data guidance: well-tuned data reads and writes are usually under 10 ms, with 4-20 ms as a common tuned range.')
            }
        }
    }

    if ($null -ne $P99LatencyMs) {
        if ($P99LatencyMs -gt 15) {
            $current = Get-FioHtmlAssessmentDisplay -Status $status
            $tail = Get-FioHtmlAssessmentDisplay -Status 'Watch'
            if ($tail.Rank -gt $current.Rank) {
                $status = 'Watch'
            }
            $notes.Add('Tail latency exceeds the 10-15 ms SQL bottleneck investigation threshold.')
        }
        elseif ($P99LatencyMs -gt 10) {
            $notes.Add('Tail latency is above 10 ms; watch for sustained pressure under production load.')
        }
    }

    if ($null -ne $P999LatencyMs) {
        if ($WorkloadProfile -eq 'Log' -and $Operation -eq 'Write' -and $P999LatencyMs -gt 25) {
            $status = if ($TargetType -eq 'Smb') { 'Poor' } else { 'Bad' }
            $notes.Add('P99.9 latency shows severe commit stalls. This is a strong SQL log tail-latency warning sign.')
        }
        elseif ($P999LatencyMs -gt 30) {
            $current = Get-FioHtmlAssessmentDisplay -Status $status
            $tail = Get-FioHtmlAssessmentDisplay -Status 'Watch'
            if ($tail.Rank -gt $current.Rank) {
                $status = 'Watch'
            }
            $notes.Add('P99.9 latency indicates deeper tail stalls beyond the P99 view. Treat this as a stability risk for SQL workloads.')
        }
    }

    if ($null -ne $WorstP99LatencyMs -and $null -ne $P99LatencyMs -and $WorstP99LatencyMs -gt ($P99LatencyMs * 1.5)) {
        $notes.Add('One fio worker is materially worse than the composite tail. This suggests queue imbalance or uneven latency across the path.')
    }

    [pscustomobject]@{
        Status = $status
        Notes = @($notes | Select-Object -Unique)
    }
}

function Get-FioHtmlOperationAssessmentModel {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object]$Run,

        [Parameter(Mandatory)]
        [ValidateSet('Read', 'Write')]
        [string]$Operation
    )

    $stats = if ($Operation -eq 'Read') { $Run.Read } else { $Run.Write }
    $directMode = if ($Run.PSObject.Properties['Direct']) { [int]$Run.Direct } else { 0 }
    $meanLatencyUs = if ($stats.PSObject.Properties['MeanLatencyUs']) { $stats.MeanLatencyUs } else { $null }
    $p99LatencyUs = if ($stats.PSObject.Properties['P99LatencyUs']) { $stats.P99LatencyUs } else { $null }
    $p999LatencyUs = if ($stats.PSObject.Properties['P999LatencyUs']) { $stats.P999LatencyUs } else { $null }
    $worstP99LatencyUs = if ($stats.PSObject.Properties['WorstP99LatencyUs']) { $stats.WorstP99LatencyUs } else { $null }
    $totalIos = if ($stats.PSObject.Properties['TotalIos']) { $stats.TotalIos } else { $null }
    $bandwidthCvPercent = if ($stats.PSObject.Properties['BandwidthCvPercent']) { $stats.BandwidthCvPercent } else { $null }
    $iopsCvPercent = if ($stats.PSObject.Properties['IopsCvPercent']) { $stats.IopsCvPercent } else { $null }
    $meanMs = Convert-FioHtmlUsToMs -Microseconds $meanLatencyUs
    $p99Ms = Convert-FioHtmlUsToMs -Microseconds $p99LatencyUs
    $p999Ms = Convert-FioHtmlUsToMs -Microseconds $p999LatencyUs
    $worstP99Ms = Convert-FioHtmlUsToMs -Microseconds $worstP99LatencyUs
    $assessment = Get-FioHtmlSqlProfileAssessment -WorkloadProfile ([string]$Run.Profile) -TargetType ([string]$Run.TargetType) -Operation $Operation -MeanLatencyMs $meanMs -P99LatencyMs $p99Ms -P999LatencyMs $p999Ms -WorstP99LatencyMs $worstP99Ms -Direct $directMode
    $display = Get-FioHtmlAssessmentDisplay -Status $assessment.Status

    [pscustomobject]@{
        Operation = $Operation
        Iops = [double]$stats.Iops
        BandwidthMBps = [double]$stats.BandwidthMBps
        MeanMs = $meanMs
        P99Ms = $p99Ms
        P999Ms = $p999Ms
        WorstP99Ms = $worstP99Ms
        TotalIos = $totalIos
        BandwidthCvPercent = $bandwidthCvPercent
        IopsCvPercent = $iopsCvPercent
        RawStatus = $assessment.Status
        DisplayStatus = $display.Label
        AssessmentClass = $display.Class
        AssessmentRank = $display.Rank
        Notes = @($assessment.Notes)
    }
}

function Get-FioHtmlActiveAssessmentRows {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object]$Run
    )

    $rows = @(
        Get-FioHtmlOperationAssessmentModel -Run $Run -Operation 'Read'
        Get-FioHtmlOperationAssessmentModel -Run $Run -Operation 'Write'
    )

    $activeRows = @(
        $rows | Where-Object {
            ($null -ne $_.TotalIos -and $_.TotalIos -gt 0) -or
            ($null -ne $_.Iops -and $_.Iops -gt 0) -or
            ($null -ne $_.BandwidthMBps -and $_.BandwidthMBps -gt 0)
        }
    )

    if ($activeRows.Count -gt 0) {
        return $activeRows
    }

    return $rows
}

function Get-FioHtmlOverallAssessmentModel {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object]$Run
    )

    $rows = @(Get-FioHtmlActiveAssessmentRows -Run $Run)
    $worstRow = @($rows | Sort-Object @{ Expression = { $_.AssessmentRank }; Descending = $true }, @{ Expression = { $_.MeanMs }; Descending = $true } | Select-Object -First 1)[0]
    if ($null -eq $worstRow) {
        return [pscustomobject]@{ Label = 'No data'; Class = 'assessment-neutral'; Rank = 7; Operation = $null }
    }

    $throttlingModel = if ($Run.PSObject.Properties['Diagnostics']) { Get-FioDiagnosticsThrottlingModel -Diagnostics $Run.Diagnostics } else { $null }
    if ($null -ne $throttlingModel -and $throttlingModel.Available -and $throttlingModel.Active -and @('MaxThroughput', 'BackupRestore') -contains [string]$Run.Profile -and [string]$Run.TargetType -eq 'Smb' -and $worstRow.AssessmentRank -ge 3) {
        return [pscustomobject]@{ Label = 'Saturated'; Class = 'assessment-neutral'; Rank = 2; Operation = 'Aggregate' }
    }

    [pscustomobject]@{
        Label = $worstRow.DisplayStatus
        Class = $worstRow.AssessmentClass
        Rank = $worstRow.AssessmentRank
        Operation = $worstRow.Operation
    }
}

function Get-FioHtmlProfileTargetText {
    [CmdletBinding()]
    param(
        [string]$WorkloadProfile,
        [string]$TargetType
    )

    switch ($WorkloadProfile) {
        'MaxIOPs' {
            if ($TargetType -eq 'Smb') {
                return 'MaxIOPs is a best-case random-I/O profile. It runs isolated 4K randread and randwrite phases over the same files so read and write IOPS can be compared without a mixed-workload split.'
            }

            return 'MaxIOPs is a best-case random-I/O profile. It runs isolated 4K randread and randwrite phases over the same files so peak read and write IOPS are measured separately under heavy queue depth.'
        }
        'MaxThroughput' {
            if ($TargetType -eq 'Smb') {
                return 'MaxThroughput is a best-case bandwidth profile. It runs separate large-block sequential read and write phases so each direction can be compared against the SMB path limit.'
            }

            return 'MaxThroughput is a best-case bandwidth profile. It runs separate large-block sequential read and write phases so each direction can be measured near the path limit.'
        }
        'BackupRestore' {
            if ($TargetType -eq 'Smb') {
                return 'Backup and restore over SMB should show stable large-block throughput with latency spikes kept out of sustained operation.'
            }

            return 'Backup and restore are throughput-led workloads. Look for strong large-block MB/s without sustained double-digit latency.'
        }
        'DbccScan' {
            if ($TargetType -eq 'Smb') {
                return 'DBCC-like scans over SMB should maintain predictable sequential read throughput while avoiding sustained tail-latency spikes.'
            }

            return 'DBCC-like scan workloads should keep large-block read latency controlled while favoring consistent throughput.'
        }
        'Log' {
            if ($TargetType -eq 'Smb') {
                return 'SMB log writes should still trend toward low single-digit ms; 10-15 ms remains the escalation line.'
            }

            return 'Log writes are best around 1-5 ms.'
        }
        default {
            if ($TargetType -eq 'Smb') {
                return 'SMB data/tempdb I/O should still stay below 10-15 ms where possible; slightly higher overhead can be normal without SMB Direct.'
            }

            return 'Data/tempdb I/O is healthiest under 10 ms; 4-20 ms is a common tuned range.'
        }
    }
}

function Get-FioHtmlSqlFitBlurb {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object]$Run
    )

    $targetText = Get-FioHtmlProfileTargetText -WorkloadProfile ([string]$Run.Profile) -TargetType ([string]$Run.TargetType)
    $rows = @(Get-FioHtmlActiveAssessmentRows -Run $Run)
    $focusRow = @($rows | Sort-Object @{ Expression = { $_.AssessmentRank }; Descending = $true }, @{ Expression = { $_.MeanMs }; Descending = $true } | Select-Object -First 1)[0]
    $focusNote = if ($null -ne $focusRow -and @($focusRow.Notes).Count -gt 0) { [string](@($focusRow.Notes | Select-Object -Unique)[0]) } else { $null }
    $throttlingModel = if ($Run.PSObject.Properties['Diagnostics']) { Get-FioDiagnosticsThrottlingModel -Diagnostics $Run.Diagnostics } else { $null }
    if ($null -ne $throttlingModel -and $throttlingModel.Available -and $throttlingModel.Active -and @('MaxThroughput', 'BackupRestore') -contains [string]$Run.Profile -and [string]$Run.TargetType -eq 'Smb') {
        $aggregateBandwidth = Get-FioAggregateBandwidthMBps -Run $Run
        $focusNote = ('Observed aggregate throughput is {0:N2} MB/s while SMB client counters show queueing or credit stalls. Treat this as saturation evidence before reading the run as an outright storage miss.' -f $aggregateBandwidth)
    }
    elseif ($null -ne $throttlingModel -and $throttlingModel.Available -and -not $throttlingModel.Active -and [string]$Run.TargetType -eq 'Smb' -and @('MaxThroughput', 'MaxIOPs', 'BackupRestore', 'Tempdb', 'Data') -contains [string]$Run.Profile) {
        $stabilityModels = @(Get-FioDiagnosticsBandwidthStabilityModels -Diagnostics $Run.Diagnostics)
        $burstySeries = @($stabilityModels | Where-Object { $_.AssessmentLabel -eq 'Bursty' })
        if ($burstySeries.Count -gt 0 -and $throttlingModel.QueueDepthMax -lt 4.0 -and $throttlingModel.ClientLatencyMaxMs -lt 20.0 -and $throttlingModel.CreditStallsMaxPerSec -le 0.0) {
            $focusNote = 'Windowed fio delivery is bursty, but SMB client queue depth, client latency, and credit stalls stayed flat. Treat this as paced or burst-delivery behavior on the service path rather than clear client-side distress.'
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($targetText) -and -not [string]::IsNullOrWhiteSpace($focusNote)) {
        return ('{0} {1}' -f $targetText, $focusNote)
    }

    if (-not [string]::IsNullOrWhiteSpace($targetText)) {
        return $targetText
    }

    if (-not [string]::IsNullOrWhiteSpace($focusNote)) {
        return $focusNote
    }

    return 'No SQL-profile interpretation was available for this run.'
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

                $badgeHtml = (
                    '<span class="{0}" title="{1}"><span class="setting-key">{2}</span><span class="setting-value">{3}</span></span>' -f
                    $className,
                    [System.Net.WebUtility]::HtmlEncode($title),
                    [System.Net.WebUtility]::HtmlEncode($label),
                    [System.Net.WebUtility]::HtmlEncode($value)
                )
            $badges.Add($badgeHtml)
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
        [string]$WorkloadProfile,

                [Parameter(Mandatory)]
                [object[]]$Runs
        )

        $orderedRuns = @($Runs | Sort-Object TimestampUtc)
        $recentRuns = @($orderedRuns | Select-Object -Last 6)
        $maxReadBandwidth = @($recentRuns | ForEach-Object { $_.Read.BandwidthMBps } | Where-Object { $null -ne $_ } | Measure-Object -Maximum).Maximum
        $maxWriteBandwidth = @($recentRuns | ForEach-Object { $_.Write.BandwidthMBps } | Where-Object { $null -ne $_ } | Measure-Object -Maximum).Maximum
        if ($null -eq $maxReadBandwidth -or $maxReadBandwidth -le 0) { $maxReadBandwidth = 1 }
        if ($null -eq $maxWriteBandwidth -or $maxWriteBandwidth -le 0) { $maxWriteBandwidth = 1 }

        $rows = New-Object System.Collections.Generic.List[string]
        for ($index = 0; $index -lt $recentRuns.Count; $index++) {
                $run = $recentRuns[$index]
                $previous = if ($index -gt 0) { $recentRuns[$index - 1] } else { $null }
                $readP99Ms = if ($null -ne $run.Read.P99LatencyUs) { [math]::Round(($run.Read.P99LatencyUs / 1000.0), 2) } else { $null }
                $writeP99Ms = if ($null -ne $run.Write.P99LatencyUs) { [math]::Round(($run.Write.P99LatencyUs / 1000.0), 2) } else { $null }
                $previousReadP99Ms = if ($null -ne $previous -and $null -ne $previous.Read.P99LatencyUs) { [math]::Round(($previous.Read.P99LatencyUs / 1000.0), 2) } else { $null }
                $previousWriteP99Ms = if ($null -ne $previous -and $null -ne $previous.Write.P99LatencyUs) { [math]::Round(($previous.Write.P99LatencyUs / 1000.0), 2) } else { $null }
                $readIopsDelta = Format-FioHtmlDelta -Current $run.Read.Iops -Previous $(if ($null -ne $previous) { $previous.Read.Iops } else { $null })
                $writeIopsDelta = Format-FioHtmlDelta -Current $run.Write.Iops -Previous $(if ($null -ne $previous) { $previous.Write.Iops } else { $null })
                $readDelta = Format-FioHtmlDelta -Current $run.Read.BandwidthMBps -Previous $(if ($null -ne $previous) { $previous.Read.BandwidthMBps } else { $null }) -Suffix ' MB/s'
                $writeDelta = Format-FioHtmlDelta -Current $run.Write.BandwidthMBps -Previous $(if ($null -ne $previous) { $previous.Write.BandwidthMBps } else { $null }) -Suffix ' MB/s'
                $readLatencyDelta = Format-FioHtmlDelta -Current $readP99Ms -Previous $previousReadP99Ms -Suffix ' ms' -LowerIsBetter
                $writeLatencyDelta = Format-FioHtmlDelta -Current $writeP99Ms -Previous $previousWriteP99Ms -Suffix ' ms' -LowerIsBetter
                $readWidth = [math]::Round((($run.Read.BandwidthMBps / $maxReadBandwidth) * 100.0), 2)
                $writeWidth = [math]::Round((($run.Write.BandwidthMBps / $maxWriteBandwidth) * 100.0), 2)
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
        <div>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $run.Read.Iops))) IOPS</div>
        <div class='delta $($readIopsDelta.Class)'>$([System.Net.WebUtility]::HtmlEncode($readIopsDelta.Text)) IOPS</div>
        <div class='metric-cell'>
            <span>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $run.Read.BandwidthMBps -Suffix ' MB/s')))</span>
            <div class='mini-track'><div class='mini-fill throughput-read' style='width: ${readWidth}%'></div></div>
        </div>
        <div class='delta $($readDelta.Class)'>$([System.Net.WebUtility]::HtmlEncode($readDelta.Text))</div>
    </td>
    <td>
        <div>$([System.Net.WebUtility]::HtmlEncode((Format-FioHtmlMetric -Value $run.Write.Iops))) IOPS</div>
        <div class='delta $($writeIopsDelta.Class)'>$([System.Net.WebUtility]::HtmlEncode($writeIopsDelta.Text)) IOPS</div>
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
            <h2>$([System.Net.WebUtility]::HtmlEncode($WorkloadProfile))</h2>
            <p>Recent runs for this workload profile, with each row compared against the previous run in the same profile.</p>
        </div>
        <div class='pill'>$($recentRuns.Count) runs shown</div>
    </div>
    <table>
        <thead>
            <tr>
                <th>Run Time</th>
                <th>Target</th>
                <th>Read Performance</th>
                <th>Write Performance</th>
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

function New-FioHtmlLineChartSection {
        [CmdletBinding()]
        param(
                [Parameter(Mandatory)]
                [string]$Title,

                [Parameter(Mandatory)]
                [object[]]$Series,

                [string]$Subtitle,

                [int]$Height = 240
        )

        $allPoints = @($Series | ForEach-Object { @($_.Points) })
        if ($allPoints.Count -eq 0) {
                return @"
<section class='chart-card diagnostics-card'>
    <div class='section-heading'>
        <div>
            <h2>$([System.Net.WebUtility]::HtmlEncode($Title))</h2>
            <p>$([System.Net.WebUtility]::HtmlEncode($(if ($Subtitle) { $Subtitle } else { 'No diagnostics captured.' })))</p>
        </div>
    </div>
</section>
"@
        }

        $paddingLeft = 50
        $paddingRight = 18
        $paddingTop = 14
        $paddingBottom = 30
        $width = 900
        $plotWidth = $width - $paddingLeft - $paddingRight
        $plotHeight = $Height - $paddingTop - $paddingBottom

        $times = @($allPoints | ForEach-Object { [double]$_.TimeMs })
        $values = @($allPoints | ForEach-Object { [double]$_.Value })
        $maxTime = ($times | Measure-Object -Maximum).Maximum
        $minTime = ($times | Measure-Object -Minimum).Minimum
        $maxValue = ($values | Measure-Object -Maximum).Maximum
        $minValue = ($values | Measure-Object -Minimum).Minimum
        if ($null -eq $maxTime -or $maxTime -le $minTime) { $maxTime = [math]::Max(1, [double]$maxTime) }
        if ($null -eq $maxValue) { $maxValue = 1 }
        if ($null -eq $minValue) { $minValue = 0 }
        if ($maxValue -le $minValue) {
                if ($maxValue -eq 0) {
                        $maxValue = 1
                }
                else {
                        $minValue = 0
                }
        }

        $gridLines = New-Object System.Collections.Generic.List[string]
        for ($tick = 0; $tick -lt 5; $tick++) {
                $ratio = $tick / 4.0
                $y = [math]::Round($paddingTop + ($plotHeight * $ratio), 2)
                $gridLines.Add("<line x1='$paddingLeft' y1='$y' x2='$(($paddingLeft + $plotWidth))' y2='$y' class='diagnostic-grid-line' />")
        }

        $palette = @{
                'read' = '#0f766e'
                'write' = '#b45309'
                'latencyavg-read' = '#2563eb'
                'latencymax-read' = '#60a5fa'
                'latencyavg-write' = '#7c3aed'
                'latencymax-write' = '#c084fc'
            'perfmonlatency-read' = '#1d4ed8'
            'perfmonlatency-write' = '#db2777'
            'perfmonqueue-client' = '#4f46e5'
            'perfmoncreditstalls-client' = '#dc2626'
        }

        $seriesMarkup = New-Object System.Collections.Generic.List[string]
        $legendItems = New-Object System.Collections.Generic.List[string]
        foreach ($item in $Series) {
                $metricDirectionKey = '{0}-{1}' -f $item.Metric.ToLowerInvariant(), $item.Direction.ToLowerInvariant()
                $fallbackKey = $item.Direction.ToLowerInvariant()
                $color = if ($palette.ContainsKey($metricDirectionKey)) { $palette[$metricDirectionKey] } elseif ($palette.ContainsKey($fallbackKey)) { $palette[$fallbackKey] } else { '#1f2937' }
                $points = @(
                        foreach ($point in @($item.Points)) {
                                $timeValue = [double]$point.TimeMs
                                $value = [double]$point.Value
                                $x = if ($maxTime -le $minTime) { $paddingLeft } else { [math]::Round($paddingLeft + ((($timeValue - $minTime) / ($maxTime - $minTime)) * $plotWidth), 2) }
                                $y = [math]::Round($paddingTop + (1 - (($value - $minValue) / ($maxValue - $minValue))) * $plotHeight, 2)
                                '{0},{1}' -f $x, $y
                        }
                )

                $seriesMarkup.Add("<polyline fill='none' stroke='$color' stroke-width='3' stroke-linejoin='round' stroke-linecap='round' points='$($points -join ' ')' />")
                $legendItems.Add("<span class='diagnostic-legend-item'><span class='diagnostic-legend-swatch' style='background: $color'></span>$([System.Net.WebUtility]::HtmlEncode([string]$item.Label))</span>")
        }

        $yTop = Format-FioHtmlMetric -Value $maxValue -Suffix (' ' + $Series[0].Unit) -Decimals $Series[0].PreferredDecimals
        $yMid = Format-FioHtmlMetric -Value ([math]::Round((($maxValue + $minValue) / 2.0), $Series[0].PreferredDecimals)) -Suffix (' ' + $Series[0].Unit) -Decimals $Series[0].PreferredDecimals
        $yBottom = Format-FioHtmlMetric -Value $minValue -Suffix (' ' + $Series[0].Unit) -Decimals $Series[0].PreferredDecimals
        $xStart = '0s'
        $xEnd = '{0:N0}s' -f ([math]::Round(($maxTime / 1000.0), 0))

        return @"
<section class='chart-card diagnostics-card'>
    <div class='section-heading'>
        <div>
            <h2>$([System.Net.WebUtility]::HtmlEncode($Title))</h2>
            <p>$([System.Net.WebUtility]::HtmlEncode($Subtitle))</p>
        </div>
    </div>
    <div class='diagnostic-legend'>
        $($legendItems -join [Environment]::NewLine)
    </div>
    <div class='diagnostic-chart-wrap'>
        <div class='diagnostic-axis diagnostic-axis-y'>
            <span>$([System.Net.WebUtility]::HtmlEncode($yTop))</span>
            <span>$([System.Net.WebUtility]::HtmlEncode($yMid))</span>
            <span>$([System.Net.WebUtility]::HtmlEncode($yBottom))</span>
        </div>
        <svg viewBox='0 0 $width $Height' class='diagnostic-chart' role='img' aria-label='$([System.Net.WebUtility]::HtmlEncode($Title))'>
            <rect x='$paddingLeft' y='$paddingTop' width='$plotWidth' height='$plotHeight' class='diagnostic-plot-bg' />
            $($gridLines -join [Environment]::NewLine)
            $($seriesMarkup -join [Environment]::NewLine)
        </svg>
    </div>
    <div class='diagnostic-axis diagnostic-axis-x'>
        <span>$xStart</span>
        <span>$xEnd</span>
    </div>
</section>
"@
}

function New-FioHtmlDiagnosticsSection {
        [CmdletBinding()]
        param(
                [AllowNull()]
                [object]$Diagnostics
        )

        $state = Get-FioDiagnosticsRenderState -Diagnostics $Diagnostics
        $message = if ($null -ne $Diagnostics -and $Diagnostics.PSObject.Properties['Message'] -and -not [string]::IsNullOrWhiteSpace([string]$Diagnostics.Message)) {
            [string]$Diagnostics.Message
        }
        elseif (-not $state.Requested) {
            'Diagnostics were not requested for this run. Re-run with -EnableLogs to emit windowed throughput, IOPS, and latency logs.'
        }
        elseif (-not $state.Available) {
            'Diagnostics were requested, but no chartable fio telemetry was captured. Check the iteration console log for fio log creation errors.'
        }
        else {
            'Windowed fio telemetry aggregated across per-job logs so throughput stability, throttling, cache warm-up, and transient latency spikes are visible in the report.'
        }

        $summaryItems = @(
                [pscustomobject]@{ Label = 'Status'; Value = $state.Label },
                [pscustomobject]@{ Label = 'Window'; Value = if ($null -ne $Diagnostics -and $Diagnostics.PSObject.Properties['LogWindowMs']) { '{0} ms' -f $Diagnostics.LogWindowMs } else { '{0} ms' -f $script:FioDiagnosticsWindowMs } },
            [pscustomobject]@{ Label = 'Warmup trim'; Value = if ($null -ne $Diagnostics -and $Diagnostics.PSObject.Properties['WarmupTrimSamples']) { '{0} samples' -f $Diagnostics.WarmupTrimSamples } else { '{0} samples' -f $script:FioDiagnosticsWarmupTrimSamples } },
            [pscustomobject]@{ Label = 'Smoothing'; Value = if ($null -ne $Diagnostics -and $Diagnostics.PSObject.Properties['SmoothingWindowSamples']) { '{0}-sample moving avg' -f $Diagnostics.SmoothingWindowSamples } else { '{0}-sample moving avg' -f $script:FioDiagnosticsSmoothingWindowSamples } },
                [pscustomobject]@{ Label = 'Duration'; Value = if ($null -ne $Diagnostics -and $Diagnostics.PSObject.Properties['TimeRangeMs'] -and $null -ne $Diagnostics.TimeRangeMs) { '{0:N1} s' -f ($Diagnostics.TimeRangeMs / 1000.0) } else { '-' } },
                [pscustomobject]@{ Label = 'Series'; Value = if ($null -ne $Diagnostics -and $Diagnostics.PSObject.Properties['Series']) { [string](@($Diagnostics.Series).Count) } else { '0' } },
                [pscustomobject]@{ Label = 'Source files'; Value = if ($null -ne $Diagnostics -and $Diagnostics.PSObject.Properties['SourceFiles']) { [string](@($Diagnostics.SourceFiles).Count) } else { '0' } }
        )

        if ($null -ne $Diagnostics -and $Diagnostics.PSObject.Properties['Throttling'] -and $null -ne $Diagnostics.Throttling -and $Diagnostics.Throttling.PSObject.Properties['Available'] -and $Diagnostics.Throttling.Available) {
            $summaryItems += [pscustomobject]@{ Label = 'Saturation'; Value = [string]$Diagnostics.Throttling.Label }
        }

        if ($null -ne $Diagnostics -and $Diagnostics.PSObject.Properties['MissingMetrics'] -and @($Diagnostics.MissingMetrics).Count -gt 0) {
            $summaryItems += [pscustomobject]@{ Label = 'Missing'; Value = (@($Diagnostics.MissingMetrics) -join ', ') }
        }

        $summaryBadges = @(
                foreach ($item in $summaryItems) {
                        "<span class='setting-badge'><span class='setting-key'>$([System.Net.WebUtility]::HtmlEncode($item.Label))</span><span class='setting-value'>$([System.Net.WebUtility]::HtmlEncode($item.Value))</span></span>"
                }
        )

        $notice = "<div class='diagnostic-notice $($state.Class)'><strong>$([System.Net.WebUtility]::HtmlEncode($state.Label)):</strong> $([System.Net.WebUtility]::HtmlEncode($message))</div>"

        if ($null -eq $Diagnostics -or -not $Diagnostics.Available) {
                return @"
<section class='table-card diagnostics-card'>
    <div class='section-heading'>
        <div>
            <h2>Diagnostics</h2>
            <p>Diagnostics state and collection health for this run.</p>
        </div>
    </div>
    <div class='settings-badges'>
        $($summaryBadges -join [Environment]::NewLine)
    </div>
    $notice
</section>
"@
        }

        $throughputSeries = @($Diagnostics.Series | Where-Object { $_.Metric -eq 'bandwidth' })
        $stabilitySeries = @($Diagnostics.Series | Where-Object { $_.Metric -eq 'bandwidthStability' })
        $iopsSeries = @($Diagnostics.Series | Where-Object { $_.Metric -eq 'iops' })
        $latencySeries = @($Diagnostics.Series | Where-Object { $_.Metric -like 'latency*' })
        $perfmonLatencySeries = @($Diagnostics.Series | Where-Object { $_.Metric -eq 'perfmonLatency' })
        $perfmonQueueSeries = @($Diagnostics.Series | Where-Object { $_.Metric -eq 'perfmonQueue' })
        $perfmonCreditStallSeries = @($Diagnostics.Series | Where-Object { $_.Metric -eq 'perfmonCreditStalls' })
        $cards = New-Object System.Collections.Generic.List[string]
        $cards.Add((New-FioHtmlLineChartSection -Title 'Throughput Over Time' -Series $throughputSeries -Subtitle ('Steady-state fio bandwidth view sampled every {0} ms after trimming the first {1} samples and applying a moving average.' -f $Diagnostics.LogWindowMs, $(if ($Diagnostics.PSObject.Properties['WarmupTrimSamples']) { $Diagnostics.WarmupTrimSamples } else { $script:FioDiagnosticsWarmupTrimSamples }))))
        $cards.Add((New-FioHtmlLineChartSection -Title 'BW Stability Over Time' -Series $stabilitySeries -Subtitle ('Rolling {0}-sample coefficient of variation from the steady-state bandwidth view. Lower is steadier.' -f $script:FioDiagnosticsStabilityWindowSamples)))
        $cards.Add((New-FioHtmlLineChartSection -Title 'IOPS Over Time' -Series $iopsSeries -Subtitle ('Steady-state fio IOPS view sampled every {0} ms after trimming the first {1} samples and applying a moving average.' -f $Diagnostics.LogWindowMs, $(if ($Diagnostics.PSObject.Properties['WarmupTrimSamples']) { $Diagnostics.WarmupTrimSamples } else { $script:FioDiagnosticsWarmupTrimSamples }))))
        $cards.Add((New-FioHtmlLineChartSection -Title 'Completion Latency Over Time' -Series $latencySeries -Subtitle 'Steady-state completion latency view after warmup trimming and light smoothing, useful for spotting sustained stalls rather than one-window spikes.'))
        if ($perfmonLatencySeries.Count -gt 0) {
            $cards.Add((New-FioHtmlLineChartSection -Title 'SMB Client Latency Over Time' -Series $perfmonLatencySeries -Subtitle 'PerfMon SMB client read/write latency counters. This is client-observed SMB latency, useful for spotting queueing beyond storage-service latency.'))
        }
        if ($perfmonQueueSeries.Count -gt 0) {
            $cards.Add((New-FioHtmlLineChartSection -Title 'SMB Queue Over Time' -Series $perfmonQueueSeries -Subtitle 'PerfMon SMB client queue depth. Sustained growth here is a strong saturation signal when throughput plateaus.'))
        }
        if ($perfmonCreditStallSeries.Count -gt 0) {
            $cards.Add((New-FioHtmlLineChartSection -Title 'SMB Credit Stalls Over Time' -Series $perfmonCreditStallSeries -Subtitle 'PerfMon SMB credit stalls per second. Non-zero values suggest the SMB client is being gated while the path is saturated.'))
        }

        return @"
<section class='table-card diagnostics-card'>
    <div class='section-heading'>
        <div>
            <h2>Diagnostics</h2>
            <p>Diagnostics state and chartable fio telemetry for this run.</p>
        </div>
    </div>
    <div class='settings-badges'>
        $($summaryBadges -join [Environment]::NewLine)
    </div>
    $notice
    <div class='chart-grid diagnostics-grid'>
        $($cards -join [Environment]::NewLine)
    </div>
</section>
"@
}

function Get-FioDiagnosticsSeriesColor {
    [CmdletBinding()]
    param(
        [string]$Metric,
        [string]$Direction
    )

    switch ('{0}-{1}' -f $Metric.ToLowerInvariant(), $Direction.ToLowerInvariant()) {
        'bandwidth-read' { return '#0f766e' }
        'bandwidth-write' { return '#b45309' }
        'bandwidthstability-read' { return '#0891b2' }
        'bandwidthstability-write' { return '#dc2626' }
        'iops-read' { return '#0f766e' }
        'iops-write' { return '#b45309' }
        'latencyavg-read' { return '#2563eb' }
        'latencymax-read' { return '#60a5fa' }
        'latencyavg-write' { return '#7c3aed' }
        'latencymax-write' { return '#c084fc' }
        'perfmonlatency-read' { return '#1d4ed8' }
        'perfmonlatency-write' { return '#db2777' }
        'perfmonqueue-client' { return '#4f46e5' }
        'perfmoncreditstalls-client' { return '#dc2626' }
        default { return '#1f2937' }
    }
}

function Get-FioDiagnosticsChartSampleTimes {
    [CmdletBinding()]
    param(
        [int[]]$Times,

        [int]$MaxPoints = 24
    )

    if ($null -eq $Times -or $Times.Count -eq 0) {
        return @()
    }

    if ($Times.Count -le $MaxPoints) {
        return @($Times)
    }

    $targetDivisor = [math]::Max(($MaxPoints - 1), 1)
    $step = [math]::Max([int][math]::Ceiling(($Times.Count - 1) / [double]$targetDivisor), 1)
    $sampleTimes = New-Object System.Collections.Generic.List[int]
    for ($index = 0; $index -lt $Times.Count; $index++) {
        $isEdge = $index -eq 0 -or $index -eq ($Times.Count - 1)
        if ($isEdge -or ($index % $step -eq 0)) {
            $sampleTimes.Add([int]$Times[$index])
        }
    }

    return @($sampleTimes.ToArray() | Sort-Object -Unique)
}

function Get-FioDiagnosticsChartModels {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$Diagnostics
    )

    if ($null -eq $Diagnostics -or -not $Diagnostics.Available -or @($Diagnostics.Series).Count -eq 0) {
        return @()
    }

    $definitions = @(
        [pscustomobject]@{ Key = 'bandwidth'; Title = 'Throughput Over Time'; Subtitle = 'Steady-state fio bandwidth samples across the selected run, after warmup trimming and moving-average smoothing.'; YAxisTitle = 'MB/s' }
        [pscustomobject]@{ Key = 'bandwidthStability'; Title = 'BW Stability Over Time'; Subtitle = 'Rolling bandwidth coefficient of variation from the steady-state view. Lower percentages indicate steadier delivery.'; YAxisTitle = 'CV %' }
        [pscustomobject]@{ Key = 'iops'; Title = 'IOPS Over Time'; Subtitle = 'Steady-state fio IOPS samples across the selected run, after warmup trimming and moving-average smoothing.'; YAxisTitle = 'IOPS' }
        [pscustomobject]@{ Key = 'latency'; Title = 'Completion Latency Over Time'; Subtitle = 'Steady-state average and max completion latency, useful for spotting sustained stalls and throttling.'; YAxisTitle = 'Milliseconds' }
        [pscustomobject]@{ Key = 'perfmonLatency'; Title = 'SMB Client Latency Over Time'; Subtitle = 'PerfMon SMB client read/write latency counters captured on the benchmark host, trimmed to the steady-state window.'; YAxisTitle = 'Milliseconds' }
        [pscustomobject]@{ Key = 'perfmonQueue'; Title = 'SMB Queue Over Time'; Subtitle = 'PerfMon SMB client queue depth. Sustained buildup usually means the path is saturated or being throttled.'; YAxisTitle = 'Queue depth' }
        [pscustomobject]@{ Key = 'perfmonCreditStalls'; Title = 'SMB Credit Stalls Over Time'; Subtitle = 'PerfMon SMB credit stalls per second. Non-zero values suggest client-side flow control while saturated.'; YAxisTitle = 'Stalls/sec' }
    )

    $models = New-Object System.Collections.Generic.List[object]
    foreach ($definition in $definitions) {
        $metricSeries = @(
            if ($definition.Key -eq 'latency') {
                @($Diagnostics.Series | Where-Object { $_.Metric -like 'latency*' })
            }
            else {
                @($Diagnostics.Series | Where-Object { $_.Metric -eq $definition.Key })
            }
        )

        if (@($metricSeries).Count -eq 0) {
            continue
        }

        $times = @($metricSeries | ForEach-Object { @($_.Points) } | ForEach-Object { [int]$_.TimeMs } | Sort-Object -Unique)
        $chartTimes = @(Get-FioDiagnosticsChartSampleTimes -Times $times)
        $categories = @($chartTimes | ForEach-Object { '{0:0.##}s' -f ($_ / 1000.0) })
        $seriesRows = New-Object System.Collections.Generic.List[object]
        foreach ($series in $metricSeries) {
            $pointMap = @{}
            foreach ($point in @($series.Points)) {
                $pointMap[[int]$point.TimeMs] = [double]$point.Value
            }

            $values = @(
                foreach ($time in $chartTimes) {
                    if ($pointMap.ContainsKey($time)) {
                        [math]::Round([double]$pointMap[$time], 4)
                    }
                    else {
                        $null
                    }
                }
            )

            $seriesRows.Add([pscustomobject]@{
                Name = [string]$series.Label
                Color = Get-FioDiagnosticsSeriesColor -Metric ([string]$series.Metric) -Direction ([string]$series.Direction)
                Values = $values
            })
        }

        $pointRows = @(
            foreach ($time in $times) {
                $row = [ordered]@{
                    TimeSec = [math]::Round(($time / 1000.0), 3)
                }

                foreach ($series in $metricSeries) {
                    $matchingPoint = @($series.Points | Where-Object { [int]$_.TimeMs -eq $time } | Select-Object -First 1)
                    $pointValue = $null
                    if ($matchingPoint.Count -gt 0) {
                        $pointValue = [math]::Round([double]$matchingPoint[0].Value, 4)
                    }

                    $row[[string]$series.Label] = $pointValue
                }

                [pscustomobject]$row
            }
        )

        $models.Add([pscustomobject]@{
            Key = $definition.Key
            Title = $definition.Title
            Subtitle = $definition.Subtitle
            YAxisTitle = $definition.YAxisTitle
            Categories = [string[]]$categories
            Series = $seriesRows.ToArray()
            PointRows = @($pointRows)
        })
    }

    return $models.ToArray()
}

function Export-FioSqlBenchHtmlReportStatic {
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

        function local:ConvertToHtmlText {
                param([AllowNull()][object]$Value)

                if ($null -eq $Value) {
                        return '-'
                }

                $text = [string]$Value
                if ([string]::IsNullOrWhiteSpace($text)) {
                        return '-'
                }

                return [System.Net.WebUtility]::HtmlEncode($text)
        }

        function local:NewHtmlTableMarkup {
                param(
                        [object[]]$Rows,
                        [string]$TableClass = 'report-table'
                )

                if ($null -eq $Rows -or $Rows.Count -eq 0) {
                        return "<div class='empty-state'>No rows to display.</div>"
                }

                $headers = @($Rows[0].PSObject.Properties | ForEach-Object { $_.Name })
                $headerHtml = ($headers | ForEach-Object { '<th>{0}</th>' -f (ConvertToHtmlText $_) }) -join ''
                $rowHtml = New-Object System.Collections.Generic.List[string]
                foreach ($row in $Rows) {
                        $cells = foreach ($header in $headers) {
                                $value = $row.PSObject.Properties[$header].Value
                                '<td>{0}</td>' -f (ConvertToHtmlText $value)
                        }
                        $rowHtml.Add('<tr>{0}</tr>' -f ($cells -join ''))
                }

                return @"
<div class='table-shell'>
        <table class='$TableClass'>
                <thead>
                        <tr>$headerHtml</tr>
                </thead>
                <tbody>
                        $($rowHtml -join [Environment]::NewLine)
                </tbody>
        </table>
</div>
"@
        }

        function local:NewCollapsibleTableMarkup {
                param(
                        [string]$Title,
                        [object[]]$Rows
                )

                $rowCount = if ($null -ne $Rows) { $Rows.Count } else { 0 }
                $tableMarkup = NewHtmlTableMarkup -Rows $Rows -TableClass 'report-table report-table-compact'
                return @"
<details class='collapsible-table'>
        <summary>$([System.Net.WebUtility]::HtmlEncode($Title)) ($rowCount rows)</summary>
        $tableMarkup
</details>
"@
        }

    function local:NewCollapsibleSectionMarkup {
        param(
            [Parameter(Mandatory)]
            [string]$Id,

            [Parameter(Mandatory)]
            [string]$Title,

            [Parameter(Mandatory)]
            [string]$Description,

            [string]$BadgeText,

            [Parameter(Mandatory)]
            [string]$InnerHtml,

            [switch]$Open
        )

        return @"
<details id='$([System.Net.WebUtility]::HtmlEncode($Id))' class='section-toggle section-card'$(if ($Open) { ' open' })>
    <summary>
        <div class='section-toggle-head'>
            <div>
                <h2>$([System.Net.WebUtility]::HtmlEncode($Title))</h2>
                <p>$([System.Net.WebUtility]::HtmlEncode($Description))</p>
            </div>
            $(if (-not [string]::IsNullOrWhiteSpace($BadgeText)) { "<div class='pill'>$([System.Net.WebUtility]::HtmlEncode($BadgeText))</div>" } else { '' })
        </div>
    </summary>
    <div class='section-toggle-body'>
        $InnerHtml
    </div>
</details>
"@
    }

        function local:GetDiagnosticsSeriesByKey {
                param(
                        [object]$Diagnostics,
                        [string]$Key
                )

                if ($null -eq $Diagnostics -or $null -eq $Diagnostics.Series) {
                        return @()
                }

                switch ($Key) {
                        'bandwidth' { return @($Diagnostics.Series | Where-Object { $_.Metric -eq 'bandwidth' }) }
                    'bandwidthStability' { return @($Diagnostics.Series | Where-Object { $_.Metric -eq 'bandwidthStability' }) }
                        'iops' { return @($Diagnostics.Series | Where-Object { $_.Metric -eq 'iops' }) }
                        'latency' { return @($Diagnostics.Series | Where-Object { $_.Metric -like 'latency*' }) }
                    'perfmonLatency' { return @($Diagnostics.Series | Where-Object { $_.Metric -eq 'perfmonLatency' }) }
                    'perfmonQueue' { return @($Diagnostics.Series | Where-Object { $_.Metric -eq 'perfmonQueue' }) }
                    'perfmonCreditStalls' { return @($Diagnostics.Series | Where-Object { $_.Metric -eq 'perfmonCreditStalls' }) }
                        default { return @() }
                }
        }

            function local:GetProfileFocusMetadata {
                param(
                    [string]$Profile
                )

                switch ($Profile) {
                    'MaxIOPs' {
                        return [pscustomobject]@{
                            Category = 'Iops'
                            Label = 'Peak IOPS'
                        }
                    }
                    'Data' {
                        return [pscustomobject]@{
                            Category = 'Iops'
                            Label = 'Mixed IOPS'
                        }
                    }
                    'Tempdb' {
                        return [pscustomobject]@{
                            Category = 'Iops'
                            Label = 'Scratch IOPS'
                        }
                    }
                    'Log' {
                        return [pscustomobject]@{
                            Category = 'Latency'
                            Label = 'Write latency'
                        }
                    }
                    'BackupRestore' {
                        return [pscustomobject]@{
                            Category = 'Throughput'
                            Label = 'Backup throughput'
                        }
                    }
                    'DbccScan' {
                        return [pscustomobject]@{
                            Category = 'Throughput'
                            Label = 'Scan throughput'
                        }
                    }
                    'MaxThroughput' {
                        return [pscustomobject]@{
                            Category = 'Throughput'
                            Label = 'Peak throughput'
                        }
                    }
                    default {
                        return [pscustomobject]@{
                            Category = 'Throughput'
                            Label = 'General throughput'
                        }
                    }
                }
            }

            function local:GetOperationFocusMetricModel {
                param(
                    [Parameter(Mandatory)]
                    [object]$Run,

                    [Parameter(Mandatory)]
                    [string]$Operation,

                    [Parameter(Mandatory)]
                    [object]$Focus
                )

                $stats = if ($Operation -eq 'Read') { $Run.Read } else { $Run.Write }
                $bandwidthText = '{0:N2} MB/s' -f [double]$stats.BandwidthMBps
                $iopsText = '{0:N0} IOPS' -f [double]$stats.Iops
                $p99Ms = if ($null -ne $stats.P99LatencyUs) { '{0:N2} ms' -f ($stats.P99LatencyUs / 1000.0) } else { '-' }

                switch ($Focus.Category) {
                    'Iops' {
                        return [pscustomobject]@{
                            Label = ('{0} IOPS' -f $Operation)
                            Primary = $iopsText
                            Secondary = $bandwidthText
                        }
                    }
                    'Latency' {
                        return [pscustomobject]@{
                            Label = ('{0} p99' -f $Operation)
                            Primary = $p99Ms
                            Secondary = $iopsText
                        }
                    }
                    default {
                        return [pscustomobject]@{
                            Label = ('{0} throughput' -f $Operation)
                            Primary = $bandwidthText
                            Secondary = $iopsText
                        }
                    }
                }
            }

        if ($Runs.Count -eq 0) {
                throw 'Cannot build an HTML report without any runs.'
        }

        $orderedRuns = @($Runs | Sort-Object TimestampUtc)
        $latestRun = @($orderedRuns | Sort-Object TimestampUtc -Descending | Select-Object -First 1)[0]
        $rollupRows = if ($null -ne $Rollups) { @($Rollups) } else { @(Get-FioHistoricalRollup -Runs $orderedRuns) }
        $profileGroups = @($orderedRuns | Group-Object Profile | Sort-Object Name)
        $profileLatestRuns = @(
                foreach ($profileGroup in $profileGroups) {
                        @($profileGroup.Group | Sort-Object TimestampUtc -Descending | Select-Object -First 1)[0]
                }
        )
        $profileDiagnosticsSelections = @(
                foreach ($profileGroup in $profileGroups) {
                        $candidateRuns = @($profileGroup.Group | Sort-Object TimestampUtc -Descending)
                        $selectedDiagnosticsRun = @($candidateRuns | Where-Object { $_.Diagnostics -and $_.Diagnostics.Available } | Select-Object -First 1)[0]
                        if ($null -eq $selectedDiagnosticsRun) {
                                $selectedDiagnosticsRun = @($candidateRuns | Where-Object { $_.Diagnostics -and $_.Diagnostics.Requested } | Select-Object -First 1)[0]
                        }
                        if ($null -eq $selectedDiagnosticsRun) {
                                $selectedDiagnosticsRun = $candidateRuns[0]
                        }

                        [pscustomobject]@{
                                Profile = [string]$profileGroup.Name
                                Run = $selectedDiagnosticsRun
                                State = Get-FioDiagnosticsRenderState -Diagnostics $selectedDiagnosticsRun.Diagnostics
                                TotalRuns = $candidateRuns.Count
                                AvailableRuns = @($candidateRuns | Where-Object { $_.Diagnostics -and $_.Diagnostics.Available }).Count
                                RequestedRuns = @($candidateRuns | Where-Object { $_.Diagnostics -and $_.Diagnostics.Requested }).Count
                        }
                }
        )

        $diagnosticsReadyProfiles = @($profileDiagnosticsSelections | Where-Object { $_.State.Available }).Count
        $iopsFocusedLatestRuns = @($profileLatestRuns | Where-Object { (GetProfileFocusMetadata -Profile ([string]$_.Profile)).Category -eq 'Iops' })
        if ($iopsFocusedLatestRuns.Count -eq 0) {
            $iopsFocusedLatestRuns = $profileLatestRuns
        }
        $throughputFocusedLatestRuns = @($profileLatestRuns | Where-Object { (GetProfileFocusMetadata -Profile ([string]$_.Profile)).Category -eq 'Throughput' })
        if ($throughputFocusedLatestRuns.Count -eq 0) {
            $throughputFocusedLatestRuns = $profileLatestRuns
        }
        $bestReadIopsProfileRun = @($iopsFocusedLatestRuns | Sort-Object { [double]$_.Read.Iops } -Descending | Select-Object -First 1)[0]
        $bestWriteIopsProfileRun = @($iopsFocusedLatestRuns | Sort-Object { [double]$_.Write.Iops } -Descending | Select-Object -First 1)[0]
        $bestReadThroughputProfileRun = @($throughputFocusedLatestRuns | Sort-Object { [double]$_.Read.BandwidthMBps } -Descending | Select-Object -First 1)[0]
        $bestWriteThroughputProfileRun = @($throughputFocusedLatestRuns | Sort-Object { [double]$_.Write.BandwidthMBps } -Descending | Select-Object -First 1)[0]
        $lowestMeanLatencyResult = @(
            foreach ($run in $profileLatestRuns) {
                foreach ($row in @(Get-FioHtmlActiveAssessmentRows -Run $run | Where-Object { $null -ne $_.MeanMs -and $_.MeanMs -gt 0 })) {
                    [pscustomobject]@{
                        Profile = [string]$run.Profile
                        Operation = [string]$row.Operation
                        MeanMs = [double]$row.MeanMs
                        Assessment = [string]$row.DisplayStatus
                    }
                }
            }
        )
        $lowestReadLatencyResult = $lowestMeanLatencyResult | Where-Object { $_.Operation -eq 'Read' } | Sort-Object MeanMs, Profile | Select-Object -First 1
        $lowestWriteLatencyResult = $lowestMeanLatencyResult | Where-Object { $_.Operation -eq 'Write' } | Sort-Object MeanMs, Profile | Select-Object -First 1
        $newestProfileSnapshot = @($profileLatestRuns | Sort-Object TimestampUtc -Descending | Select-Object -First 1)[0]

        $heroMetricCards = @(
                [pscustomobject]@{ Label = 'Profiles represented'; Value = [string]$profileLatestRuns.Count }
                [pscustomobject]@{ Label = 'Captured runs'; Value = [string]$orderedRuns.Count }
                [pscustomobject]@{ Label = 'Diagnostics-ready'; Value = [string]$diagnosticsReadyProfiles }
                [pscustomobject]@{ Label = 'Latest captured'; Value = (Format-FioHtmlTimestamp -TimestampUtc $latestRun.TimestampUtc) }
        )

        $heroMetricsHtml = foreach ($metric in $heroMetricCards) {
                @"
<article class='metric-card'>
        <div class='metric-label'>$([System.Net.WebUtility]::HtmlEncode([string]$metric.Label))</div>
        <div class='metric-value'>$([System.Net.WebUtility]::HtmlEncode([string]$metric.Value))</div>
</article>
"@
        }

        $bestReadIopsProfileHtml = [System.Net.WebUtility]::HtmlEncode([string]$bestReadIopsProfileRun.Profile)
        $bestReadIopsValueHtml = [System.Net.WebUtility]::HtmlEncode(('{0:N0} IOPS' -f [double]$bestReadIopsProfileRun.Read.Iops))
        $bestWriteIopsProfileHtml = [System.Net.WebUtility]::HtmlEncode([string]$bestWriteIopsProfileRun.Profile)
        $bestWriteIopsValueHtml = [System.Net.WebUtility]::HtmlEncode(('{0:N0} IOPS' -f [double]$bestWriteIopsProfileRun.Write.Iops))
        $bestReadThroughputProfileHtml = [System.Net.WebUtility]::HtmlEncode([string]$bestReadThroughputProfileRun.Profile)
        $bestReadThroughputValueHtml = [System.Net.WebUtility]::HtmlEncode(('{0:N2} MB/s' -f [double]$bestReadThroughputProfileRun.Read.BandwidthMBps))
        $bestWriteThroughputProfileHtml = [System.Net.WebUtility]::HtmlEncode([string]$bestWriteThroughputProfileRun.Profile)
        $bestWriteThroughputValueHtml = [System.Net.WebUtility]::HtmlEncode(('{0:N2} MB/s' -f [double]$bestWriteThroughputProfileRun.Write.BandwidthMBps))
        $lowestReadLatencyContextHtml = if ($null -ne $lowestReadLatencyResult) { [System.Net.WebUtility]::HtmlEncode([string]$lowestReadLatencyResult.Profile) } else { '-' }
        $lowestReadLatencyValueHtml = if ($null -ne $lowestReadLatencyResult) { [System.Net.WebUtility]::HtmlEncode(('{0:N2} ms' -f [double]$lowestReadLatencyResult.MeanMs)) } else { '-' }
        $lowestReadLatencyAssessmentHtml = if ($null -ne $lowestReadLatencyResult) { [System.Net.WebUtility]::HtmlEncode([string]$lowestReadLatencyResult.Assessment) } else { '-' }
        $lowestWriteLatencyContextHtml = if ($null -ne $lowestWriteLatencyResult) { [System.Net.WebUtility]::HtmlEncode([string]$lowestWriteLatencyResult.Profile) } else { '-' }
        $lowestWriteLatencyValueHtml = if ($null -ne $lowestWriteLatencyResult) { [System.Net.WebUtility]::HtmlEncode(('{0:N2} ms' -f [double]$lowestWriteLatencyResult.MeanMs)) } else { '-' }
        $lowestWriteLatencyAssessmentHtml = if ($null -ne $lowestWriteLatencyResult) { [System.Net.WebUtility]::HtmlEncode([string]$lowestWriteLatencyResult.Assessment) } else { '-' }

        $highlightHtml = @(
            @'
    <article class="highlight-card">
        <div class="metric-label">IOPS Leaders</div>
        <div class="highlight-stat-grid">
            <div class="highlight-stat">
                <div class="highlight-stat-label tone-read">Max Read IOPS</div>
                <div class="highlight-stat-context">{0}</div>
                <div class="highlight-metric tone-read">{1}</div>
            </div>
            <div class="highlight-stat">
                <div class="highlight-stat-label tone-write">Max Write IOPS</div>
                <div class="highlight-stat-context">{2}</div>
                <div class="highlight-metric tone-write">{3}</div>
            </div>
        </div>
    </article>
'@ -f $bestReadIopsProfileHtml, $bestReadIopsValueHtml, $bestWriteIopsProfileHtml, $bestWriteIopsValueHtml
            @'
    <article class="highlight-card">
        <div class="metric-label">Throughput Leaders</div>
        <div class="highlight-stat-grid">
            <div class="highlight-stat">
                <div class="highlight-stat-label tone-read">Max Read Throughput</div>
                <div class="highlight-stat-context">{0}</div>
                <div class="highlight-metric tone-read">{1}</div>
            </div>
            <div class="highlight-stat">
                <div class="highlight-stat-label tone-write">Max Write Throughput</div>
                <div class="highlight-stat-context">{2}</div>
                <div class="highlight-metric tone-write">{3}</div>
            </div>
        </div>
    </article>
'@ -f $bestReadThroughputProfileHtml, $bestReadThroughputValueHtml, $bestWriteThroughputProfileHtml, $bestWriteThroughputValueHtml
            @'
    <article class="highlight-card">
        <div class="metric-label">Latency Leaders</div>
        <div class="highlight-stat-grid">
            <div class="highlight-stat">
                <div class="highlight-stat-label tone-read">Lowest Read Mean Latency</div>
                <div class="highlight-stat-context">{0}</div>
                <div class="highlight-metric tone-read">{1}</div>
                <div class="highlight-secondary">{2} assessment on current mean latency</div>
            </div>
            <div class="highlight-stat">
                <div class="highlight-stat-label tone-write">Lowest Write Mean Latency</div>
                <div class="highlight-stat-context">{3}</div>
                <div class="highlight-metric tone-write">{4}</div>
                <div class="highlight-secondary">{5} assessment on current mean latency</div>
            </div>
        </div>
    </article>
'@ -f $lowestReadLatencyContextHtml, $lowestReadLatencyValueHtml, $lowestReadLatencyAssessmentHtml, $lowestWriteLatencyContextHtml, $lowestWriteLatencyValueHtml, $lowestWriteLatencyAssessmentHtml
        )

        $latestByProfileHtml = foreach ($run in @($profileLatestRuns | Sort-Object Profile)) {
                $diagState = Get-FioDiagnosticsRenderState -Diagnostics $run.Diagnostics
                $focus = GetProfileFocusMetadata -Profile ([string]$run.Profile)
            $assessment = Get-FioHtmlOverallAssessmentModel -Run $run
            $sqlFitBlurb = Get-FioHtmlSqlFitBlurb -Run $run
                $readMetricModel = GetOperationFocusMetricModel -Run $run -Operation 'Read' -Focus $focus
                $writeMetricModel = GetOperationFocusMetricModel -Run $run -Operation 'Write' -Focus $focus
            $channelCount = if ($run.SmbMetadata -and $null -ne $run.SmbMetadata.MultichannelPathCount) { [int]$run.SmbMetadata.MultichannelPathCount } else { $null }
            $rdmaCount = if ($run.SmbMetadata -and $null -ne $run.SmbMetadata.RdmaPathCount) { [int]$run.SmbMetadata.RdmaPathCount } else { $null }
            $stabilityBadgeModels = @(Get-FioDiagnosticsBandwidthStabilityModels -Diagnostics $run.Diagnostics)
            $profileDetailSettings = @(
                [pscustomobject]@{ Key = 'BlockSize'; Label = 'bs'; Value = [string]$run.BlockSize }
                [pscustomobject]@{ Key = 'QueueDepth'; Label = 'qd'; Value = if ($null -ne $run.QueueDepth) { [string]$run.QueueDepth } else { $null } }
                [pscustomobject]@{ Key = 'NumJobs'; Label = 'jobs'; Value = if ($null -ne $run.NumJobs) { [string]$run.NumJobs } else { $null } }
                [pscustomobject]@{ Key = 'FileSizeGB'; Label = 'size'; Value = if ($null -ne $run.FileSizeGB) { '{0} GB' -f ([math]::Round([double]$run.FileSizeGB, 0)) } else { $null } }
                [pscustomobject]@{ Key = 'Iterations'; Label = 'iters'; Value = if ($null -ne $run.IterationCount) { [string]$run.IterationCount } else { $null } }
                [pscustomobject]@{ Key = 'AggregateBw'; Label = 'agg bw'; Value = if ($run.PSObject.Properties['AggregateBandwidthMBps'] -and $null -ne $run.AggregateBandwidthMBps -and [double]$run.AggregateBandwidthMBps -gt 0) { '{0:N2} MB/s' -f [double]$run.AggregateBandwidthMBps } else { $null } }
                [pscustomobject]@{ Key = 'FioVersion'; Label = 'fio'; Value = [string]$run.FioVersion }
                [pscustomobject]@{ Key = 'Server'; Label = 'server'; Value = if ($run.SmbMetadata) { [string]$run.SmbMetadata.ServerName } else { $null } }
                [pscustomobject]@{ Key = 'Share'; Label = 'share'; Value = if ($run.SmbMetadata) { [string]$run.SmbMetadata.ShareName } else { $null } }
                [pscustomobject]@{ Key = 'Channels'; Label = 'channels'; Value = if ($null -ne $channelCount -and $channelCount -gt 0) { [string]$channelCount } else { $null } }
                [pscustomobject]@{ Key = 'Rdma'; Label = 'rdma'; Value = if ($null -ne $rdmaCount -and $rdmaCount -gt 0) { [string]$rdmaCount } else { $null } }
                [pscustomobject]@{ Key = 'TargetType'; Label = 'type'; Value = if (-not $run.SmbMetadata) { [string]$run.TargetType } else { $null } }
            )
            $profileDetailBadges = ConvertTo-FioHtmlSettingBadges -Settings $profileDetailSettings
            $stabilityBadgeHtml = @(
                foreach ($stabilityBadge in $stabilityBadgeModels) {
                    "<span class='stability-pill {0}'>{1} {2}</span>" -f $stabilityBadge.Class, [System.Net.WebUtility]::HtmlEncode([string]$stabilityBadge.DisplayLabel), [System.Net.WebUtility]::HtmlEncode([string]$stabilityBadge.DisplayValue)
                }
            )
                $statusClass = switch ($diagState.Status) {
                        'Available' { 'diag-good' }
                        'Partial' { 'diag-warn' }
                        'MissingLogs' { 'diag-bad' }
                        'Unreadable' { 'diag-bad' }
                        'MissingSummary' { 'diag-bad' }
                        'Unavailable' { 'diag-bad' }
                        default { 'diag-neutral' }
                }
                $readP99Number = if ($null -ne $run.Read.P99LatencyUs) { '{0:N2}' -f ($run.Read.P99LatencyUs / 1000.0) } else { '-' }
                $writeP99Number = if ($null -ne $run.Write.P99LatencyUs) { '{0:N2}' -f ($run.Write.P99LatencyUs / 1000.0) } else { '-' }
                $readP99Unit = if ($null -ne $run.Read.P99LatencyUs) { 'ms' } else { $null }
                $writeP99Unit = if ($null -ne $run.Write.P99LatencyUs) { 'ms' } else { $null }
                $readLatencyClassSuffix = if ($null -eq $readP99Unit) { ' profile-latency-stat-empty' } else { '' }
                $writeLatencyClassSuffix = if ($null -eq $writeP99Unit) { ' profile-latency-stat-empty' } else { '' }
                $readP99UnitHtml = if ($null -ne $readP99Unit) { '<span class=''profile-latency-unit''>{0}</span>' -f [System.Net.WebUtility]::HtmlEncode($readP99Unit) } else { '' }
                $writeP99UnitHtml = if ($null -ne $writeP99Unit) { '<span class=''profile-latency-unit''>{0}</span>' -f [System.Net.WebUtility]::HtmlEncode($writeP99Unit) } else { '' }
                $profileNameHtml = [System.Net.WebUtility]::HtmlEncode([string]$run.Profile)
                $profileStampHtml = [System.Net.WebUtility]::HtmlEncode((Format-FioHtmlTimestamp -TimestampUtc $run.TimestampUtc))
                $diagnosticAnchorHtml = [System.Net.WebUtility]::HtmlEncode(('diagnostics-{0}' -f ([string]$run.Profile).ToLowerInvariant()))
                $focusLabelHtml = [System.Net.WebUtility]::HtmlEncode([string]$focus.Label)
                $assessmentClass = [string]$assessment.Class
                $assessmentLabelHtml = [System.Net.WebUtility]::HtmlEncode([string]$assessment.Label)
                $targetTextHtml = [System.Net.WebUtility]::HtmlEncode(('{0} · {1}' -f $run.TargetType, $run.TargetPath))
                $readLabelHtml = [System.Net.WebUtility]::HtmlEncode([string]$readMetricModel.Label)
                $readPrimaryHtml = [System.Net.WebUtility]::HtmlEncode([string]$readMetricModel.Primary)
                $readSecondaryHtml = [System.Net.WebUtility]::HtmlEncode([string]$readMetricModel.Secondary)
                $writeLabelHtml = [System.Net.WebUtility]::HtmlEncode([string]$writeMetricModel.Label)
                $writePrimaryHtml = [System.Net.WebUtility]::HtmlEncode([string]$writeMetricModel.Primary)
                $writeSecondaryHtml = [System.Net.WebUtility]::HtmlEncode([string]$writeMetricModel.Secondary)
                $readP99NumberHtml = [System.Net.WebUtility]::HtmlEncode([string]$readP99Number)
                $writeP99NumberHtml = [System.Net.WebUtility]::HtmlEncode([string]$writeP99Number)
                $diagnosticsDetailHtml = [System.Net.WebUtility]::HtmlEncode([string]$diagState.Detail)
                $sqlFitBlurbHtml = [System.Net.WebUtility]::HtmlEncode([string]$sqlFitBlurb)
                @'
<article class="profile-card">
        <div class="profile-card-head">
                <div>
                        <h3>{0}</h3>
                        <div class="profile-stamp">{1}</div>
                        <a class="profile-jump-link" href="#{2}">Jump to diagnostics</a>
                </div>
        <div class="profile-card-badges">
            <span class="focus-pill">{3}</span>
            <span class="assessment-pill {4}">{5}</span>
            {22}
        </div>
        </div>
        <div class="profile-target">{6}</div>
        <div class="profile-detail-badges">
            {7}
        </div>
        <div class="profile-metric-grid">
            <div class="profile-metric profile-metric-primary">
            <span class="profile-metric-label">{8}</span>
            <span class="profile-metric-value">{9}</span>
            <span class="profile-metric-note">{10}</span>
                </div>
            <div class="profile-metric profile-metric-primary">
            <span class="profile-metric-label">{11}</span>
            <span class="profile-metric-value">{12}</span>
            <span class="profile-metric-note">{13}</span>
                </div>
            <div class="profile-metric profile-metric-latency">
                        <span class="profile-metric-label">Latency envelope</span>
                    <div class="profile-latency-grid">
                        <div class="profile-latency-stat{14}">
                            <span class="profile-latency-key">Read</span>
                            <span class="profile-latency-number">{15}</span>
                            {16}
                        </div>
                        <div class="profile-latency-stat{17}">
                            <span class="profile-latency-key">Write</span>
                            <span class="profile-latency-number">{18}</span>
                            {19}
                        </div>
                    </div>
                    <span class="profile-metric-note">Diagnostics coverage: {20}</span>
                </div>
        </div>
        <p class="profile-analysis">{21}</p>
</article>
'@ -f $profileNameHtml, $profileStampHtml, $diagnosticAnchorHtml, $focusLabelHtml, $assessmentClass, $assessmentLabelHtml, $targetTextHtml, $profileDetailBadges, $readLabelHtml, $readPrimaryHtml, $readSecondaryHtml, $writeLabelHtml, $writePrimaryHtml, $writeSecondaryHtml, $readLatencyClassSuffix, $readP99NumberHtml, $readP99UnitHtml, $writeLatencyClassSuffix, $writeP99NumberHtml, $writeP99UnitHtml, $diagnosticsDetailHtml, $sqlFitBlurbHtml, ($stabilityBadgeHtml -join '')
        }

        $coverageRows = @(
                foreach ($selection in @($profileDiagnosticsSelections | Sort-Object Profile)) {
                        [pscustomobject]@{
                                Profile = [string]$selection.Profile
                                SelectedRunId = [string]$selection.Run.RunId
                                Timestamp = Format-FioHtmlTimestamp -TimestampUtc $selection.Run.TimestampUtc
                                Diagnostics = $selection.State.Label
                                Detail = $selection.State.Detail
                                AvailableRuns = $selection.AvailableRuns
                                RequestedRuns = $selection.RequestedRuns
                                TotalRuns = $selection.TotalRuns
                        }
                }
        )
        $coverageTableHtml = NewHtmlTableMarkup -Rows $coverageRows

        $diagnosticsSectionsHtml = foreach ($selection in @($profileDiagnosticsSelections | Sort-Object Profile)) {
                $selectedRun = $selection.Run
                $selectedRunId = [string]$selectedRun.RunId
            $selectedRunAssessment = Get-FioHtmlOverallAssessmentModel -Run $selectedRun
            $selectedRunAnalysis = Get-FioHtmlSqlFitBlurb -Run $selectedRun
            $summaryMessage = if ($selectedRun.Diagnostics) { [string]$selectedRun.Diagnostics.Message } else { 'Diagnostics were not captured for this profile selection.' }
            $summaryStatusClass = switch ($selection.State.Status) {
                'Available' { 'diag-good' }
                'Partial' { 'diag-warn' }
                'MissingLogs' { 'diag-bad' }
                'Unreadable' { 'diag-bad' }
                'MissingSummary' { 'diag-bad' }
                'Unavailable' { 'diag-bad' }
                default { 'diag-neutral' }
            }
                $selectedRunIdHtml = [System.Net.WebUtility]::HtmlEncode($selectedRunId)
                $diagnosticsLabelHtml = [System.Net.WebUtility]::HtmlEncode($selection.State.Label)
                $assessmentClass = [string]$selectedRunAssessment.Class
                $assessmentLabelHtml = [System.Net.WebUtility]::HtmlEncode([string]$selectedRunAssessment.Label)
                $summaryTimeHtml = [System.Net.WebUtility]::HtmlEncode((Format-FioHtmlTimestamp -TimestampUtc $selectedRun.TimestampUtc))
                $summaryTargetTypeHtml = [System.Net.WebUtility]::HtmlEncode([string]$selectedRun.TargetType)
                $summarySeriesCount = if ($selectedRun.Diagnostics) { @($selectedRun.Diagnostics.Series).Count } else { 0 }
                $summaryDuration = if ($selectedRun.Diagnostics -and $null -ne $selectedRun.Diagnostics.TimeRangeMs) { '{0:N1}s' -f ($selectedRun.Diagnostics.TimeRangeMs / 1000.0) } else { '-' }
                $summarySeriesCountHtml = [System.Net.WebUtility]::HtmlEncode([string]$summarySeriesCount)
                $summaryDurationHtml = [System.Net.WebUtility]::HtmlEncode([string]$summaryDuration)
                $summaryCoverageHtml = [System.Net.WebUtility]::HtmlEncode([string]$selection.State.Detail)
                $summaryTargetPathHtml = [System.Net.WebUtility]::HtmlEncode([string]$selectedRun.TargetPath)
                $summaryMessageHtml = [System.Net.WebUtility]::HtmlEncode($summaryMessage)
                $summaryAnalysisHtml = [System.Net.WebUtility]::HtmlEncode($selectedRunAnalysis)
                $summaryMetaHtml = @'
    <div class="diagnostic-summary">
        <div class="diagnostic-summary-head">
            <div class="diagnostic-summary-run">
                <div class="metric-label">Selected Run</div>
                <div class="diagnostic-summary-runid">{0}</div>
            </div>
            <div class="diagnostic-summary-badges">
                <span class="diag-pill {1}">{2}</span>
                <span class="assessment-pill {3}">{4}</span>
            </div>
        </div>
        <div class="diagnostic-summary-meta">
            <span class="meta-chip"><span class="meta-chip-label">Time</span><span class="meta-chip-value">{5}</span></span>
            <span class="meta-chip"><span class="meta-chip-label">Target</span><span class="meta-chip-value">{6}</span></span>
            <span class="meta-chip"><span class="meta-chip-label">Series</span><span class="meta-chip-value">{7}</span></span>
            <span class="meta-chip"><span class="meta-chip-label">Duration</span><span class="meta-chip-value">{8}</span></span>
            <span class="meta-chip"><span class="meta-chip-label">Coverage</span><span class="meta-chip-value">{9}</span></span>
        </div>
        <div class="diagnostic-summary-target">{10}</div>
        <div class="diagnostic-summary-copy">
            <p class="diagnostic-summary-message">{11}</p>
            <p class="diagnostic-summary-analysis">{12}</p>
        </div>
    </div>
'@ -f $selectedRunIdHtml, $summaryStatusClass, $diagnosticsLabelHtml, $assessmentClass, $assessmentLabelHtml, $summaryTimeHtml, $summaryTargetTypeHtml, $summarySeriesCountHtml, $summaryDurationHtml, $summaryCoverageHtml, $summaryTargetPathHtml, $summaryMessageHtml, $summaryAnalysisHtml

                $chartModels = @(Get-FioDiagnosticsChartModels -Diagnostics $selectedRun.Diagnostics)
                $chartStacks = foreach ($chartModel in $chartModels) {
                        $chartSeries = @(GetDiagnosticsSeriesByKey -Diagnostics $selectedRun.Diagnostics -Key $chartModel.Key)
                        if ($chartSeries.Count -eq 0) {
                                continue
                        }
                        $chartMarkup = New-FioHtmlLineChartSection -Title $chartModel.Title -Series $chartSeries -Subtitle ('{0} · {1}' -f $chartModel.Subtitle, $selectedRunId)
                        $tableMarkup = NewCollapsibleTableMarkup -Title ('Show raw data for {0}' -f $chartModel.Title) -Rows @($chartModel.PointRows)
                    @'
        <div class="diagnostic-card-stack">
            {0}
            {1}
        </div>
'@ -f $chartMarkup, $tableMarkup
                }

                if ($chartStacks.Count -eq 0) {
                        $chartStacks = @("<div class='empty-state'>No chartable diagnostics were captured for this selection.</div>")
                }

                $diagnosticSectionId = [System.Net.WebUtility]::HtmlEncode([string]$selection.Profile.ToLowerInvariant())
                $diagnosticProfileTitleHtml = [System.Net.WebUtility]::HtmlEncode([string]$selection.Profile)
                $chartStacksHtml = $chartStacks -join [Environment]::NewLine

                @'
        <section id="diagnostics-{0}" class="section-card">
            <div class="section-heading">
                <div>
                    <h2>{1} Diagnostics</h2>
                    <p>Latest diagnostics-ready run selected for this profile.</p>
                </div>
            </div>
            {2}
            <div class="diagnostic-grid">
                {3}
            </div>
        </section>
'@ -f $diagnosticSectionId, $diagnosticProfileTitleHtml, $summaryMetaHtml, $chartStacksHtml
        }

        $profileHistorySectionsHtml = foreach ($profileGroup in $profileGroups) {
                $profileRows = @(
                        foreach ($run in @($profileGroup.Group | Sort-Object TimestampUtc -Descending)) {
                                $diagState = Get-FioDiagnosticsRenderState -Diagnostics $run.Diagnostics
                                [pscustomobject]@{
                                        Timestamp = Format-FioHtmlTimestamp -TimestampUtc $run.TimestampUtc
                                        RunId = [string]$run.RunId
                                        TargetType = [string]$run.TargetType
                                        TargetPath = [string]$run.TargetPath
                                        Diagnostics = $diagState.Label
                                        ReadMBps = [math]::Round([double]$run.Read.BandwidthMBps, 2)
                                        ReadIops = [math]::Round([double]$run.Read.Iops, 2)
                                        WriteMBps = [math]::Round([double]$run.Write.BandwidthMBps, 2)
                                        WriteIops = [math]::Round([double]$run.Write.Iops, 2)
                                        ReadP99Ms = if ($null -ne $run.Read.P99LatencyUs) { [math]::Round(($run.Read.P99LatencyUs / 1000.0), 2) } else { $null }
                                        WriteP99Ms = if ($null -ne $run.Write.P99LatencyUs) { [math]::Round(($run.Write.P99LatencyUs / 1000.0), 2) } else { $null }
                                }
                        }
                )
                $profileTableHtml = NewHtmlTableMarkup -Rows $profileRows
                $profileGroupNameHtml = [System.Net.WebUtility]::HtmlEncode([string]$profileGroup.Name)
                $profileRunCountHtml = [System.Net.WebUtility]::HtmlEncode(('{0} runs' -f $profileRows.Count))
                @'
        <section class="section-card">
            <div class="section-heading">
                <div>
                    <h2>{0}</h2>
                    <p>Run history for this profile.</p>
                </div>
                <div class="pill">{1}</div>
            </div>
            {2}
        </section>
'@ -f $profileGroupNameHtml, $profileRunCountHtml, $profileTableHtml
        }

        $rollupTableData = @(
                foreach ($rollup in $rollupRows | Sort-Object Profile, TargetType, TargetPath) {
                        [pscustomobject]@{
                                Profile = [string]$rollup.Profile
                                TargetType = [string]$rollup.TargetType
                                TargetPath = [string]$rollup.TargetPath
                                Runs = [string]$rollup.RunCount
                                DiagnosticsRequested = [string]$rollup.DiagnosticsRequestedRuns
                                DiagnosticsAvailable = [string]$rollup.DiagnosticsAvailableRuns
                                ReadIopsAvg = if ($rollup.ReadIops) { [math]::Round([double]$rollup.ReadIops.Avg, 2) } else { $null }
                                ReadMBpsAvg = if ($rollup.ReadBandwidthMBps) { [math]::Round([double]$rollup.ReadBandwidthMBps.Avg, 2) } else { $null }
                                WriteIopsAvg = if ($rollup.WriteIops) { [math]::Round([double]$rollup.WriteIops.Avg, 2) } else { $null }
                                WriteMBpsAvg = if ($rollup.WriteBandwidthMBps) { [math]::Round([double]$rollup.WriteBandwidthMBps.Avg, 2) } else { $null }
                                LastTimestamp = Format-FioHtmlTimestamp -TimestampUtc $rollup.LastTimestampUtc
                        }
                }
        )
        $rollupTableHtml = NewHtmlTableMarkup -Rows $rollupTableData

        $runTableData = @(
                foreach ($run in $orderedRuns | Sort-Object TimestampUtc -Descending) {
                        $diagState = Get-FioDiagnosticsRenderState -Diagnostics $run.Diagnostics
                        [pscustomobject]@{
                                Timestamp = Format-FioHtmlTimestamp -TimestampUtc $run.TimestampUtc
                                RunId = [string]$run.RunId
                                Profile = [string]$run.Profile
                                TargetType = [string]$run.TargetType
                                TargetPath = [string]$run.TargetPath
                                Iterations = [string]$run.IterationCount
                                Diagnostics = $diagState.Label
                                ReadIops = [math]::Round([double]$run.Read.Iops, 2)
                                ReadMBps = [math]::Round([double]$run.Read.BandwidthMBps, 2)
                                WriteIops = [math]::Round([double]$run.Write.Iops, 2)
                                WriteMBps = [math]::Round([double]$run.Write.BandwidthMBps, 2)
                        }
                }
        )
        $runTableHtml = NewHtmlTableMarkup -Rows $runTableData

        $resultsRootText = if ([string]::IsNullOrWhiteSpace($ResultsRoot)) { '-' } else { $ResultsRoot }

        $html = @"
<!DOCTYPE html>
<html lang='en'>
<head>
    <meta charset='utf-8' />
    <meta name='viewport' content='width=device-width, initial-scale=1' />
    <title>$([System.Net.WebUtility]::HtmlEncode($Title))</title>
    <style>
        :root {
            --bg: #eef3f8;
            --panel: rgba(255,255,255,0.94);
            --panel-soft: rgba(248,251,254,0.96);
            --ink: #162133;
            --muted: #5f7085;
            --line: rgba(148,163,184,0.22);
            --accent: #0f766e;
            --accent-2: #b45309;
            --accent-3: #2563eb;
            --shadow: rgba(15,23,42,0.10);
        }
        * { box-sizing: border-box; }
        body {
            margin: 0;
            color: var(--ink);
            font-family: Aptos, "Segoe UI Variable Text", "Segoe UI", sans-serif;
            background:
                radial-gradient(circle at top right, rgba(20,184,166,0.15), transparent 22%),
                radial-gradient(circle at top left, rgba(245,158,11,0.10), transparent 18%),
                linear-gradient(180deg, #f6f2ea 0%, var(--bg) 36%, #edf2f7 100%);
        }
        main {
            max-width: 1480px;
            margin: 0 auto;
            padding: 26px 18px 48px;
        }
        h1, h2, h3, p { margin: 0; }
        .hero, .section-card, .metric-card, .profile-card, .highlight-card, .chart-card, .collapsible-table {
            border: 1px solid var(--line);
            border-radius: 24px;
            background: var(--panel);
            box-shadow: 0 18px 45px var(--shadow);
            backdrop-filter: blur(12px);
        }
        .hero {
            padding: 26px;
            margin-bottom: 18px;
        }
        .hero-title {
            font-size: 2.2rem;
            line-height: 1.08;
            letter-spacing: -0.04em;
            margin-bottom: 10px;
        }
        .hero-subtitle {
            color: var(--muted);
            font-size: 0.92rem;
            line-height: 1.45;
            max-width: 72ch;
        }
        .hero-meta {
            display: flex;
            flex-wrap: wrap;
            gap: 10px 18px;
            margin-top: 12px;
            color: var(--muted);
            font-size: 0.8rem;
        }
        .metric-grid, .latest-grid, .highlight-grid, .diagnostic-grid {
            display: grid;
            gap: 16px;
        }
        .metric-grid {
            grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
            margin-top: 18px;
        }
        .metric-card, .highlight-card {
            padding: 18px;
        }
        .metric-label, .profile-metric-label {
            display: block;
            color: var(--muted);
            font-size: 0.76rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            margin-bottom: 6px;
        }
        .metric-value {
            font-size: 1.46rem;
            font-weight: 800;
        }
        .highlight-grid {
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            margin: 18px 0 22px;
        }
        .highlight-card {
            background: linear-gradient(180deg, rgba(15,23,42,0.98), rgba(30,41,59,0.96));
            color: #f8fafc;
            min-height: 168px;
            padding: 18px 20px;
        }
        .highlight-card .metric-label {
            color: #93a4b8;
            margin-bottom: 12px;
        }
        .highlight-stat-grid {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 18px;
            min-height: 100px;
        }
        .highlight-stat {
            min-width: 0;
        }
        .highlight-stat-single {
            display: grid;
            align-content: end;
            min-height: 100px;
        }
        .highlight-stat-label {
            font-size: 0.72rem;
            letter-spacing: 0.07em;
            text-transform: uppercase;
            font-weight: 800;
            margin-bottom: 8px;
        }
        .highlight-stat-context {
            color: #dbe5f3;
            font-size: 0.98rem;
            font-weight: 750;
            margin-bottom: 8px;
            line-height: 1.1;
        }
        .highlight-card .tone-read {
            color: #67e8f9;
        }
        .highlight-card .tone-write {
            color: #fbbf24;
        }
        .tone-read {
            color: #67e8f9;
        }
        .tone-write {
            color: #fbbf24;
        }
        .highlight-metric {
            font-size: 1.98rem;
            font-weight: 800;
            line-height: 0.98;
            letter-spacing: -0.03em;
            margin-bottom: 8px;
        }
        .highlight-secondary {
            color: #d5deea;
            font-size: 0.88rem;
            line-height: 1.25;
            margin-top: 10px;
        }
        .section-card {
            padding: 18px;
            margin-bottom: 18px;
        }
        .section-card.section-toggle {
            padding: 0;
            overflow: hidden;
        }
        .section-heading {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 12px;
            margin-bottom: 14px;
        }
        .section-heading p {
            color: var(--muted);
            font-size: 0.86rem;
            line-height: 1.38;
            margin-top: 4px;
        }
        .section-heading h2 {
            font-size: 1.06rem;
            line-height: 1.2;
        }
        .pill {
            border: 1px solid var(--line);
            border-radius: 999px;
            padding: 6px 10px;
            color: var(--muted);
            font-size: 0.72rem;
            white-space: nowrap;
            background: rgba(255,255,255,0.7);
        }
        .section-toggle > summary {
            list-style: none;
            cursor: pointer;
            padding: 18px;
            background: linear-gradient(180deg, rgba(248,250,252,0.92), rgba(241,245,249,0.90));
            border-bottom: 1px solid rgba(148,163,184,0.14);
        }
        .section-toggle > summary::-webkit-details-marker {
            display: none;
        }
        .section-toggle-head {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 12px;
            padding-right: 28px;
            position: relative;
        }
        .section-toggle-head::after {
            content: '+';
            position: absolute;
            right: 0;
            top: 2px;
            color: var(--accent);
            font-size: 1.25rem;
            font-weight: 800;
            line-height: 1;
        }
        .section-toggle[open] .section-toggle-head::after {
            content: '\2212';
        }
        .section-toggle > summary p {
            color: var(--muted);
            line-height: 1.45;
            margin-top: 4px;
        }
        .section-toggle-body {
            padding: 18px;
            background: rgba(255,255,255,0.74);
        }
        .latest-grid {
            grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
        }
        .profile-card {
            padding: 18px;
            background: linear-gradient(180deg, rgba(255,255,255,0.98), rgba(238,244,251,0.98));
            box-shadow: 0 20px 48px rgba(15,23,42,0.08);
            border-color: rgba(148,163,184,0.2);
        }
        .profile-card-head {
            display: flex;
            justify-content: space-between;
            gap: 12px;
            align-items: flex-start;
            margin-bottom: 12px;
        }
        .profile-card-badges {
            display: flex;
            flex-wrap: wrap;
            justify-content: flex-end;
            gap: 8px;
        }
        .profile-card h3 {
            font-size: 1.08rem;
            line-height: 1.15;
        }
        .profile-stamp {
            margin-top: 5px;
            color: var(--muted);
            font-size: 0.8rem;
        }
        .profile-jump-link {
            display: inline-flex;
            align-items: center;
            margin-top: 8px;
            color: #2563eb;
            font-size: 0.74rem;
            font-weight: 700;
            text-decoration: none;
            line-height: 1.2;
        }
        .profile-jump-link:hover {
            color: #1d4ed8;
            text-decoration: underline;
        }
        .profile-target {
            color: #334155;
            font-size: 0.84rem;
            line-height: 1.35;
            margin-bottom: 10px;
            word-break: break-word;
            padding-bottom: 8px;
            border-bottom: 1px solid rgba(148,163,184,0.14);
        }
        .profile-detail-badges {
            display: flex;
            flex-wrap: wrap;
            gap: 7px;
            margin: 0 0 12px;
        }
        .profile-metric-grid {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 12px;
        }
        .profile-metric {
            border: 1px solid rgba(148,163,184,0.18);
            border-radius: 18px;
            background: linear-gradient(180deg, rgba(255,255,255,1), rgba(240,245,251,0.98));
            padding: 12px 14px;
            min-height: 108px;
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.72);
        }
        .profile-metric-primary {
            background: linear-gradient(180deg, rgba(255,255,255,1), rgba(235,242,250,0.98));
            border-color: rgba(96,165,250,0.2);
        }
        .profile-metric-latency {
            background: linear-gradient(180deg, rgba(252,253,255,0.98), rgba(236,242,251,0.98));
            border-color: rgba(148,163,184,0.24);
        }
        .profile-metric-value {
            display: block;
            font-weight: 800;
            font-size: 1.28rem;
            line-height: 1.12;
            letter-spacing: -0.02em;
        }
        .profile-metric-note {
            display: block;
            margin-top: 8px;
            color: var(--muted);
            font-size: 0.76rem;
            line-height: 1.25;
        }
        .profile-latency-grid {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 8px;
            margin-top: 2px;
        }
        .profile-latency-stat {
            min-width: 0;
        }
        .profile-latency-key {
            display: block;
            color: var(--muted);
            font-size: 0.68rem;
            font-weight: 800;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            margin-bottom: 4px;
        }
        .profile-latency-number {
            display: block;
            font-size: clamp(0.84rem, 0.9vw, 0.98rem);
            font-weight: 800;
            line-height: 1.04;
            letter-spacing: -0.03em;
            font-variant-numeric: tabular-nums;
            white-space: nowrap;
        }
        .profile-latency-unit {
            display: block;
            margin-top: 2px;
            color: var(--muted);
            font-size: 0.62rem;
            font-weight: 700;
            letter-spacing: 0.04em;
            text-transform: uppercase;
        }
        .profile-latency-stat-empty .profile-latency-number {
            letter-spacing: 0;
        }
        .profile-detail-badges .setting-badge {
            padding: 4px 8px;
            font-size: 0.7rem;
            background: rgba(255,255,255,0.88);
        }
        .profile-detail-badges .setting-key {
            font-size: 0.62rem;
        }
        .profile-detail-badges .setting-value {
            font-size: 0.72rem;
        }
        .profile-analysis,
        .diagnostic-summary-analysis {
            margin-top: 10px;
            color: #475569;
            font-size: 0.8rem;
            line-height: 1.4;
            max-width: 90ch;
        }
        .focus-pill {
            display: inline-flex;
            align-items: center;
            padding: 6px 10px;
            border-radius: 999px;
            border: 1px solid rgba(37,99,235,0.14);
            background: rgba(37,99,235,0.08);
            color: #1d4ed8;
            font-size: 0.72rem;
            font-weight: 800;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            white-space: nowrap;
        }
        .assessment-pill {
            display: inline-flex;
            align-items: center;
            padding: 6px 10px;
            border-radius: 999px;
            font-size: 0.76rem;
            font-weight: 800;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            white-space: nowrap;
        }
        .assessment-excellent { background: rgba(16,185,129,0.16); color: #047857; }
        .assessment-good { background: rgba(59,130,246,0.14); color: #1d4ed8; }
        .assessment-poor { background: rgba(239,68,68,0.14); color: #b91c1c; }
        .assessment-neutral { background: rgba(148,163,184,0.18); color: #475569; }
        .stability-pill {
            display: inline-flex;
            align-items: center;
            padding: 6px 10px;
            border-radius: 999px;
            font-size: 0.72rem;
            font-weight: 800;
            letter-spacing: 0.04em;
            white-space: nowrap;
        }
        .stability-good { background: rgba(16,185,129,0.16); color: #047857; }
        .stability-warn { background: rgba(245,158,11,0.18); color: #b45309; }
        .stability-bad { background: rgba(239,68,68,0.14); color: #b91c1c; }
        .stability-neutral { background: rgba(148,163,184,0.18); color: #475569; }
        .diag-pill {
            display: inline-flex;
            align-items: center;
            padding: 6px 10px;
            border-radius: 999px;
            font-size: 0.76rem;
            font-weight: 800;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            white-space: nowrap;
        }
        .diag-good { background: rgba(16,185,129,0.16); color: #047857; }
        .diag-warn { background: rgba(245,158,11,0.18); color: #b45309; }
        .diag-bad { background: rgba(239,68,68,0.14); color: #b91c1c; }
        .diag-neutral { background: rgba(148,163,184,0.18); color: #475569; }
        .diagnostic-grid {
            grid-template-columns: repeat(auto-fit, minmax(480px, 1fr));
            align-items: start;
            margin-top: 16px;
        }
        .diagnostic-card-stack {
            display: grid;
            gap: 12px;
            min-width: 0;
        }
        .chart-card {
            padding: 14px;
            background: var(--panel-soft);
        }
        .chart-card h2 {
            font-size: 1.02rem;
        }
        .chart-card .section-heading p {
            font-size: 0.81rem;
            line-height: 1.34;
        }
        .diagnostic-summary {
            border: 1px solid rgba(148,163,184,0.14);
            border-radius: 20px;
            padding: 16px 18px;
            background: linear-gradient(180deg, rgba(248,250,252,0.84), rgba(255,255,255,0.9));
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.8);
        }
        .diagnostic-summary-head {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 14px;
            padding-bottom: 12px;
            border-bottom: 1px solid rgba(148,163,184,0.14);
        }
        .diagnostic-summary-run {
            min-width: 0;
        }
        .diagnostic-summary-runid {
            font-size: 1rem;
            font-weight: 800;
            line-height: 1.15;
            word-break: break-word;
        }
        .diagnostic-summary-badges {
            display: flex;
            flex-wrap: wrap;
            justify-content: flex-end;
            gap: 8px;
        }
        .diagnostic-summary-meta {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 8px;
            margin-top: 12px;
        }
        .meta-chip,
        .setting-badge {
            display: inline-flex;
            align-items: center;
            justify-content: space-between;
            gap: 10px;
            padding: 5px 9px;
            border-radius: 999px;
            border: 1px solid rgba(148,163,184,0.18);
            background: rgba(255,255,255,0.78);
            color: #334155;
            font-size: 0.74rem;
            line-height: 1.25;
        }
        .meta-chip-label {
            color: var(--muted);
            font-weight: 800;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            font-size: 0.64rem;
        }
        .meta-chip-value {
            font-weight: 700;
            text-align: right;
            font-variant-numeric: tabular-nums;
        }
        .setting-key {
            color: var(--muted);
            font-weight: 800;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            font-size: 0.66rem;
        }
        .setting-value {
            font-weight: 700;
        }
        .diagnostic-summary-target {
            margin-top: 12px;
            color: #0f172a;
            font-size: 0.82rem;
            font-family: Consolas, "Cascadia Mono", monospace;
            line-height: 1.35;
            word-break: break-word;
        }
        .diagnostic-summary-copy {
            margin-top: 12px;
            padding-top: 12px;
            border-top: 1px solid rgba(148,163,184,0.12);
        }
        .diagnostic-summary-message {
            margin-top: 0;
            color: var(--muted);
            font-size: 0.8rem;
            line-height: 1.35;
            max-width: 92ch;
        }
        .diagnostic-summary-copy .diagnostic-summary-analysis {
            margin-top: 8px;
        }
        .empty-state {
            padding: 18px;
            border: 1px dashed rgba(148,163,184,0.36);
            border-radius: 18px;
            color: var(--muted);
            background: rgba(255,255,255,0.76);
        }
        .table-shell {
            overflow: auto;
            border: 1px solid rgba(148,163,184,0.14);
            border-radius: 18px;
            background: rgba(255,255,255,0.78);
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.56);
        }
        table.report-table {
            width: 100%;
            border-collapse: collapse;
            min-width: 920px;
        }
        table.report-table th,
        table.report-table td {
            padding: 11px 12px;
            text-align: left;
            border-bottom: 1px solid rgba(148,163,184,0.14);
            vertical-align: top;
        }
        table.report-table th {
            position: sticky;
            top: 0;
            z-index: 1;
            background: rgba(248,250,252,0.98);
            color: #334155;
            font-size: 0.76rem;
            font-weight: 800;
            letter-spacing: 0.05em;
            text-transform: uppercase;
        }
        table.report-table tbody tr:nth-child(even) {
            background: rgba(248,250,252,0.55);
        }
        table.report-table tbody tr:hover {
            background: rgba(20,184,166,0.08);
        }
        table.report-table-compact {
            min-width: 760px;
        }
        .collapsible-table {
            padding: 14px;
            background: rgba(255,255,255,0.88);
        }
        .collapsible-table summary {
            cursor: pointer;
            font-weight: 700;
            color: #1f2937;
            font-size: 0.88rem;
            list-style: none;
        }
        .collapsible-table summary::-webkit-details-marker {
            display: none;
        }
        .collapsible-table summary::before {
            content: '+';
            display: inline-block;
            width: 18px;
            color: var(--accent);
            font-weight: 800;
        }
        .collapsible-table[open] summary::before {
            content: '\2212';
        }
        .collapsible-table .table-shell {
            margin-top: 12px;
        }
        .diagnostic-legend {
            display: flex;
            flex-wrap: wrap;
            gap: 10px 14px;
            margin-bottom: 12px;
        }
        .diagnostic-legend-item {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            color: var(--muted);
            font-size: 0.78rem;
            font-weight: 600;
        }
        .diagnostic-legend-swatch {
            width: 12px;
            height: 12px;
            border-radius: 999px;
            flex: 0 0 auto;
        }
        .diagnostic-chart-wrap {
            display: grid;
            grid-template-columns: 58px 1fr;
            gap: 10px;
            align-items: stretch;
        }
        .diagnostic-chart {
            width: 100%;
            height: auto;
            display: block;
        }
        .diagnostic-axis {
            display: flex;
            color: var(--muted);
            font-size: 0.72rem;
            font-weight: 600;
        }
        .diagnostic-axis-y {
            flex-direction: column;
            justify-content: space-between;
            padding: 10px 0 30px;
            text-align: right;
        }
        .diagnostic-axis-x {
            justify-content: space-between;
            padding-left: 68px;
            margin-top: 6px;
        }
        .diagnostic-plot-bg {
            fill: rgba(248,250,252,0.78);
        }
        .diagnostic-grid-line {
            stroke: rgba(148,163,184,0.24);
            stroke-width: 1;
        }
        .section-nav {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin: 0 0 18px;
            position: sticky;
            top: 0;
            z-index: 20;
            padding: 10px 0 14px;
            backdrop-filter: blur(10px);
        }
        .section-nav a {
            display: inline-flex;
            align-items: center;
            padding: 8px 12px;
            border-radius: 999px;
            border: 1px solid var(--line);
            background: rgba(255,255,255,0.75);
            color: var(--ink);
            text-decoration: none;
            font-size: 0.72rem;
            font-weight: 800;
            letter-spacing: 0.05em;
            text-transform: uppercase;
        }
        .section-nav a:hover {
            background: rgba(20,184,166,0.10);
            border-color: rgba(15,118,110,0.28);
        }
        @media (max-width: 960px) {
            .diagnostic-grid {
                grid-template-columns: 1fr;
            }
        }
        @media (max-width: 720px) {
            main {
                padding: 18px 12px 34px;
            }
            .hero {
                padding: 20px;
            }
            .hero-title {
                font-size: 1.8rem;
            }
            .highlight-stat-grid {
                grid-template-columns: 1fr;
                gap: 12px;
            }
            .profile-metric-grid,
            .metric-grid,
            .latest-grid,
            .highlight-grid {
                grid-template-columns: 1fr;
            }
            .profile-latency-grid {
                grid-template-columns: 1fr;
                gap: 8px;
            }
            .diagnostic-chart-wrap {
                grid-template-columns: 1fr;
            }
            .diagnostic-axis-y {
                display: none;
            }
            .diagnostic-axis-x {
                padding-left: 0;
            }
            .diagnostic-summary-head {
                flex-direction: column;
                align-items: flex-start;
            }
            .diagnostic-summary-meta {
                grid-template-columns: 1fr;
            }
            .diagnostic-summary-badges,
            .profile-card-badges {
                justify-content: flex-start;
            }
            .section-toggle-head {
                padding-right: 22px;
            }
            .section-toggle-head,
            .section-heading {
                flex-direction: column;
            }
        }
    </style>
</head>
<body>
    <main>
        <section class='hero'>
            <h1 class='hero-title'>$([System.Net.WebUtility]::HtmlEncode($Title))</h1>
            <p class='hero-subtitle'>Cross-profile benchmark scan built from the freshest run in each profile, with throughput, BW stability, IOPS, latency, and diagnostics coverage prioritized for quick comparison.</p>
            <div class='hero-meta'>
                <span>Generated $([System.Net.WebUtility]::HtmlEncode((Get-Date).ToString('yyyy-MM-dd HH:mm:ss')))</span>
                <span>Results root: $([System.Net.WebUtility]::HtmlEncode($resultsRootText))</span>
            </div>
            <div class='metric-grid'>
                $($heroMetricsHtml -join [Environment]::NewLine)
            </div>
        </section>

        <nav class='section-nav'>
            <a href='#overview'>Overview</a>
            <a href='#diagnostics'>Diagnostics</a>
            <a href='#profiles'>Profiles</a>
            <a href='#rollups'>Rollups</a>
            <a href='#runs'>Runs</a>
        </nav>

        <section id='overview' class='section-card'>
            <div class='section-heading'>
                <div>
                    <h2>Cross-Profile Highlights</h2>
                    <p>The overview compares the freshest run from every profile present in history.</p>
                </div>
            </div>
            <div class='highlight-grid'>
                $($highlightHtml -join [Environment]::NewLine)
            </div>
            <div class='section-heading'>
                <div>
                    <h2>Latest By Profile</h2>
                    <p>Each card represents the newest run captured for that profile.</p>
                </div>
            </div>
            <div class='latest-grid'>
                $($latestByProfileHtml -join [Environment]::NewLine)
            </div>
        </section>

        <section id='diagnostics' class='section-card'>
            <div class='section-heading'>
                <div>
                    <h2>Diagnostics Coverage By Profile</h2>
                    <p>Each profile picks the newest diagnostics-ready run available. Raw data tables under each chart are collapsed by default.</p>
                </div>
            </div>
            $coverageTableHtml
        </section>

        $($diagnosticsSectionsHtml -join [Environment]::NewLine)

        $(NewCollapsibleSectionMarkup -Id 'profiles' -Title 'Profiles' -Description 'Full history for each benchmark profile.' -BadgeText ('{0} profiles' -f $profileGroups.Count) -InnerHtml ($profileHistorySectionsHtml -join [Environment]::NewLine))

        $(NewCollapsibleSectionMarkup -Id 'rollups' -Title 'Historical Rollups' -Description 'Aggregated averages by profile and target path.' -BadgeText ('{0} rollups' -f $rollupRows.Count) -InnerHtml $rollupTableHtml)

        $(NewCollapsibleSectionMarkup -Id 'runs' -Title 'Runs' -Description 'Complete run-level detail ordered newest first.' -BadgeText ('{0} runs' -f $orderedRuns.Count) -InnerHtml $runTableHtml)
    </main>
</body>
</html>
"@

        $html | Set-Content -Path $Path -Encoding utf8
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

    Export-FioSqlBenchHtmlReportStatic -Runs $Runs -Path $Path -Title $Title -ResultsRoot $ResultsRoot -Rollups $Rollups
    return
}

Export-ModuleMember -Function Resolve-FioSqlBenchTarget, Get-FioSqlBenchProfileDefaults, Merge-FioSqlBenchSettings, New-FioSqlBenchRunContext, New-FioSqlBenchJobContent, Get-FioBenchFilePaths, Test-FioPreparedFiles, Resolve-FioBinary, Invoke-FioSqlBenchRun, ConvertFrom-FioJsonToSummary, Export-FioSqlBenchCsv, Export-FioSqlBenchDiagnosticsCsv, Import-FioSqlBenchHistory, Get-FioHistoricalRollup, Export-FioSqlBenchHistoricalCsv, Export-FioSqlBenchHtmlReport, New-FioHtmlProfileComparisonSection, ConvertTo-FioHtmlSettingBadges, Get-FioCpuAffinitySummary
