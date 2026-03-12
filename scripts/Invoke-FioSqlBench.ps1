<#
.SYNOPSIS
Runs SQL-oriented fio benchmarks against a local directory or SMB share.

.DESCRIPTION
This script builds a safe, file-based fio job for one of the built-in SQL-like
profiles (`Data`, `Log`, `Tempdb`, `BackupRestore`, or `DbccScan`), executes it,
and writes raw fio JSON plus normalized JSON/CSV/HTML summaries under the chosen
results directory.

Use `-DryRun` to inspect the effective settings and generated fio job without
requiring fio to be installed or any I/O to be executed.

.PARAMETER TargetPath
Existing directory to use for benchmark files. This can be a local folder or a
UNC path to an SMB share. Raw device paths are intentionally rejected.

.PARAMETER TargetType
Forces local or SMB classification. `Auto` infers SMB for UNC paths and local
for everything else.

.PARAMETER Profile
 Built-in SQL-like fio template to start from. `Data` models random 8K mixed I/O,
 `Log` models sequential log writes, `Tempdb` models heavier scratch traffic,
 `BackupRestore` models large-block sequential transfer, and `DbccScan` models
 large-block scan-heavy reads.

.PARAMETER FileSizeGB
Total file size across all fio workers. The script divides this across `NumJobs`.

.PARAMETER RuntimeSec
Measured runtime for the benchmark phase.

.PARAMETER RampSec
Warm-up time before fio starts reporting metrics.

.PARAMETER Iterations
Number of repeated fio runs to execute with the same settings.

.PARAMETER QueueDepth
fio queue depth (`iodepth`).

.PARAMETER NumJobs
Number of fio workers (`numjobs`).

.PARAMETER BlockSize
fio block size (`bs`).

.PARAMETER ReadMix
Read percentage for mixed workloads (`rwmixread`). Ignored for pure write tests.

.PARAMETER Fsync
fio `fsync` frequency. The `Log` profile defaults to `1`.

.PARAMETER Direct
Controls buffered vs direct I/O. `Auto` defaults to direct I/O for both local
and SMB targets in the built-in profiles so client-side caching is reduced by
default.

.PARAMETER OutputRoot
Root folder where result artifacts are written.

.PARAMETER RunLabel
Optional label appended to the generated run folder name.

.PARAMETER FioPath
Explicit path to `fio.exe`. If omitted, the script searches PATH, installed
program entries, and common Program Files locations.

.PARAMETER EnableLogs
Adds fio bandwidth and latency log files to the result set.

.PARAMETER KeepJobFile
Preserves the generated `.fio` job file in the results directory.

.PARAMETER NoCleanup
Preserves the temporary benchmark data files in the target directory.

.PARAMETER ReusePreparedFiles
Reuses an existing validated prepared-file set for the same profile and fio
settings under the target path. This can drastically reduce repeated test time,
especially for SMB targets, at the cost of keeping a persistent prep cache.

.PARAMETER DryRun
Generates settings and job content without requiring fio or touching the target.

.PARAMETER PassThru
Emits the structured PowerShell result object after the friendly console output.
Use this when you want to pipe the result into other commands or automation.

.PARAMETER Help
Shows the script help text without requiring any other parameters.

.EXAMPLE
./scripts/Invoke-FioSqlBench.ps1 -Help

.EXAMPLE
./scripts/Invoke-FioSqlBench.ps1 -TargetPath 'D:\SqlBench' -Profile Data -DryRun

.EXAMPLE
./scripts/Invoke-FioSqlBench.ps1 -TargetPath '\\fileserver\sqlbench' -Profile Log -Direct Off
#>
[CmdletBinding(SupportsShouldProcess, DefaultParameterSetName = 'Run')]
param(
    [Parameter(Mandatory, ParameterSetName = 'Run')]
    [string]$TargetPath,

    [Parameter(ParameterSetName = 'Run')]
    [ValidateSet('Auto', 'Local', 'Smb')]
    [string]$TargetType = 'Auto',

    [Parameter(ParameterSetName = 'Run')]
    [ValidateSet('Data', 'Log', 'Tempdb', 'BackupRestore', 'DbccScan')]
    [string]$Profile = 'Data',

    [Parameter(ParameterSetName = 'Run')]
    [Nullable[decimal]]$FileSizeGB,
    [Parameter(ParameterSetName = 'Run')]
    [Nullable[int]]$RuntimeSec,
    [Parameter(ParameterSetName = 'Run')]
    [Nullable[int]]$RampSec,
    [Parameter(ParameterSetName = 'Run')]
    [Nullable[int]]$Iterations,
    [Parameter(ParameterSetName = 'Run')]
    [Nullable[int]]$QueueDepth,
    [Parameter(ParameterSetName = 'Run')]
    [Nullable[int]]$NumJobs,
    [Parameter(ParameterSetName = 'Run')]
    [string]$BlockSize,
    [Parameter(ParameterSetName = 'Run')]
    [Nullable[int]]$ReadMix,
    [Parameter(ParameterSetName = 'Run')]
    [Nullable[int]]$Fsync,

    [Parameter(ParameterSetName = 'Run')]
    [ValidateSet('Auto', 'On', 'Off')]
    [string]$Direct = 'Auto',

    [Parameter(ParameterSetName = 'Run')]
    [string]$OutputRoot = (Join-Path -Path (Get-Location) -ChildPath 'results'),
    [Parameter(ParameterSetName = 'Run')]
    [string]$RunLabel,
    [Parameter(ParameterSetName = 'Run')]
    [string]$FioPath,
    [Parameter(ParameterSetName = 'Run')]
    [switch]$EnableLogs,
    [Parameter(ParameterSetName = 'Run')]
    [switch]$KeepJobFile,
    [Parameter(ParameterSetName = 'Run')]
    [switch]$NoCleanup,
    [Parameter(ParameterSetName = 'Run')]
    [switch]$ReusePreparedFiles,
    [Parameter(ParameterSetName = 'Run')]
    [switch]$DryRun,
    [Parameter(ParameterSetName = 'Run')]
    [switch]$PassThru,

    [Parameter(ParameterSetName = 'Help')]
    [Alias('h', '?')]
    [switch]$Help
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-FioConsoleBanner {
    param(
        [string]$Title,
        [string]$Subtitle
    )

    $line = '=' * 72
    Write-Host $line -ForegroundColor DarkCyan
    Write-Host (" {0}" -f $Title) -ForegroundColor Cyan
    if ($Subtitle) {
        Write-Host (" {0}" -f $Subtitle) -ForegroundColor DarkGray
    }
    Write-Host $line -ForegroundColor DarkCyan
}

function Write-FioStage {
    param(
        [string]$Title,
        [string]$Status = 'INFO'
    )

    $color = switch ($Status) {
        'OK' { 'Green' }
        'WARN' { 'Yellow' }
        'RUN' { 'Cyan' }
        default { 'White' }
    }

    Write-Host ''
    Write-Host ("[{0}] {1}" -f $Status, $Title) -ForegroundColor $color
}

function Write-FioProperty {
    param(
        [string]$Name,
        [AllowNull()][object]$Value
    )

    $display = if ($null -eq $Value -or [string]::IsNullOrWhiteSpace([string]$Value)) { '-' } else { [string]$Value }
    Write-Host ("  {0,-20} : {1}" -f $Name, $display) -ForegroundColor Gray
}

function Format-FioBoolean {
    param(
        [AllowNull()][object]$Value
    )

    if ($null -eq $Value -or [string]::IsNullOrWhiteSpace([string]$Value)) {
        return '-'
    }

    if ([bool]$Value) {
        return 'Yes'
    }

    return 'No'
}

function Write-FioSmbReport {
    param(
        [pscustomobject]$SmbMetadata,
        [int]$Direct
    )

    if ($null -eq $SmbMetadata) {
        return
    }

    Write-FioProperty -Name 'SMB server' -Value $SmbMetadata.ServerName
    Write-FioProperty -Name 'SMB share' -Value $SmbMetadata.ShareName
    Write-FioProperty -Name 'SMB dialect' -Value $SmbMetadata.Dialect
    Write-FioProperty -Name 'SMB user' -Value $SmbMetadata.Credential
    Write-FioProperty -Name 'SMB opens' -Value $SmbMetadata.NumOpens
    Write-FioProperty -Name 'SMB CA share' -Value (Format-FioBoolean -Value $SmbMetadata.ContinuouslyAvailable)
    Write-FioProperty -Name 'SMB encrypted' -Value (Format-FioBoolean -Value $SmbMetadata.EncryptData)
    Write-FioProperty -Name 'SMB channels' -Value $SmbMetadata.MultichannelPathCount
    Write-FioProperty -Name 'SMB RDMA paths' -Value $SmbMetadata.RdmaPathCount

    Write-Host 'SMB path assessment' -ForegroundColor Cyan

    if ($Direct -eq 1) {
        Write-Host '  - Direct I/O is enabled, so this run is reducing client-side cache effects by default.' -ForegroundColor Green
    }
    else {
        Write-Host '  - Buffered I/O is enabled. SMB client cache can inflate reads and mixed workloads.' -ForegroundColor Yellow
    }

    if ($SmbMetadata.Dialect) {
        if ([string]$SmbMetadata.Dialect -like '3*') {
            Write-Host ('  - SMB {0} is active. That is the expected protocol family for modern SQL-over-SMB testing.' -f $SmbMetadata.Dialect) -ForegroundColor Green
        }
        else {
            Write-Host ('  - SMB dialect {0} is older than SMB 3.x. Treat results carefully if you are comparing against Microsoft SQL-over-SMB guidance.' -f $SmbMetadata.Dialect) -ForegroundColor Yellow
        }
    }

    if ($null -ne $SmbMetadata.MultichannelPathCount) {
        if ([int]$SmbMetadata.MultichannelPathCount -gt 1) {
            Write-Host ('  - {0} SMB channels are active to this server. Multichannel is contributing to the path.' -f $SmbMetadata.MultichannelPathCount) -ForegroundColor Green
        }
        elseif ([int]$SmbMetadata.MultichannelPathCount -eq 1) {
            Write-Host '  - Only one active SMB channel was visible. Network redundancy or throughput scaling may be limited.' -ForegroundColor Yellow
        }
    }

    if ($null -ne $SmbMetadata.RdmaPathCount) {
        if ([int]$SmbMetadata.RdmaPathCount -gt 0) {
            Write-Host ('  - {0} RDMA-capable SMB path(s) were detected. SMB Direct is available on at least part of the route.' -f $SmbMetadata.RdmaPathCount) -ForegroundColor Green
        }
        else {
            Write-Host '  - No RDMA-capable SMB paths were detected. Expect more CPU and latency overhead than an SMB Direct path.' -ForegroundColor DarkYellow
        }
    }

    if ($null -ne $SmbMetadata.ContinuouslyAvailable -and -not [bool]$SmbMetadata.ContinuouslyAvailable) {
        Write-Host '  - The share does not report continuous availability. That may matter if you are using this result as a SQL HA storage baseline.' -ForegroundColor DarkYellow
    }
}

function Format-FioByteCount {
    param(
        [AllowNull()][double]$Bytes
    )

    if ($null -eq $Bytes -or $Bytes -lt 0) {
        return '-'
    }

    if ($Bytes -ge 1TB) { return ('{0:N2} TB' -f ($Bytes / 1TB)) }
    if ($Bytes -ge 1GB) { return ('{0:N2} GB' -f ($Bytes / 1GB)) }
    if ($Bytes -ge 1MB) { return ('{0:N2} MB' -f ($Bytes / 1MB)) }
    if ($Bytes -ge 1KB) { return ('{0:N2} KB' -f ($Bytes / 1KB)) }
    return ('{0:N0} B' -f $Bytes)
}

function Get-FioPhysicalMemoryBytes {
    try {
        $computerSystem = Get-CimInstance -ClassName Win32_ComputerSystem -ErrorAction Stop
        if ($null -ne $computerSystem.TotalPhysicalMemory) {
            return [double]$computerSystem.TotalPhysicalMemory
        }
    }
    catch {
    }

    try {
        return [double]([Microsoft.VisualBasic.Devices.ComputerInfo]::new().TotalPhysicalMemory)
    }
    catch {
    }

    return $null
}

function Get-FioCacheBypassAssessment {
    param(
        [string]$TargetType,
        [pscustomobject]$Settings,
        [AllowNull()][double]$PhysicalMemoryBytes
    )

    $messages = New-Object System.Collections.Generic.List[object]
    $riskLevel = 'Low'
    $cacheMode = if ($Settings.Direct -eq 1) { 'Direct I/O' } else { 'Buffered I/O' }

    if ($TargetType -eq 'Local') {
        if ($Settings.Direct -eq 1) {
            $messages.Add([pscustomobject]@{ Color = 'Green'; Text = 'Local direct I/O is enabled. With fio windowsaio this requests FILE_FLAG_NO_BUFFERING, which bypasses the Windows file cache.' })
        }
        else {
            $riskLevel = 'High'
            $messages.Add([pscustomobject]@{ Color = 'Yellow'; Text = 'Local buffered I/O is enabled. The Windows file cache can materially inflate read and mixed-workload results.' })
        }
    }
    else {
        if ($Settings.Direct -eq 1) {
            $riskLevel = 'Medium'
            $messages.Add([pscustomobject]@{ Color = 'Cyan'; Text = 'Direct I/O was requested for SMB. This reduces client-side caching risk if the path honors it, but server-side and storage-side cache can still influence results.' })
        }
        else {
            $riskLevel = 'High'
            $messages.Add([pscustomobject]@{ Color = 'Yellow'; Text = 'Buffered SMB I/O is enabled. Client cache, server cache, and storage cache can all inflate results.' })
        }
    }

    if ($null -ne $PhysicalMemoryBytes) {
        if ($Settings.Direct -eq 0 -and $Settings.TotalFileBytes -lt $PhysicalMemoryBytes) {
            $riskLevel = 'High'
            $messages.Add([pscustomobject]@{ Color = 'Yellow'; Text = ('Working set {0} is smaller than physical RAM {1}. Buffered runs at this size are very likely to be memory-cached.' -f (Format-FioByteCount -Bytes $Settings.TotalFileBytes), (Format-FioByteCount -Bytes $PhysicalMemoryBytes)) })
        }
    }

    if ($Settings.TotalFileBytes -lt 4GB) {
        if ($riskLevel -eq 'Low') {
            $riskLevel = 'Medium'
        }
        $messages.Add([pscustomobject]@{ Color = 'Yellow'; Text = ('Working set {0} is very small for storage benchmarking. Even with direct I/O, device, controller, or SMB-server cache can overstate performance.' -f (Format-FioByteCount -Bytes $Settings.TotalFileBytes)) })
    }

    $recommendedMinimumBytes = $null
    if ($null -ne $PhysicalMemoryBytes -and $Settings.Direct -eq 0) {
        $recommendedMinimumBytes = [math]::Max([double](4GB), [double]([math]::Ceiling(($PhysicalMemoryBytes * 1.25) / 1GB) * 1GB))
    }
    elseif ($Settings.TotalFileBytes -lt 4GB) {
        $recommendedMinimumBytes = [double](4GB)
    }

    $messages.Add([pscustomobject]@{ Color = 'DarkGray'; Text = 'fio can bypass the Windows page cache here, but it cannot generically bypass controller DRAM, SSD firmware cache, RAID cache, or remote SMB server cache for file-based workloads.' })

    [pscustomobject]@{
        CacheMode = $cacheMode
        RiskLevel = $riskLevel
        PhysicalMemoryBytes = $PhysicalMemoryBytes
        RecommendedMinimumBytes = $recommendedMinimumBytes
        Messages = $messages
    }
}

function Write-FioCacheAssessment {
    param(
        [pscustomobject]$Assessment
    )

    $status = switch ($Assessment.RiskLevel) {
        'High' { 'WARN' }
        'Medium' { 'WARN' }
        default { 'OK' }
    }

    Write-FioStage -Title 'Cache-bypass assessment' -Status $status
    Write-FioProperty -Name 'Cache mode' -Value $Assessment.CacheMode
    if ($null -ne $Assessment.PhysicalMemoryBytes) {
        Write-FioProperty -Name 'Physical RAM' -Value (Format-FioByteCount -Bytes $Assessment.PhysicalMemoryBytes)
    }
    if ($null -ne $Assessment.RecommendedMinimumBytes) {
        Write-FioProperty -Name 'Suggested size' -Value (Format-FioByteCount -Bytes $Assessment.RecommendedMinimumBytes)
    }

    foreach ($message in $Assessment.Messages) {
        Write-Host ("  - {0}" -f $message.Text) -ForegroundColor $message.Color
    }
}

function Convert-FioUsToMs {
    param(
        [AllowNull()][double]$Microseconds
    )

    if ($null -eq $Microseconds) {
        return $null
    }

    [math]::Round(($Microseconds / 1000.0), 2)
}

function Get-FioAssessmentVisual {
    param(
        [string]$Status
    )

    switch ($Status) {
        'Excellent' { return [pscustomobject]@{ Color = 'Green'; Rank = 0 } }
        'Very good' { return [pscustomobject]@{ Color = 'Green'; Rank = 1 } }
        'Good' { return [pscustomobject]@{ Color = 'DarkGreen'; Rank = 2 } }
        'Watch' { return [pscustomobject]@{ Color = 'Yellow'; Rank = 3 } }
        'Poor' { return [pscustomobject]@{ Color = 'DarkYellow'; Rank = 4 } }
        'Bad' { return [pscustomobject]@{ Color = 'Red'; Rank = 5 } }
        default { return [pscustomobject]@{ Color = 'Magenta'; Rank = 6 } }
    }
}

function Get-FioGenericLatencyAssessment {
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

function Get-FioSqlProfileAssessment {
    param(
        [string]$Profile,
        [string]$TargetType,
        [string]$Operation,
        [AllowNull()][double]$MeanLatencyMs,
        [AllowNull()][double]$P99LatencyMs,
        [AllowNull()][double]$P999LatencyMs,
        [AllowNull()][double]$WorstP99LatencyMs,
        [int]$Direct
    )

    $notes = New-Object System.Collections.Generic.List[string]
    $status = Get-FioGenericLatencyAssessment -LatencyMs $MeanLatencyMs

    switch ($Profile) {
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
            $current = Get-FioAssessmentVisual -Status $status
            $tail = Get-FioAssessmentVisual -Status 'Watch'
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
        if ($Profile -eq 'Log' -and $Operation -eq 'Write' -and $P999LatencyMs -gt 25) {
            $status = if ($TargetType -eq 'Smb') { 'Poor' } else { 'Bad' }
            $notes.Add('P99.9 latency shows severe commit stalls. This is a strong SQL log tail-latency warning sign.')
        }
        elseif ($P999LatencyMs -gt 30) {
            $current = Get-FioAssessmentVisual -Status $status
            $tail = Get-FioAssessmentVisual -Status 'Watch'
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
        Color = (Get-FioAssessmentVisual -Status $status).Color
        Notes = $notes
    }
}

function Get-FioOperationRenderModel {
    param(
        [string]$Profile,
        [string]$TargetType,
        [string]$Operation,
        [pscustomobject]$Stats,
        [int]$Direct
    )

    $meanMs = Convert-FioUsToMs -Microseconds $Stats.MeanLatencyUs
    $p95Ms = Convert-FioUsToMs -Microseconds $Stats.P95LatencyUs
    $p99Ms = Convert-FioUsToMs -Microseconds $Stats.P99LatencyUs
    $p999Ms = Convert-FioUsToMs -Microseconds $Stats.P999LatencyUs
    $worstP99Ms = Convert-FioUsToMs -Microseconds $Stats.WorstP99LatencyUs
    $assessment = Get-FioSqlProfileAssessment -Profile $Profile -TargetType $TargetType -Operation $Operation -MeanLatencyMs $meanMs -P99LatencyMs $p99Ms -P999LatencyMs $p999Ms -WorstP99LatencyMs $worstP99Ms -Direct $Direct

    [pscustomobject]@{
        Operation = $Operation
        Iops = [math]::Round($Stats.Iops, 2)
        BandwidthMBps = [math]::Round($Stats.BandwidthMBps, 2)
        MeanMs = $meanMs
        P95Ms = $p95Ms
        P99Ms = $p99Ms
        P999Ms = $p999Ms
        WorstP99Ms = $worstP99Ms
        TotalIos = $Stats.TotalIos
        BandwidthCvPercent = $Stats.BandwidthCvPercent
        IopsCvPercent = $Stats.IopsCvPercent
        Status = $assessment.Status
        Color = $assessment.Color
        Notes = $assessment.Notes
    }
}

function Write-FioPerformanceTable {
    param(
        [string]$Profile,
        [string]$TargetType,
        [pscustomobject]$Summary
    )

    $rows = @(
        Get-FioOperationRenderModel -Profile $Profile -TargetType $TargetType -Operation 'Read' -Stats $Summary.Read -Direct $Summary.Direct
        Get-FioOperationRenderModel -Profile $Profile -TargetType $TargetType -Operation 'Write' -Stats $Summary.Write -Direct $Summary.Direct
    )

    Write-Host ''
    Write-Host 'SQL-oriented performance summary' -ForegroundColor Cyan
    Write-Host ('-' * 114) -ForegroundColor DarkCyan
    Write-Host ('  {0,-9} {1,12} {2,12} {3,10} {4,10} {5,10} {6,11} {7,12}' -f 'Operation', 'IOPS', 'MB/s', 'Mean ms', 'P95 ms', 'P99 ms', 'P99.9 ms', 'Assessment') -ForegroundColor DarkGray

    foreach ($row in $rows) {
        Write-Host ('  {0,-9} {1,12:N2} {2,12:N2} {3,10:N2} {4,10:N2} {5,10:N2} {6,11:N2} ' -f $row.Operation, $row.Iops, $row.BandwidthMBps, $row.MeanMs, $row.P95Ms, $row.P99Ms, $row.P999Ms) -NoNewline -ForegroundColor Gray
        Write-Host ('{0,12}' -f $row.Status) -ForegroundColor $row.Color
    }
    Write-Host ('-' * 114) -ForegroundColor DarkCyan
}

function Write-FioSqlInterpretation {
    param(
        [string]$Profile,
        [string]$TargetType,
        [pscustomobject]$Summary
    )

    $rows = @(
        Get-FioOperationRenderModel -Profile $Profile -TargetType $TargetType -Operation 'Read' -Stats $Summary.Read -Direct $Summary.Direct
        Get-FioOperationRenderModel -Profile $Profile -TargetType $TargetType -Operation 'Write' -Stats $Summary.Write -Direct $Summary.Direct
    )

    Write-Host 'SQL latency interpretation' -ForegroundColor Cyan
    Write-FioProperty -Name 'Microsoft rule' -Value 'Sustained 10-15 ms usually warrants SQL I/O investigation.'
    switch ($Profile) {
        'BackupRestore' {
            if ($TargetType -eq 'Smb') {
                Write-FioProperty -Name 'Profile target' -Value 'Backup and restore over SMB should show stable large-block throughput with latency spikes kept out of sustained operation.'
            }
            else {
                Write-FioProperty -Name 'Profile target' -Value 'Backup and restore are throughput-led workloads. Look for strong large-block MB/s without sustained double-digit latency.'
            }
        }
        'DbccScan' {
            if ($TargetType -eq 'Smb') {
                Write-FioProperty -Name 'Profile target' -Value 'DBCC-like scans over SMB should maintain predictable sequential read throughput while avoiding sustained tail-latency spikes.'
            }
            else {
                Write-FioProperty -Name 'Profile target' -Value 'DBCC-like scan workloads should keep large-block read latency controlled while favoring consistent throughput.'
            }
        }
        'Log' {
            if ($TargetType -eq 'Smb') {
                Write-FioProperty -Name 'Profile target' -Value 'SMB log writes should still trend toward low single-digit ms; 10-15 ms remains the escalation line.'
            }
            else {
                Write-FioProperty -Name 'Profile target' -Value 'Log writes are best around 1-5 ms.'
            }
        }
        default {
            if ($TargetType -eq 'Smb') {
                Write-FioProperty -Name 'Profile target' -Value 'SMB data/tempdb I/O should still stay below 10-15 ms where possible; slightly higher overhead can be normal without SMB Direct.'
            }
            else {
                Write-FioProperty -Name 'Profile target' -Value 'Data/tempdb I/O is healthiest under 10 ms; 4-20 ms is a common tuned range.'
            }
        }
    }

    if ($TargetType -eq 'Smb') {
        Write-FioProperty -Name 'SMB note' -Value 'SQL over SMB is supported; Microsoft recommends adequate bandwidth, SMB Multichannel, and SMB Direct/RDMA where available.'
    }

    foreach ($row in $rows) {
        Write-Host ''
        Write-Host ("  {0}" -f $row.Operation) -ForegroundColor $row.Color
        Write-FioProperty -Name 'Mean latency' -Value ("{0:N2} ms" -f $row.MeanMs)
        Write-FioProperty -Name 'P99 latency' -Value ("{0:N2} ms" -f $row.P99Ms)
        Write-FioProperty -Name 'P99.9 latency' -Value ("{0:N2} ms" -f $row.P999Ms)
        if ($null -ne $row.WorstP99Ms) {
            Write-FioProperty -Name 'Worst job P99' -Value ("{0:N2} ms" -f $row.WorstP99Ms)
        }
        if ($null -ne $row.BandwidthCvPercent) {
            Write-FioProperty -Name 'BW stability' -Value ("CV {0:N2}%" -f $row.BandwidthCvPercent)
        }
        Write-FioProperty -Name 'Assessment' -Value $row.Status
        foreach ($note in $row.Notes | Select-Object -Unique) {
            Write-Host ("    - {0}" -f $note) -ForegroundColor DarkGray
        }
    }
}

function Get-FioProfileRecommendations {
    param(
        [string]$Profile,
        [string]$TargetType,
        [pscustomobject]$Summary
    )

    $read = Get-FioOperationRenderModel -Profile $Profile -TargetType $TargetType -Operation 'Read' -Stats $Summary.Read -Direct $Summary.Direct
    $write = Get-FioOperationRenderModel -Profile $Profile -TargetType $TargetType -Operation 'Write' -Stats $Summary.Write -Direct $Summary.Direct

    $recommendations = New-Object System.Collections.Generic.List[object]

    switch ($Profile) {
        'BackupRestore' {
            $readBandwidthCv = if ($null -ne $read.BandwidthCvPercent) { $read.BandwidthCvPercent } else { 0 }
            $writeBandwidthCv = if ($null -ne $write.BandwidthCvPercent) { $write.BandwidthCvPercent } else { 0 }
            if ($read.BandwidthMBps -ge 250 -and $write.BandwidthMBps -ge 250 -and $readBandwidthCv -le 20 -and $writeBandwidthCv -le 20) {
                $recommendations.Add([pscustomobject]@{ Color = 'Green'; Text = 'Large-block backup or restore transfer looks healthy. The path is sustaining meaningful sequential throughput in both directions.' })
            }
            else {
                $recommendations.Add([pscustomobject]@{ Color = 'Yellow'; Text = 'Large-block backup or restore throughput or stability is modest. Check network bandwidth, backup target write policy, and whether the storage path is saturating before SQL does.' })
            }
        }
        'DbccScan' {
            if ($read.MeanMs -le 10 -and $read.P999Ms -le 20 -and $read.BandwidthMBps -ge 150) {
                $recommendations.Add([pscustomobject]@{ Color = 'Green'; Text = 'DBCC-like scan reads are behaving predictably. This is a reasonable baseline for large sequential inspection workloads.' })
            }
            else {
                $recommendations.Add([pscustomobject]@{ Color = 'Yellow'; Text = 'DBCC-like scan reads show either limited throughput or elevated latency. Review read-ahead efficiency, storage queueing, and any SMB serialization in the path.' })
            }
        }
        'Log' {
            if ($write.P99Ms -le 10 -and $write.P999Ms -le 20 -and $write.MeanMs -le 5) {
                $recommendations.Add([pscustomobject]@{ Color = 'Green'; Text = 'Sequential log-style writes look healthy. This profile is in a range that should not point to WRITELOG pressure by itself.' })
            }
            else {
                $recommendations.Add([pscustomobject]@{ Color = 'Yellow'; Text = 'Log-style writes are above ideal. Review write-through performance, cache policy, fsync behavior, and storage commit latency.' })
            }
        }
        'Tempdb' {
            if ($read.P99Ms -le 10 -and $write.P99Ms -le 15 -and $write.P999Ms -le 30) {
                $recommendations.Add([pscustomobject]@{ Color = 'Green'; Text = 'Tempdb-like random I/O is behaving within a generally healthy SQL range.' })
            }
            else {
                $recommendations.Add([pscustomobject]@{ Color = 'Yellow'; Text = 'Tempdb-like small-block concurrency shows pressure. Investigate tail latency, queue depth, and storage contention under mixed random I/O.' })
            }
        }
        default {
            if ($read.P99Ms -le 10 -and $write.P99Ms -le 15 -and $write.P999Ms -le 30) {
                $recommendations.Add([pscustomobject]@{ Color = 'Green'; Text = 'This looks like a solid baseline for OLTP-style data-file behavior. Validate again with a larger working set and longer runtime.' })
            }
            else {
                $recommendations.Add([pscustomobject]@{ Color = 'Yellow'; Text = 'OLTP-style data behavior shows tail latency risk. Investigate queue depth, background contention, and storage saturation before treating this as production-ready.' })
            }
        }
    }

    if (($null -ne $read.BandwidthCvPercent -and $read.BandwidthCvPercent -gt 25) -or ($null -ne $write.BandwidthCvPercent -and $write.BandwidthCvPercent -gt 25)) {
        $recommendations.Add([pscustomobject]@{ Color = 'Yellow'; Text = 'Bandwidth stability is uneven across samples. Re-run at a longer duration and check for throttling, burst behavior, or background contention before treating this as a baseline.' })
    }

    if ($TargetType -eq 'Smb') {
        $recommendations.Add([pscustomobject]@{ Color = 'Cyan'; Text = 'Because this target is SMB-backed, verify SMB Multichannel, SMB Direct/RDMA where available, network bandwidth headroom, and continuous-availability support on the share.' })
    }

    $recommendations
}

function Write-FioRecommendations {
    param(
        [string]$Profile,
        [string]$TargetType,
        [pscustomobject]$Summary
    )

    $recommendations = Get-FioProfileRecommendations -Profile $Profile -TargetType $TargetType -Summary $Summary
    Write-Host 'Recommendations' -ForegroundColor Cyan
    foreach ($item in $recommendations) {
        Write-Host ("  - {0}" -f $item.Text) -ForegroundColor $item.Color
    }
}

function Get-FioStatTriplet {
    param(
        [object[]]$Values
    )

    $numeric = @($Values | Where-Object { $null -ne $_ } | ForEach-Object { [double]$_ })
    if ($numeric.Count -eq 0) {
        return [pscustomobject]@{ Min = $null; Avg = $null; Max = $null }
    }

    [pscustomobject]@{
        Min = [math]::Round((($numeric | Measure-Object -Minimum).Minimum), 2)
        Avg = [math]::Round((($numeric | Measure-Object -Average).Average), 2)
        Max = [math]::Round((($numeric | Measure-Object -Maximum).Maximum), 2)
    }
}

function Write-FioRollupTable {
    param(
        [object[]]$Summaries
    )

    if ($Summaries.Count -le 1) {
        return
    }

    Write-Host ''
    Write-Host 'Iteration rollup (min / avg / max)' -ForegroundColor Cyan
    Write-Host ('-' * 92) -ForegroundColor DarkCyan
    Write-Host ('  {0,-9} {1,-12} {2,12} {3,12} {4,12}' -f 'Operation', 'Metric', 'Min', 'Avg', 'Max') -ForegroundColor DarkGray

    $definitions = @(
        @{ Operation = 'Read';  Metric = 'IOPS';    Values = @($Summaries | ForEach-Object { $_.Read.Iops }) }
        @{ Operation = 'Read';  Metric = 'MB/s';    Values = @($Summaries | ForEach-Object { $_.Read.BandwidthMBps }) }
        @{ Operation = 'Read';  Metric = 'Mean ms'; Values = @($Summaries | ForEach-Object { Convert-FioUsToMs -Microseconds $_.Read.MeanLatencyUs }) }
        @{ Operation = 'Read';  Metric = 'P99 ms';  Values = @($Summaries | ForEach-Object { Convert-FioUsToMs -Microseconds $_.Read.P99LatencyUs }) }
        @{ Operation = 'Read';  Metric = 'P99.9 ms'; Values = @($Summaries | ForEach-Object { Convert-FioUsToMs -Microseconds $_.Read.P999LatencyUs }) }
        @{ Operation = 'Write'; Metric = 'IOPS';    Values = @($Summaries | ForEach-Object { $_.Write.Iops }) }
        @{ Operation = 'Write'; Metric = 'MB/s';    Values = @($Summaries | ForEach-Object { $_.Write.BandwidthMBps }) }
        @{ Operation = 'Write'; Metric = 'Mean ms'; Values = @($Summaries | ForEach-Object { Convert-FioUsToMs -Microseconds $_.Write.MeanLatencyUs }) }
        @{ Operation = 'Write'; Metric = 'P99 ms';  Values = @($Summaries | ForEach-Object { Convert-FioUsToMs -Microseconds $_.Write.P99LatencyUs }) }
        @{ Operation = 'Write'; Metric = 'P99.9 ms'; Values = @($Summaries | ForEach-Object { Convert-FioUsToMs -Microseconds $_.Write.P999LatencyUs }) }
    )

    foreach ($definition in $definitions) {
        $triplet = Get-FioStatTriplet -Values $definition.Values
        Write-Host ('  {0,-9} {1,-12} {2,12:N2} {3,12:N2} {4,12:N2}' -f $definition.Operation, $definition.Metric, $triplet.Min, $triplet.Avg, $triplet.Max) -ForegroundColor Gray
    }
    Write-Host ('-' * 92) -ForegroundColor DarkCyan
}

function Get-FioAggregateSummary {
    param(
        [string]$RunId,
        [string]$Profile,
        [string]$TargetPath,
        [string]$TargetType,
        [string]$ResultDirectory,
        [object[]]$Summaries
    )

    if ($Summaries.Count -eq 1) {
        return $Summaries[0]
    }

    $readIops = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Read.Iops }
    $readBandwidth = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Read.BandwidthMBps }
    $readIoMb = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Read.IoMB }
    $readTotalIos = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Read.TotalIos }
    $readMeanLatency = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Read.MeanLatencyUs }
    $readP50Latency = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Read.P50LatencyUs }
    $readP95Latency = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Read.P95LatencyUs }
    $readP99Latency = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Read.P99LatencyUs }
    $readP999Latency = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Read.P999LatencyUs }
    $readWorstP99Latency = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Read.WorstP99LatencyUs }
    $readWorstP999Latency = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Read.WorstP999LatencyUs }
    $readBandwidthCv = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Read.BandwidthCvPercent }
    $readIopsCv = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Read.IopsCvPercent }

    $writeIops = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Write.Iops }
    $writeBandwidth = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Write.BandwidthMBps }
    $writeIoMb = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Write.IoMB }
    $writeTotalIos = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Write.TotalIos }
    $writeMeanLatency = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Write.MeanLatencyUs }
    $writeP50Latency = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Write.P50LatencyUs }
    $writeP95Latency = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Write.P95LatencyUs }
    $writeP99Latency = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Write.P99LatencyUs }
    $writeP999Latency = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Write.P999LatencyUs }
    $writeWorstP99Latency = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Write.WorstP99LatencyUs }
    $writeWorstP999Latency = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Write.WorstP999LatencyUs }
    $writeBandwidthCv = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Write.BandwidthCvPercent }
    $writeIopsCv = Get-FioAverageValue -Summaries $Summaries -ValueScript { param($summary) $summary.Write.IopsCvPercent }

    $read = [pscustomobject]@{
        Iops = $readIops
        BandwidthMBps = $readBandwidth
        IoMB = $readIoMb
        TotalIos = $readTotalIos
        MeanLatencyUs = $readMeanLatency
        P50LatencyUs = $readP50Latency
        P95LatencyUs = $readP95Latency
        P99LatencyUs = $readP99Latency
        P999LatencyUs = $readP999Latency
        WorstP99LatencyUs = $readWorstP99Latency
        WorstP999LatencyUs = $readWorstP999Latency
        BandwidthCvPercent = $readBandwidthCv
        IopsCvPercent = $readIopsCv
    }

    $write = [pscustomobject]@{
        Iops = $writeIops
        BandwidthMBps = $writeBandwidth
        IoMB = $writeIoMb
        TotalIos = $writeTotalIos
        MeanLatencyUs = $writeMeanLatency
        P50LatencyUs = $writeP50Latency
        P95LatencyUs = $writeP95Latency
        P99LatencyUs = $writeP99Latency
        P999LatencyUs = $writeP999Latency
        WorstP99LatencyUs = $writeWorstP99Latency
        WorstP999LatencyUs = $writeWorstP999Latency
        BandwidthCvPercent = $writeBandwidthCv
        IopsCvPercent = $writeIopsCv
    }

    [pscustomobject]@{
        RunId = $RunId
        TimestampUtc = [DateTime]::UtcNow.ToString('o')
        Iteration = 'Average'
        TargetPath = $TargetPath
        TargetType = $TargetType
        RuntimeSec = $Summaries[0].RuntimeSec
        RampSec = $Summaries[0].RampSec
        FileSizeGB = $Summaries[0].FileSizeGB
        BlockSize = $Summaries[0].BlockSize
        QueueDepth = $Summaries[0].QueueDepth
        NumJobs = $Summaries[0].NumJobs
        Direct = $Summaries[0].Direct
        ReadWrite = $Summaries[0].ReadWrite
        ReadMix = $Summaries[0].ReadMix
        Fsync = $Summaries[0].Fsync
        ResultDirectory = $ResultDirectory
        Read = $read
        Write = $write
    }
}

function Get-FioAverageValue {
    param(
        [object[]]$Summaries,
        [scriptblock]$ValueScript
    )

    $values = foreach ($summary in $Summaries) {
        $value = & $ValueScript $summary
        if ($null -ne $value) {
            [double]$value
        }
    }

    if (-not $values -or $values.Count -eq 0) {
        return $null
    }

    [math]::Round((($values | Measure-Object -Average).Average), 2)
}

function Write-FioSettingsBlock {
    param(
        [pscustomobject]$TargetInfo,
        [pscustomobject]$Settings,
        [pscustomobject]$RunContext,
        [string]$Profile
    )

    Write-FioProperty -Name 'Profile' -Value $Profile
    Write-FioProperty -Name 'Target path' -Value $TargetInfo.Path
    Write-FioProperty -Name 'Target type' -Value $TargetInfo.Type
    if ($TargetInfo.Type -eq 'Smb' -and $null -ne $TargetInfo.SmbMetadata) {
        Write-FioProperty -Name 'SMB remote' -Value $TargetInfo.SmbMetadata.RemotePath
        if ($TargetInfo.SmbMetadata.MappedDrive) {
            Write-FioProperty -Name 'Mapped drive' -Value $TargetInfo.SmbMetadata.MappedDrive
        }
        Write-FioSmbReport -SmbMetadata $TargetInfo.SmbMetadata -Direct $Settings.Direct
    }
    $targetCreated = if ($TargetInfo.CreatedDirectory) { 'Yes' } else { 'No' }
    Write-FioProperty -Name 'Target created' -Value $targetCreated
    Write-FioProperty -Name 'Run id' -Value $RunContext.RunId
    Write-FioProperty -Name 'Results' -Value $RunContext.ResultDirectory
    Write-FioProperty -Name 'Runtime (sec)' -Value $Settings.RuntimeSec
    Write-FioProperty -Name 'Ramp (sec)' -Value $Settings.RampSec
    Write-FioProperty -Name 'Iterations' -Value $Settings.Iterations
    Write-FioProperty -Name 'Block size' -Value $Settings.BlockSize
    Write-FioProperty -Name 'Queue depth' -Value $Settings.QueueDepth
    Write-FioProperty -Name 'Jobs' -Value $Settings.NumJobs
    Write-FioProperty -Name 'Direct I/O' -Value $(if ($Settings.Direct -eq 1) { 'Enabled' } else { 'Disabled' })
    Write-FioProperty -Name 'Total size (GB)' -Value ([math]::Round(($Settings.TotalFileBytes / 1GB), 2))
    if ($null -ne $Settings.ReadMix) {
        Write-FioProperty -Name 'Read mix (%)' -Value $Settings.ReadMix
    }
    if ($Settings.Fsync -gt 0) {
        Write-FioProperty -Name 'fsync' -Value $Settings.Fsync
    }
}

function Write-FioIterationSummary {
    param(
        [string]$Profile,
        [string]$TargetType,
        [pscustomobject]$Summary
    )

    Write-FioPerformanceTable -Profile $Profile -TargetType $TargetType -Summary $Summary
    Write-FioSqlInterpretation -Profile $Profile -TargetType $TargetType -Summary $Summary
    Write-FioRecommendations -Profile $Profile -TargetType $TargetType -Summary $Summary
}

function Write-FioArtifacts {
    param(
        [string]$SummaryJsonPath,
        [string]$SummaryCsvPath,
        [string]$SummaryHtmlPath
    )

    Write-FioProperty -Name 'Summary JSON' -Value $SummaryJsonPath
    Write-FioProperty -Name 'Summary CSV' -Value $SummaryCsvPath
    Write-FioProperty -Name 'Summary HTML' -Value $SummaryHtmlPath
}

function Write-FioPreparedFileCheck {
    param(
        [pscustomobject]$Validation
    )

    Write-Host 'Prepared file verification' -ForegroundColor Cyan
    foreach ($file in $Validation.Files) {
        $status = if ($file.IsValid) { 'OK' } else { 'WARN' }
        $color = if ($file.IsValid) { 'Green' } else { 'Yellow' }
        Write-Host ("  [{0}] {1}" -f $status, $file.Path) -ForegroundColor $color
        Write-FioProperty -Name 'Expected bytes' -Value $file.ExpectedLength
        Write-FioProperty -Name 'Actual bytes' -Value $file.Length
    }
}

function Test-FioPreparationRequired {
    param(
        [pscustomobject]$Settings
    )

    if ($Settings.ReadWrite -eq 'write' -and ($null -eq $Settings.ReadMix -or $Settings.ReadMix -eq 0)) {
        return $false
    }

    return $true
}

function Get-FioPreparationCacheKey {
    param(
        [string]$Profile,
        [string]$TargetType,
        [pscustomobject]$Settings
    )

    $seed = [ordered]@{
        Profile = $Profile
        TargetType = $TargetType
        FileSizeGB = $Settings.FileSizeGB
        BlockSize = $Settings.BlockSize
        QueueDepth = $Settings.QueueDepth
        NumJobs = $Settings.NumJobs
        Direct = $Settings.Direct
        ReadWrite = $Settings.ReadWrite
        ReadMix = $Settings.ReadMix
        Fsync = $Settings.Fsync
    } | ConvertTo-Json -Compress

    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($seed)
        $hashBytes = $sha.ComputeHash($bytes)
        return -join ($hashBytes | ForEach-Object { $_.ToString('x2') } | Select-Object -First 8)
    }
    finally {
        $sha.Dispose()
    }
}

function New-FioExecutionRunContext {
    param(
        [pscustomobject]$RunContext,
        [string]$TargetRunDirectory
    )

    [pscustomobject]@{
        RunId = $RunContext.RunId
        ResultDirectory = $RunContext.ResultDirectory
        TargetRunDirectory = $TargetRunDirectory
        TimestampUtc = $RunContext.TimestampUtc
    }
}

$modulePath = Join-Path -Path $PSScriptRoot -ChildPath '..\src\FioSqlBench\FioSqlBench.psm1'
Import-Module -Name $modulePath -Force

if ($Help) {
    Get-Help -Full $MyInvocation.MyCommand.Path
    return
}

$resolvedTarget = Resolve-FioSqlBenchTarget -TargetPath $TargetPath -TargetType $TargetType
$profileDefaults = Get-FioSqlBenchProfileDefaults -Profile $Profile
$Profile = [string]$profileDefaults.ProfileName

$effectiveSettings = Merge-FioSqlBenchSettings `
    -TargetInfo $resolvedTarget `
    -ProfileDefaults $profileDefaults `
    -FileSizeGB $FileSizeGB `
    -RuntimeSec $RuntimeSec `
    -RampSec $RampSec `
    -Iterations $Iterations `
    -QueueDepth $QueueDepth `
    -NumJobs $NumJobs `
    -BlockSize $BlockSize `
    -ReadMix $ReadMix `
    -Fsync $Fsync `
    -Direct $Direct

$runContext = New-FioSqlBenchRunContext `
    -TargetInfo $resolvedTarget `
    -OutputRoot $OutputRoot `
    -RunLabel $RunLabel `
    -Profile $Profile `
    -Settings $effectiveSettings

$physicalMemoryBytes = Get-FioPhysicalMemoryBytes
$cacheAssessment = Get-FioCacheBypassAssessment -TargetType $resolvedTarget.Type -Settings $effectiveSettings -PhysicalMemoryBytes $physicalMemoryBytes

$targetWorkDirectory = $runContext.TargetRunDirectory
$preparationCacheKey = $null
if ($ReusePreparedFiles) {
    $preparationCacheKey = Get-FioPreparationCacheKey -Profile $Profile -TargetType $resolvedTarget.Type -Settings $effectiveSettings
    $targetWorkDirectory = Join-Path -Path $resolvedTarget.Path -ChildPath (Join-Path -Path '.fio-sql-bench-cache' -ChildPath ('{0}-{1}' -f $Profile.ToLowerInvariant(), $preparationCacheKey))
}

$executionRunContext = New-FioExecutionRunContext -RunContext $runContext -TargetRunDirectory $targetWorkDirectory

$prepJobContent = New-FioSqlBenchJobContent `
    -Settings $effectiveSettings `
    -RunContext $executionRunContext `
    -Phase Prep

$jobContent = New-FioSqlBenchJobContent `
    -Settings $effectiveSettings `
    -RunContext $executionRunContext `
    -Phase Bench `
    -EnableLogs:$EnableLogs

Write-FioConsoleBanner -Title 'fio SQL Bench' -Subtitle 'Windows-first fio harness for SQL-like storage workloads'
Write-FioStage -Title 'Resolved benchmark plan' -Status 'OK'
Write-FioSettingsBlock -TargetInfo $resolvedTarget -Settings $effectiveSettings -RunContext $runContext -Profile $Profile
Write-FioCacheAssessment -Assessment $cacheAssessment

if ($resolvedTarget.CreatedDirectory) {
    Write-FioStage -Title 'Created target directory for benchmark files' -Status 'OK'
    Write-FioProperty -Name 'Created path' -Value $resolvedTarget.Path
}

if ($DryRun) {
    Write-FioStage -Title 'Dry run complete: generated fio job without executing I/O' -Status 'OK'
    Write-FioProperty -Name 'Target work dir' -Value $executionRunContext.TargetRunDirectory
    if ($ReusePreparedFiles) {
        Write-FioProperty -Name 'Prep cache key' -Value $preparationCacheKey
    }
    Write-FioProperty -Name 'Prep job preview' -Value (Join-Path -Path $runContext.ResultDirectory -ChildPath 'fio-prep-job.fio')
    Write-FioProperty -Name 'Bench job preview' -Value (Join-Path -Path $runContext.ResultDirectory -ChildPath 'fio-job.fio')
    Write-Host ''
    Write-Host 'Generated fio prep job preview' -ForegroundColor Cyan
    Write-Host ('-' * 72) -ForegroundColor DarkCyan
    Write-Host $prepJobContent -ForegroundColor Gray
    Write-Host ('-' * 72) -ForegroundColor DarkCyan
    Write-Host ''
    Write-Host 'Generated fio benchmark job preview' -ForegroundColor Cyan
    Write-Host ('-' * 72) -ForegroundColor DarkCyan
    Write-Host $jobContent -ForegroundColor Gray
    Write-Host ('-' * 72) -ForegroundColor DarkCyan

    $dryRunResult = [pscustomobject]@{
        DryRun = $true
        RunId = $runContext.RunId
        Profile = $Profile
        TargetType = $resolvedTarget.Type
        TargetPath = $resolvedTarget.Path
        ResultDirectory = $runContext.ResultDirectory
        TargetRunDirectory = $runContext.TargetRunDirectory
        EffectiveSettings = $effectiveSettings
        PrepJobPreview = $prepJobContent
        JobPreview = $jobContent
    }
    if ($PassThru) {
        $dryRunResult
    }
    return
}

# fio discovery is separated so operators can override it explicitly while still
# getting a better-than-PATH search on Windows installations.
$fioBinary = Resolve-FioBinary -ExplicitPath $FioPath

Write-FioStage -Title 'Resolved fio executable' -Status 'OK'
Write-FioProperty -Name 'fio path' -Value $fioBinary.Path
Write-FioProperty -Name 'fio version' -Value $fioBinary.Version

New-Item -ItemType Directory -Path $runContext.ResultDirectory -Force | Out-Null
New-Item -ItemType Directory -Path $executionRunContext.TargetRunDirectory -Force | Out-Null

Write-FioStage -Title 'Prepared working directories' -Status 'OK'
Write-FioProperty -Name 'Results dir' -Value $runContext.ResultDirectory
Write-FioProperty -Name 'Target work dir' -Value $executionRunContext.TargetRunDirectory
if ($ReusePreparedFiles) {
    Write-FioProperty -Name 'Prep cache key' -Value $preparationCacheKey
}

$prepJobFilePath = Join-Path -Path $runContext.ResultDirectory -ChildPath 'fio-prep-job.fio'
$jobFilePath = Join-Path -Path $runContext.ResultDirectory -ChildPath 'fio-job.fio'
$prepJobContent | Set-Content -Path $prepJobFilePath -Encoding ascii
$jobContent | Set-Content -Path $jobFilePath -Encoding ascii

Write-FioStage -Title 'Wrote fio job files' -Status 'OK'
Write-FioProperty -Name 'Prep job file' -Value $prepJobFilePath
Write-FioProperty -Name 'Bench job file' -Value $jobFilePath

$iterationSummaries = New-Object System.Collections.Generic.List[object]
$runSucceeded = $false
$preparedFiles = Get-FioBenchFilePaths -Settings $effectiveSettings -RunContext $executionRunContext
$prepRequired = Test-FioPreparationRequired -Settings $effectiveSettings

try {
    if (-not $PSCmdlet.ShouldProcess($executionRunContext.TargetRunDirectory, "Run fio profile $Profile")) {
        return
    }

    if (-not $prepRequired) {
        Write-FioStage -Title 'Skipped benchmark file preparation for write-only workload' -Status 'OK'
    }
    else {
        $preparedValidation = Test-FioPreparedFiles -Paths $preparedFiles -ExpectedLengthBytes $effectiveSettings.FileSizePerJobBytes
        $canReusePreparedFiles = $ReusePreparedFiles -and $preparedValidation.AllValid

        if ($canReusePreparedFiles) {
            Write-FioStage -Title 'Reusing validated prepared benchmark files' -Status 'OK'
            Write-FioProperty -Name 'Prepared file set' -Value $executionRunContext.TargetRunDirectory
        }
        else {
            $prepJsonPath = Join-Path -Path $runContext.ResultDirectory -ChildPath 'iter-01-prep-fio.json'
            $prepConsolePath = Join-Path -Path $runContext.ResultDirectory -ChildPath 'iter-01-prep-console.log'

            Write-FioStage -Title 'Preparing benchmark files once before timed iterations' -Status 'RUN'
            Write-FioProperty -Name 'Prep JSON output' -Value $prepJsonPath
            Write-FioProperty -Name 'Prep console log' -Value $prepConsolePath

            Invoke-FioSqlBenchRun `
                -FioPath $fioBinary.Path `
                -JobFilePath $prepJobFilePath `
                -OutputJsonPath $prepJsonPath `
                -ConsoleLogPath $prepConsolePath | Out-Null

            $preparedValidation = Test-FioPreparedFiles -Paths $preparedFiles -ExpectedLengthBytes $effectiveSettings.FileSizePerJobBytes
            if (-not $preparedValidation.AllValid) {
                Write-FioStage -Title 'Prepared benchmark files did not reach the expected size' -Status 'WARN'
                Write-FioPreparedFileCheck -Validation $preparedValidation
                throw "Prepared benchmark files on '$($resolvedTarget.Path)' are smaller than expected. See $prepConsolePath for prep details."
            }
        }
    }

    for ($iteration = 1; $iteration -le $effectiveSettings.Iterations; $iteration++) {
        $iterationPrefix = 'iter-{0:D2}' -f $iteration
        $iterationJsonPath = Join-Path -Path $runContext.ResultDirectory -ChildPath ("$iterationPrefix-fio.json")
        $iterationConsolePath = Join-Path -Path $runContext.ResultDirectory -ChildPath ("$iterationPrefix-console.log")

        Write-FioStage -Title ("Running fio iteration {0} of {1}" -f $iteration, $effectiveSettings.Iterations) -Status 'RUN'
        Write-FioProperty -Name 'JSON output' -Value $iterationJsonPath
        Write-FioProperty -Name 'Console log' -Value $iterationConsolePath

        # Keep raw fio artifacts per iteration so failures are diagnosable without
        # re-running the benchmark.
        $execution = Invoke-FioSqlBenchRun `
            -FioPath $fioBinary.Path `
            -JobFilePath $jobFilePath `
            -OutputJsonPath $iterationJsonPath `
            -ConsoleLogPath $iterationConsolePath

        $summary = ConvertFrom-FioJsonToSummary `
            -JsonPath $iterationJsonPath `
            -RunContext $executionRunContext `
            -Settings $effectiveSettings `
            -Iteration $iteration `
            -FioVersion $fioBinary.Version `
            -TargetInfo $resolvedTarget

        $summaryPath = Join-Path -Path $runContext.ResultDirectory -ChildPath ("$iterationPrefix-summary.json")
        $summary | ConvertTo-Json -Depth 10 | Set-Content -Path $summaryPath -Encoding utf8
        $iterationSummaries.Add($summary)

        Write-FioStage -Title ("Completed iteration {0}" -f $iteration) -Status 'OK'
        Write-FioIterationSummary -Profile $Profile -TargetType $resolvedTarget.Type -Summary $summary
        Write-FioProperty -Name 'Iteration summary' -Value $summaryPath
    }

    $runSucceeded = $true
}
finally {
    if (-not $KeepJobFile -and $runSucceeded -and (Test-Path -Path $prepJobFilePath)) {
        Remove-Item -Path $prepJobFilePath -Force
    }

    if (-not $KeepJobFile -and $runSucceeded -and (Test-Path -Path $jobFilePath)) {
        Remove-Item -Path $jobFilePath -Force
    }

    if (-not $NoCleanup -and -not $ReusePreparedFiles -and $runSucceeded -and (Test-Path -Path $executionRunContext.TargetRunDirectory)) {
        Remove-Item -Path $executionRunContext.TargetRunDirectory -Recurse -Force
    }
}

if ($iterationSummaries.Count -eq 0) {
    throw 'No fio iterations were executed.'
}

$summaryJsonPath = Join-Path -Path $runContext.ResultDirectory -ChildPath 'summary.json'
$summaryCsvPath = Join-Path -Path $runContext.ResultDirectory -ChildPath 'summary.csv'
$summaryHtmlPath = Join-Path -Path $runContext.ResultDirectory -ChildPath 'summary.html'

$aggregate = [pscustomobject]@{
    RunId = $runContext.RunId
    Profile = $Profile
    TargetPath = $resolvedTarget.Path
    TargetType = $resolvedTarget.Type
    ResultDirectory = $runContext.ResultDirectory
    Iterations = $iterationSummaries
}

$aggregate | ConvertTo-Json -Depth 12 | Set-Content -Path $summaryJsonPath -Encoding utf8
Export-FioSqlBenchCsv -Summaries $iterationSummaries -Path $summaryCsvPath

$htmlRuns = Import-FioSqlBenchHistory -ResultsRoot $runContext.ResultDirectory
Export-FioSqlBenchHtmlReport -Runs $htmlRuns -Path $summaryHtmlPath -Title ("fio SQL Bench Run Report - {0}" -f $runContext.RunId) -ResultsRoot $runContext.ResultDirectory

Write-FioStage -Title 'Benchmark finished successfully' -Status 'OK'
Write-FioArtifacts -SummaryJsonPath $summaryJsonPath -SummaryCsvPath $summaryCsvPath -SummaryHtmlPath $summaryHtmlPath
Write-FioProperty -Name 'Iterations' -Value $iterationSummaries.Count
Write-FioRollupTable -Summaries $iterationSummaries

if ($iterationSummaries.Count -gt 1) {
    $aggregateRender = Get-FioAggregateSummary `
        -RunId $runContext.RunId `
        -Profile $Profile `
        -TargetPath $resolvedTarget.Path `
        -TargetType $resolvedTarget.Type `
        -ResultDirectory $runContext.ResultDirectory `
        -Summaries $iterationSummaries

    Write-FioStage -Title 'Console rendering of aggregated results' -Status 'OK'
    Write-FioPerformanceTable -Profile $Profile -TargetType $resolvedTarget.Type -Summary $aggregateRender
    Write-FioSqlInterpretation -Profile $Profile -TargetType $resolvedTarget.Type -Summary $aggregateRender
    Write-FioRecommendations -Profile $Profile -TargetType $resolvedTarget.Type -Summary $aggregateRender
}

if ($PassThru) {
    $aggregate
}
