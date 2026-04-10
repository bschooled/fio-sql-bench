<#
.SYNOPSIS
Runs SQL-oriented fio benchmarks against a local directory or SMB share.

.DESCRIPTION
This script builds a safe, file-based fio job for one of the built-in SQL-like
profiles (`Data`, `Log`, `Tempdb`, `BackupRestore`, `DbccScan`, `MaxThroughput`, `MaxIOPs`, or `All`), executes it,
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
 `BackupRestore` models large-block sequential transfer, `DbccScan` models
 large-block scan-heavy reads, `MaxThroughput` runs separate large-block
 sequential read and write phases with enough concurrency to chase the highest
 sustained path throughput in each direction,
 `MaxIOPs` favors small-block random I/O with aggressive concurrency and runs
 isolated `randread` and `randwrite` phases against the same prepared files to
 chase peak read and write IOPS separately, and `All` runs the built-in profile
 set in an efficiency-oriented sequence that reuses compatible prepared files
 where possible.

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

.PARAMETER ThroughputCapMBps
Optional aggregate throughput cap in MB/s. The runner converts this into per-job fio `rate` pacing so the workload does not intentionally exceed the requested sustained bandwidth.

.PARAMETER IopsCap
Optional aggregate IOPS cap. The runner converts this into per-job fio `rate_iops` pacing so the workload does not intentionally exceed the requested sustained IOPS.

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
Adds fio bandwidth, IOPS, and latency log files to the result set so diagnostics can chart throughput stability and transient stalls. On SMB targets, the runner also attempts to capture SMB client perf counters for queueing, credit stalls, and client-observed latency.

.PARAMETER Optimized
For SMB targets, captures the current SMB client configuration, previews or applies a benchmark-oriented SMB client tuning pass, and restores the original settings when the run finishes. If changes require elevation, the script relaunches itself in an elevated PowerShell session for the active run.

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

.EXAMPLE
./scripts/Invoke-FioSqlBench.ps1 -TargetPath 'D:\SqlBench' -Profile MaxThroughput -DryRun

.EXAMPLE
./scripts/Invoke-FioSqlBench.ps1 -TargetPath '\\fileserver\sqlbench' -Profile MaxThroughput -ThroughputCapMBps 250

.EXAMPLE
./scripts/Invoke-FioSqlBench.ps1 -TargetPath 'D:\SqlBench' -Profile Data -IopsCap 30000

.EXAMPLE
./scripts/Invoke-FioSqlBench.ps1 -TargetPath '\\fileserver\sqlbench' -Profile MaxThroughput -Optimized

.EXAMPLE
./scripts/Invoke-FioSqlBench.ps1 -TargetPath 'D:\SqlBench' -Profile MaxIOPs -DryRun

.EXAMPLE
./scripts/Invoke-FioSqlBench.ps1 -TargetPath 'D:\SqlBench' -Profile All -DryRun
#>
[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSAvoidAssignmentToAutomaticVariable', 'Profile', Justification = 'The public CLI intentionally uses -Profile for backward compatibility.')]
[CmdletBinding(SupportsShouldProcess, DefaultParameterSetName = 'Run')]
param(
    [Parameter(Mandatory, ParameterSetName = 'Run')]
    [string]$TargetPath,

    [Parameter(ParameterSetName = 'Run')]
    [ValidateSet('Auto', 'Local', 'Smb')]
    [string]$TargetType = 'Auto',

    [Parameter(ParameterSetName = 'Run')]
    [ValidateSet('Data', 'Log', 'Tempdb', 'BackupRestore', 'DbccScan', 'MaxThroughput', 'MaxIOPs', 'All')]
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
    [Nullable[decimal]]$ThroughputCapMBps,

    [Parameter(ParameterSetName = 'Run')]
    [Nullable[int]]$IopsCap,

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
    [switch]$Optimized,
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
    [Parameter(ParameterSetName = 'Run')]
    [string]$PreparationCacheGroup,

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

function Move-FioIterationDiagnosticLogs {
    param(
        [Parameter(Mandatory)]
        [string]$ResultDirectory,

        [Parameter(Mandatory)]
        [string]$IterationPrefix
    )

    $movedPaths = New-Object System.Collections.Generic.List[string]
    foreach ($logFile in @(Get-ChildItem -LiteralPath $ResultDirectory -Filter 'fio*.log' -File -ErrorAction SilentlyContinue)) {
        $destinationPath = Join-Path -Path $ResultDirectory -ChildPath ("{0}-{1}" -f $IterationPrefix, $logFile.Name)
        Move-Item -LiteralPath $logFile.FullName -Destination $destinationPath -Force
        $movedPaths.Add($destinationPath)
    }

    @($movedPaths)
}

function Get-FioSmbPerfmonCollectorSpec {
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$TargetInfo
    )

    if ($TargetInfo.Type -ne 'Smb' -or $null -eq $TargetInfo.SmbMetadata) {
        return $null
    }

    try {
        $listSet = Get-Counter -ListSet 'SMB Client Shares' -ErrorAction Stop
    }
    catch {
        return $null
    }

    $availableCounters = @($listSet.Counter)
    $definitions = @(
        [pscustomobject]@{ CounterPath = '\SMB Client Shares(*)\Read Bytes/sec'; CounterName = 'Read Bytes/sec'; Metric = 'perfmonBandwidth'; Direction = 'Read'; Unit = 'MB/s'; Divisor = [double]1MB },
        [pscustomobject]@{ CounterPath = '\SMB Client Shares(*)\Write Bytes/sec'; CounterName = 'Write Bytes/sec'; Metric = 'perfmonBandwidth'; Direction = 'Write'; Unit = 'MB/s'; Divisor = [double]1MB },
        [pscustomobject]@{ CounterPath = '\SMB Client Shares(*)\Avg. sec/Read'; CounterName = 'Avg. sec/Read'; Metric = 'perfmonLatency'; Direction = 'Read'; Unit = 'ms'; Divisor = 0.001 },
        [pscustomobject]@{ CounterPath = '\SMB Client Shares(*)\Avg. sec/Write'; CounterName = 'Avg. sec/Write'; Metric = 'perfmonLatency'; Direction = 'Write'; Unit = 'ms'; Divisor = 0.001 },
        [pscustomobject]@{ CounterPath = '\SMB Client Shares(*)\Current Data Queue Length'; CounterName = 'Current Data Queue Length'; Metric = 'perfmonQueue'; Direction = 'Client'; Unit = 'queue depth'; Divisor = 1.0 },
        [pscustomobject]@{ CounterPath = '\SMB Client Shares(*)\Credit Stalls/sec'; CounterName = 'Credit Stalls/sec'; Metric = 'perfmonCreditStalls'; Direction = 'Client'; Unit = 'stalls/sec'; Divisor = 1.0 }
    )

    $enabledDefinitions = @(
        foreach ($definition in $definitions) {
            if ($availableCounters -contains $definition.CounterPath) {
                $definition
            }
        }
    )

    if ($enabledDefinitions.Count -eq 0) {
        return $null
    }

    [pscustomobject]@{
        ShareName = if ($TargetInfo.SmbMetadata.PSObject.Properties['ShareName']) { [string]$TargetInfo.SmbMetadata.ShareName } else { $null }
        ServerName = if ($TargetInfo.SmbMetadata.PSObject.Properties['ServerName']) { [string]$TargetInfo.SmbMetadata.ServerName } else { $null }
        Definitions = $enabledDefinitions
        SampleIntervalMs = 1000
    }
}

function Start-FioSmbPerfmonCapture {
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$CollectorSpec,

        [Parameter(Mandatory)]
        [string]$OutputPath
    )

    $stopPath = "$OutputPath.stop"
    if (Test-Path -LiteralPath $stopPath) {
        Remove-Item -LiteralPath $stopPath -Force -ErrorAction SilentlyContinue
    }

    $job = Start-Job -ScriptBlock {
        param($Definitions, $OutputPath, $StopPath, $ShareName, $ServerName, $SampleIntervalMs)

        Set-StrictMode -Version Latest
        $ErrorActionPreference = 'Stop'
        'TimeMs,Metric,Direction,Unit,Instance,Value' | Set-Content -Path $OutputPath -Encoding utf8
        $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
        $shareToken = if ([string]::IsNullOrWhiteSpace([string]$ShareName)) { $null } else { ([string]$ShareName).ToLowerInvariant() }
        $serverToken = if ([string]::IsNullOrWhiteSpace([string]$ServerName)) { $null } else { ([string]$ServerName).ToLowerInvariant() }

        while (-not (Test-Path -LiteralPath $StopPath)) {
            try {
                $counterPaths = @($Definitions | ForEach-Object { [string]$_.CounterPath } | Select-Object -Unique)
                $sample = Get-Counter -Counter $counterPaths -ErrorAction Stop
                $rows = New-Object System.Collections.Generic.List[string]
                foreach ($counterSample in @($sample.CounterSamples)) {
                    $definition = @($Definitions | Where-Object { $counterSample.Path -like ('*\' + $_.CounterName) } | Select-Object -First 1)
                    if ($definition.Count -eq 0) {
                        continue
                    }

                    $instanceName = if ($counterSample.PSObject.Properties['InstanceName']) { [string]$counterSample.InstanceName } else { [string]$counterSample.Path }
                    $instanceToken = $instanceName.ToLowerInvariant()
                    $instanceMatches = $true
                    if (-not [string]::IsNullOrWhiteSpace($shareToken)) {
                        $instanceMatches = $instanceToken.Contains($shareToken)
                    }
                    if (-not $instanceMatches -and -not [string]::IsNullOrWhiteSpace($serverToken)) {
                        $instanceMatches = $instanceToken.Contains($serverToken)
                    }
                    if (-not $instanceMatches) {
                        continue
                    }

                    $value = [double]$counterSample.CookedValue
                    if ($definition.Divisor -ne 1.0) {
                        $value = $value / [double]$definition.Divisor
                    }

                    $rows.Add(('{0},{1},{2},{3},"{4}",{5}' -f [int]$stopwatch.ElapsedMilliseconds, $definition.Metric, $definition.Direction, $definition.Unit, ($instanceName -replace '"', '""'), [string]::Format([System.Globalization.CultureInfo]::InvariantCulture, '{0:0.####}', $value)))
                }

                if ($rows.Count -gt 0) {
                    Add-Content -Path $OutputPath -Value $rows -Encoding utf8
                }
            }
            catch {
            }

            Start-Sleep -Milliseconds $SampleIntervalMs
        }
    } -ArgumentList @($CollectorSpec.Definitions, $OutputPath, $stopPath, $CollectorSpec.ShareName, $CollectorSpec.ServerName, $CollectorSpec.SampleIntervalMs)

    [pscustomobject]@{
        Job = $job
        OutputPath = $OutputPath
        StopPath = $stopPath
    }
}

function Stop-FioSmbPerfmonCapture {
    param(
        [AllowNull()]
        [pscustomobject]$Capture
    )

    if ($null -eq $Capture) {
        return
    }

    if (-not [string]::IsNullOrWhiteSpace([string]$Capture.StopPath)) {
        New-Item -ItemType File -Path $Capture.StopPath -Force | Out-Null
    }

    if ($Capture.PSObject.Properties['Job'] -and $null -ne $Capture.Job) {
        try {
            Wait-Job -Job $Capture.Job -Timeout 5 | Out-Null
        }
        catch {
        }

        try {
            if ($Capture.Job.State -eq 'Running') {
                Stop-Job -Job $Capture.Job -Force -ErrorAction SilentlyContinue
            }
        }
        catch {
        }

        try {
            Remove-Job -Job $Capture.Job -Force -ErrorAction SilentlyContinue
        }
        catch {
        }
    }

    if (-not [string]::IsNullOrWhiteSpace([string]$Capture.StopPath) -and (Test-Path -LiteralPath $Capture.StopPath)) {
        Remove-Item -LiteralPath $Capture.StopPath -Force -ErrorAction SilentlyContinue
    }
}

function Test-FioAdministrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function ConvertTo-FioPowerShellArgumentLiteral {
    param(
        [AllowNull()]
        [object]$Value
    )

    if ($null -eq $Value) {
        return "''"
    }

    return ("'{0}'" -f ([string]$Value).Replace("'", "''"))
}

function Get-FioSelfRelaunchArgumentLine {
    param(
        [Parameter(Mandatory)]
        [string]$ScriptPath,

        [Parameter(Mandatory)]
        [System.Collections.IDictionary]$BoundParameters
    )

    $arguments = New-Object System.Collections.Generic.List[string]
    $arguments.Add('-NoProfile')
    $arguments.Add('-ExecutionPolicy')
    $arguments.Add('Bypass')
    $arguments.Add('-File')
    $arguments.Add((ConvertTo-FioPowerShellArgumentLiteral -Value $ScriptPath))

    foreach ($entry in $BoundParameters.GetEnumerator()) {
        if ($entry.Value -is [switch] -or $entry.Value -is [System.Management.Automation.SwitchParameter]) {
            if ([bool]$entry.Value) {
                $arguments.Add("-$($entry.Key)")
            }

            continue
        }

        $arguments.Add("-$($entry.Key)")
        $arguments.Add((ConvertTo-FioPowerShellArgumentLiteral -Value $entry.Value))
    }

    return ($arguments -join ' ')
}

function Start-FioElevatedSelf {
    param(
        [Parameter(Mandatory)]
        [string]$ScriptPath,

        [Parameter(Mandatory)]
        [System.Collections.IDictionary]$BoundParameters
    )

    $hostPath = (Get-Process -Id $PID).Path
    $argumentLine = Get-FioSelfRelaunchArgumentLine -ScriptPath $ScriptPath -BoundParameters $BoundParameters

    try {
        $process = Start-Process -FilePath $hostPath -ArgumentList $argumentLine -Verb RunAs -Wait -PassThru -ErrorAction Stop
    }
    catch {
        throw 'SMB optimization requires elevation and the elevated PowerShell process could not be started.'
    }

    if ($null -ne $process) {
        exit $process.ExitCode
    }

    exit 0
}

function Get-FioSmbOptimizationSnapshot {
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$TargetInfo
    )

    $targetRemoteRoot = "\\$($TargetInfo.SmbMetadata.ServerName)\$($TargetInfo.SmbMetadata.ShareName)"
    $clientConfigRaw = Get-SmbClientConfiguration
    $clientConfiguration = [pscustomobject]@{
        EnableMultiChannel = [bool]$clientConfigRaw.EnableMultiChannel
        EnableLargeMtu = [bool]$clientConfigRaw.EnableLargeMtu
        ConnectionCountPerRssNetworkInterface = [int]$clientConfigRaw.ConnectionCountPerRssNetworkInterface
        MaximumConnectionCountPerServer = [int]$clientConfigRaw.MaximumConnectionCountPerServer
        EnableBandwidthThrottling = [bool]$clientConfigRaw.EnableBandwidthThrottling
    }

    $clientInterfaces = @()
    try {
        $clientInterfaces = @(
            Get-SmbClientNetworkInterface -ErrorAction Stop |
                Select-Object InterfaceIndex, InterfaceAlias, IpAddress, RssCapable, LinkSpeed
        )
    }
    catch {
        $clientInterfaces = @()
    }

    $multichannelConnections = @()
    try {
        $multichannelConnections = @(
            Get-SmbMultichannelConnection -ErrorAction Stop |
                Where-Object { $_.ServerName -eq $TargetInfo.SmbMetadata.ServerName } |
                Select-Object ServerName, ClientIpAddress, ServerIpAddress, ClientInterfaceIndex, ServerInterfaceIndex, Selected, ClientRdmaCapable, ServerRdmaCapable
        )
    }
    catch {
        $multichannelConnections = @()
    }

    $mappings = @()
    try {
        $mappings = @(
            Get-SmbMapping -ErrorAction Stop |
                Where-Object {
                    ([string]$_.RemotePath).TrimEnd('\\') -eq $targetRemoteRoot.TrimEnd('\\')
                } |
                Select-Object LocalPath, RemotePath, Status, UserName, Persistent
        )
    }
    catch {
        $mappings = @()
    }

    $rssAdapters = @()
    try {
        foreach ($adapter in @(Get-NetAdapter -ErrorAction Stop | Where-Object { $_.Status -eq 'Up' -and $_.HardwareInterface })) {
            try {
                $rss = Get-NetAdapterRss -Name $adapter.Name -ErrorAction Stop
                if ($null -ne $rss) {
                    $rssAdapters += [pscustomobject]@{
                        Name = [string]$adapter.Name
                        Enabled = [bool]$rss.Enabled
                        NumberOfReceiveQueues = [int]$rss.NumberOfReceiveQueues
                        Profile = [string]$rss.Profile
                    }
                }
            }
            catch {
            }
        }
    }
    catch {
        $rssAdapters = @()
    }

    return [pscustomobject]@{
        TimestampUtc = [DateTime]::UtcNow.ToString('o')
        TargetRemoteRoot = $targetRemoteRoot
        ClientConfiguration = $clientConfiguration
        ClientInterfaces = $clientInterfaces
        MultichannelConnections = $multichannelConnections
        Mappings = $mappings
        RssAdapters = $rssAdapters
    }
}

function Export-FioSmbOptimizationState {
    param(
        [AllowNull()]
        [pscustomobject]$State
    )

    if ($null -eq $State -or [string]::IsNullOrWhiteSpace([string]$State.ReportPath)) {
        return
    }

    $reportDirectory = Split-Path -Path $State.ReportPath -Parent
    if (-not [string]::IsNullOrWhiteSpace($reportDirectory) -and -not (Test-Path -LiteralPath $reportDirectory)) {
        New-Item -ItemType Directory -Path $reportDirectory -Force | Out-Null
    }

    $State | ConvertTo-Json -Depth 12 | Set-Content -Path $State.ReportPath -Encoding utf8
}

function Write-FioSmbOptimizationSummary {
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$State,

        [string]$Title = 'SMB optimization state'
    )

    $status = if ($State.PreviewOnly -or $State.Applied -or @($State.Changes).Count -eq 0) { 'OK' } else { 'WARN' }
    Write-FioStage -Title $Title -Status $status
    Write-FioProperty -Name 'Remote share' -Value $State.TargetRemoteRoot
    Write-FioProperty -Name 'Preview only' -Value $(if ($State.PreviewOnly) { 'Yes' } else { 'No' })
    Write-FioProperty -Name 'Applied' -Value $(if ($State.Applied) { 'Yes' } else { 'No' })
    Write-FioProperty -Name 'Report path' -Value $State.ReportPath
    Write-FioProperty -Name 'Initial channels' -Value @($State.Initial.MultichannelConnections | Where-Object { $null -eq $_.Selected -or [bool]$_.Selected }).Count
    if ($null -ne $State.Final) {
        Write-FioProperty -Name 'Current channels' -Value @($State.Final.MultichannelConnections | Where-Object { $null -eq $_.Selected -or [bool]$_.Selected }).Count
    }

    foreach ($change in @($State.Changes)) {
        Write-Host ("  - {0}" -f $change) -ForegroundColor DarkGray
    }

    foreach ($note in @($State.Messages)) {
        Write-Host ("  - {0}" -f $note) -ForegroundColor Yellow
    }
}

function Enable-FioOptimizedSmbClientState {
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$TargetInfo,

        [Parameter(Mandatory)]
        [string]$ResultDirectory,

        [switch]$DryRun
    )

    if ($TargetInfo.Type -ne 'Smb' -or $null -eq $TargetInfo.SmbMetadata) {
        throw '-Optimized is only supported for SMB targets.'
    }

    $reportPath = Join-Path -Path $ResultDirectory -ChildPath 'smb-optimization.json'
    $initialSnapshot = Get-FioSmbOptimizationSnapshot -TargetInfo $TargetInfo
    $changes = New-Object System.Collections.Generic.List[string]
    $messages = New-Object System.Collections.Generic.List[string]
    $changedRssAdapters = New-Object System.Collections.Generic.List[string]

    $state = [pscustomobject]@{
        Enabled = $true
        PreviewOnly = [bool]$DryRun
        Applied = $false
        TargetRemoteRoot = $initialSnapshot.TargetRemoteRoot
        ReportPath = $reportPath
        TargetInfo = [pscustomobject]@{
            Type = 'Smb'
            SmbMetadata = [pscustomobject]@{
                ServerName = $TargetInfo.SmbMetadata.ServerName
                ShareName = $TargetInfo.SmbMetadata.ShareName
            }
        }
        Initial = $initialSnapshot
        Final = $null
        Restored = $null
        Changes = $changes
        Messages = $messages
        ChangedRssAdapters = $changedRssAdapters
    }

    $desiredConnectionCount = [int][math]::Max($initialSnapshot.ClientConfiguration.ConnectionCountPerRssNetworkInterface, 4)
    $desiredServerConnectionCount = [int][math]::Max($initialSnapshot.ClientConfiguration.MaximumConnectionCountPerServer, 32)

    if (-not $initialSnapshot.ClientConfiguration.EnableMultiChannel) {
        $changes.Add('Enable SMB Multichannel.')
        if (-not $DryRun) {
            Set-SmbClientConfiguration -EnableMultiChannel $true -Force -Confirm:$false | Out-Null
            $state.Applied = $true
        }
    }

    if (-not $initialSnapshot.ClientConfiguration.EnableLargeMtu) {
        $changes.Add('Enable SMB Large MTU.')
        if (-not $DryRun) {
            Set-SmbClientConfiguration -EnableLargeMtu $true -Force -Confirm:$false | Out-Null
            $state.Applied = $true
        }
    }

    if ($initialSnapshot.ClientConfiguration.ConnectionCountPerRssNetworkInterface -lt $desiredConnectionCount) {
        $changes.Add(('Increase ConnectionCountPerRssNetworkInterface from {0} to {1}.' -f $initialSnapshot.ClientConfiguration.ConnectionCountPerRssNetworkInterface, $desiredConnectionCount))
        if (-not $DryRun) {
            Set-SmbClientConfiguration -ConnectionCountPerRssNetworkInterface $desiredConnectionCount -Force -Confirm:$false | Out-Null
            $state.Applied = $true
        }
    }

    if ($initialSnapshot.ClientConfiguration.MaximumConnectionCountPerServer -lt $desiredServerConnectionCount) {
        $changes.Add(('Increase MaximumConnectionCountPerServer from {0} to {1}.' -f $initialSnapshot.ClientConfiguration.MaximumConnectionCountPerServer, $desiredServerConnectionCount))
        if (-not $DryRun) {
            Set-SmbClientConfiguration -MaximumConnectionCountPerServer $desiredServerConnectionCount -Force -Confirm:$false | Out-Null
            $state.Applied = $true
        }
    }

    foreach ($rssAdapter in @($initialSnapshot.RssAdapters)) {
        if (-not [bool]$rssAdapter.Enabled) {
            $changes.Add(('Enable RSS on adapter {0}.' -f $rssAdapter.Name))
            if (-not $DryRun) {
                Enable-NetAdapterRss -Name $rssAdapter.Name -Confirm:$false -ErrorAction Stop | Out-Null
                $changedRssAdapters.Add([string]$rssAdapter.Name)
                $state.Applied = $true
            }
        }
    }

    $selectedConnectionCount = @($initialSnapshot.MultichannelConnections | Where-Object { $null -eq $_.Selected -or [bool]$_.Selected }).Count
    if ($selectedConnectionCount -le 1) {
        $messages.Add('One or fewer active SMB channels are currently visible. Existing SMB sessions may need to reconnect before new client settings change the channel count.')
    }

    if ($selectedConnectionCount -le 1 -and $initialSnapshot.ClientInterfaces.Count -eq 1 -and @($initialSnapshot.ClientInterfaces | Where-Object { [bool]$_.RssCapable }).Count -ge 1) {
        $messages.Add('This client currently exposes one RSS-capable SMB interface to a single ANF storage endpoint. That does not automatically mean the SMB client is misconfigured; interpret channel count together with achieved throughput and client-side queue or latency growth.')
    }

    if ($initialSnapshot.RssAdapters.Count -eq 0) {
        $messages.Add('No up hardware adapters with queryable RSS state were found. The optimization pass can still adjust SMB client settings, but multichannel scaling may remain limited by the network path.')
    }

    if ($changes.Count -eq 0) {
        $messages.Add('The current SMB client settings already meet the built-in optimization baseline.')
    }

    if ($DryRun) {
        Export-FioSmbOptimizationState -State $state
        return $state
    }

    $state.Final = Get-FioSmbOptimizationSnapshot -TargetInfo $TargetInfo
    Export-FioSmbOptimizationState -State $state
    return $state
}

function Restore-FioOptimizedSmbClientState {
    param(
        [AllowNull()]
        [pscustomobject]$State
    )

    if ($null -eq $State -or $State.PreviewOnly -or $null -eq $State.Initial) {
        return
    }

    try {
        Set-SmbClientConfiguration `
            -EnableMultiChannel ([bool]$State.Initial.ClientConfiguration.EnableMultiChannel) `
            -EnableLargeMtu ([bool]$State.Initial.ClientConfiguration.EnableLargeMtu) `
            -ConnectionCountPerRssNetworkInterface ([int]$State.Initial.ClientConfiguration.ConnectionCountPerRssNetworkInterface) `
            -MaximumConnectionCountPerServer ([int]$State.Initial.ClientConfiguration.MaximumConnectionCountPerServer) `
            -Force `
            -Confirm:$false | Out-Null
    }
    catch {
        $State.Messages.Add('Failed to restore the original SMB client configuration automatically. Review smb-optimization.json for the captured baseline.')
    }

    foreach ($adapterName in @($State.ChangedRssAdapters)) {
        try {
            Disable-NetAdapterRss -Name $adapterName -Confirm:$false -ErrorAction Stop | Out-Null
        }
        catch {
            $State.Messages.Add(('Failed to disable RSS again on adapter {0}. Review the captured optimization state.' -f $adapterName))
        }
    }

    $State.Restored = Get-FioSmbOptimizationSnapshot -TargetInfo $State.TargetInfo
    Export-FioSmbOptimizationState -State $State
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
    $status = Get-FioGenericLatencyAssessment -LatencyMs $MeanLatencyMs

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
        if ($WorkloadProfile -eq 'Log' -and $Operation -eq 'Write' -and $P999LatencyMs -gt 25) {
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
        [string]$WorkloadProfile,
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
    $assessment = Get-FioSqlProfileAssessment -WorkloadProfile $WorkloadProfile -TargetType $TargetType -Operation $Operation -MeanLatencyMs $meanMs -P99LatencyMs $p99Ms -P999LatencyMs $p999Ms -WorstP99LatencyMs $worstP99Ms -Direct $Direct

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

function Get-FioRenderableOperationRows {
    param(
        [string]$WorkloadProfile,
        [string]$TargetType,
        [pscustomobject]$Summary
    )

    $rows = @(
        Get-FioOperationRenderModel -WorkloadProfile $WorkloadProfile -TargetType $TargetType -Operation 'Read' -Stats $Summary.Read -Direct $Summary.Direct
        Get-FioOperationRenderModel -WorkloadProfile $WorkloadProfile -TargetType $TargetType -Operation 'Write' -Stats $Summary.Write -Direct $Summary.Direct
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

function Write-FioPerformanceTable {
    param(
        [string]$WorkloadProfile,
        [string]$TargetType,
        [pscustomobject]$Summary
    )

    $rows = @(Get-FioRenderableOperationRows -WorkloadProfile $WorkloadProfile -TargetType $TargetType -Summary $Summary)

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
        [string]$WorkloadProfile,
        [string]$TargetType,
        [pscustomobject]$Summary
    )

    $rows = @(Get-FioRenderableOperationRows -WorkloadProfile $WorkloadProfile -TargetType $TargetType -Summary $Summary)

    Write-Host 'SQL latency interpretation' -ForegroundColor Cyan
    Write-FioProperty -Name 'Microsoft rule' -Value 'Sustained 10-15 ms usually warrants SQL I/O investigation.'
    switch ($WorkloadProfile) {
        'MaxIOPs' {
            if ($TargetType -eq 'Smb') {
                Write-FioProperty -Name 'Profile target' -Value 'MaxIOPs is a best-case random-I/O profile. It runs isolated 4K randread and randwrite phases over the same files so read and write IOPS can be compared without a mixed-workload split.'
            }
            else {
                Write-FioProperty -Name 'Profile target' -Value 'MaxIOPs is a best-case random-I/O profile. It runs isolated 4K randread and randwrite phases over the same files so peak read and write IOPS are measured separately under heavy queue depth.'
            }
        }
        'MaxThroughput' {
            if ($TargetType -eq 'Smb') {
                Write-FioProperty -Name 'Profile target' -Value 'MaxThroughput is a best-case bandwidth profile. It runs separate large-block sequential read and write phases so each direction can be compared against the SMB path limit.'
            }
            else {
                Write-FioProperty -Name 'Profile target' -Value 'MaxThroughput is a best-case bandwidth profile. It runs separate large-block sequential read and write phases so each direction can be measured near the path limit.'
            }
        }
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
        [string]$WorkloadProfile,
        [string]$TargetType,
        [pscustomobject]$Summary
    )

    $read = Get-FioOperationRenderModel -WorkloadProfile $WorkloadProfile -TargetType $TargetType -Operation 'Read' -Stats $Summary.Read -Direct $Summary.Direct
    $write = Get-FioOperationRenderModel -WorkloadProfile $WorkloadProfile -TargetType $TargetType -Operation 'Write' -Stats $Summary.Write -Direct $Summary.Direct

    $recommendations = New-Object System.Collections.Generic.List[object]

    switch ($WorkloadProfile) {
        'MaxIOPs' {
            $activeRows = @($read, $write | Where-Object { $_.TotalIos -gt 0 -or $_.Iops -gt 0 })
            $unstableRow = $activeRows | Where-Object {
                ($null -ne $_.IopsCvPercent -and $_.IopsCvPercent -gt 20) -or
                ($null -ne $_.P999Ms -and $_.P999Ms -gt 100)
            } | Select-Object -First 1

            if ($activeRows.Count -gt 0 -and $null -eq $unstableRow) {
                $recommendations.Add([pscustomobject]@{ Color = 'Green'; Text = 'Isolated small-block random read and write runs look stable at high concurrency. Compare the achieved read and write IOPS independently against the expected limits of the media, controller, and filesystem path.' })
            }
            else {
                $recommendations.Add([pscustomobject]@{ Color = 'Yellow'; Text = 'This isolated peak-IOPS profile shows either burstiness or deep tail stalls. Re-run longer and review queue depth, controller saturation, and cache effects before treating the read or write result as a peak-random-IO baseline.' })
            }
        }
        'MaxThroughput' {
            $activeRows = @($read, $write | Where-Object { $_.TotalIos -gt 0 -or $_.BandwidthMBps -gt 0 })
            $unstableRow = $activeRows | Where-Object {
                ($null -ne $_.BandwidthCvPercent -and $_.BandwidthCvPercent -gt 20) -or
                ($null -ne $_.P999Ms -and $_.P999Ms -gt 150)
            } | Select-Object -First 1

            if ($activeRows.Count -gt 0 -and $null -eq $unstableRow) {
                $recommendations.Add([pscustomobject]@{ Color = 'Green'; Text = 'Large-block sequential throughput looks stable under saturation. Compare the achieved MB/s against the expected limits of the media, controller, and network path.' })
            }
            else {
                $recommendations.Add([pscustomobject]@{ Color = 'Yellow'; Text = 'This saturation profile shows either burstiness or deep tail stalls. Re-run longer and check for link throttling, controller cache effects, or SMB path imbalance before treating the result as a peak-throughput baseline.' })
            }
        }
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
        [string]$WorkloadProfile,
        [string]$TargetType,
        [pscustomobject]$Summary
    )

    $recommendations = Get-FioProfileRecommendations -WorkloadProfile $WorkloadProfile -TargetType $TargetType -Summary $Summary
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
        [string]$WorkloadProfile,
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
        [string]$WorkloadProfile
    )

    Write-FioProperty -Name 'Profile' -Value $WorkloadProfile
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
    if ($null -ne $Settings.ThroughputCapMBps) {
        Write-FioProperty -Name 'Throughput cap' -Value ('{0:N2} MB/s' -f [decimal]$Settings.ThroughputCapMBps)
    }
    if ($null -ne $Settings.IopsCap) {
        Write-FioProperty -Name 'IOPS cap' -Value ('{0:N0}' -f [int]$Settings.IopsCap)
    }
}

function Write-FioIterationSummary {
    param(
        [string]$WorkloadProfile,
        [string]$TargetType,
        [pscustomobject]$Summary
    )

    Write-FioPerformanceTable -WorkloadProfile $WorkloadProfile -TargetType $TargetType -Summary $Summary
    Write-FioSqlInterpretation -WorkloadProfile $WorkloadProfile -TargetType $TargetType -Summary $Summary
    Write-FioRecommendations -WorkloadProfile $WorkloadProfile -TargetType $TargetType -Summary $Summary
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
        [string]$WorkloadProfile,
        [string]$TargetType,
        [pscustomobject]$Settings
    )

    $seed = [ordered]@{
        Profile = $WorkloadProfile
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

function Get-FioAllProfilePlan {
    param(
        [pscustomobject]$TargetInfo,
        [Nullable[decimal]]$FileSizeGB,
        [Nullable[int]]$RuntimeSec,
        [Nullable[int]]$RampSec,
        [Nullable[int]]$Iterations,
        [Nullable[int]]$QueueDepth,
        [Nullable[int]]$NumJobs,
        [string]$BlockSize,
        [Nullable[int]]$ReadMix,
        [Nullable[int]]$Fsync,
        [Nullable[decimal]]$ThroughputCapMBps,
        [Nullable[int]]$IopsCap,
        [string]$Direct
    )

    $orderedProfiles = @('MaxThroughput', 'Data', 'DbccScan', 'BackupRestore', 'MaxIOPs', 'Tempdb', 'Log')
    $leaders = New-Object System.Collections.Generic.List[object]
    $plan = New-Object System.Collections.Generic.List[object]
    $groupIndex = 0

    foreach ($profileName in $orderedProfiles) {
        $profileDefaults = Get-FioSqlBenchProfileDefaults -Profile $profileName
        $settings = Merge-FioSqlBenchSettings `
            -TargetInfo $TargetInfo `
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
            -ThroughputCapMBps $ThroughputCapMBps `
            -IopsCap $IopsCap `
            -Direct $Direct

        $prepRequired = Test-FioPreparationRequired -Settings $settings
        $assignedGroup = $null
        $isLeader = $false
        $leaderProfile = $null

        if ($prepRequired) {
            $candidateLeaders = New-Object System.Collections.Generic.List[object]
            foreach ($leader in $leaders) {
                if ($leader.Settings.NumJobs -ge $settings.NumJobs -and $leader.Settings.FileSizePerJobBytes -ge $settings.FileSizePerJobBytes) {
                    $candidateLeaders.Add($leader)
                }
            }

            $candidateLeaders = @($candidateLeaders | Sort-Object @{ Expression = { $_.Settings.FileSizePerJobBytes } }, @{ Expression = { $_.Settings.NumJobs } })

            if ($candidateLeaders.Count -gt 0) {
                $selectedLeader = $candidateLeaders[0]
                $assignedGroup = [string]$selectedLeader.PreparationCacheGroup
                $leaderProfile = [string]$selectedLeader.Profile
            }
            else {
                $groupIndex++
                $assignedGroup = 'all-prep-{0:D2}' -f $groupIndex
                $leaderProfile = $profileName
                $isLeader = $true
                $leaders.Add([pscustomobject]@{
                    Profile = $profileName
                    Settings = $settings
                    PreparationCacheGroup = $assignedGroup
                })
            }
        }

        $plan.Add([pscustomobject]@{
            Profile = $profileName
            Settings = $settings
            PreparationRequired = $prepRequired
            PreparationCacheGroup = $assignedGroup
            IsPreparationLeader = $isLeader
            PreparationLeaderProfile = $leaderProfile
        })
    }

    return @($plan.ToArray())
}

function Write-FioAllProfilePlan {
    param(
        [object[]]$Plan
    )

    Write-Host ''
    Write-Host 'All-profile execution plan' -ForegroundColor Cyan
    Write-Host ('-' * 110) -ForegroundColor DarkCyan
    Write-Host ('  {0,-15} {1,-12} {2,8} {3,14} {4,-18}' -f 'Profile', 'Prep mode', 'Jobs', 'File/job GB', 'Reuse source') -ForegroundColor DarkGray

    foreach ($item in $Plan) {
        $prepMode = if (-not $item.PreparationRequired) { 'No prep' } elseif ($item.IsPreparationLeader) { 'Prep leader' } else { 'Reuse prep' }
        $fileSizePerJobGb = [math]::Round(($item.Settings.FileSizePerJobBytes / 1GB), 2)
        $reuseSource = if (-not $item.PreparationRequired) { '-' } elseif ($item.IsPreparationLeader) { [string]$item.PreparationCacheGroup } else { '{0} via {1}' -f $item.PreparationLeaderProfile, $item.PreparationCacheGroup }
        Write-Host ('  {0,-15} {1,-12} {2,8} {3,14:N2} {4,-18}' -f $item.Profile, $prepMode, $item.Settings.NumJobs, $fileSizePerJobGb, $reuseSource) -ForegroundColor Gray
    }

    Write-Host ('-' * 110) -ForegroundColor DarkCyan
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

if ($Optimized -and $resolvedTarget.Type -ne 'Smb') {
    throw '-Optimized is only supported for SMB targets.'
}

if ($Profile -eq 'All') {
    $allPlan = Get-FioAllProfilePlan `
        -TargetInfo $resolvedTarget `
        -FileSizeGB $FileSizeGB `
        -RuntimeSec $RuntimeSec `
        -RampSec $RampSec `
        -Iterations $Iterations `
        -QueueDepth $QueueDepth `
        -NumJobs $NumJobs `
        -BlockSize $BlockSize `
        -ReadMix $ReadMix `
        -Fsync $Fsync `
            -ThroughputCapMBps $ThroughputCapMBps `
            -IopsCap $IopsCap `
        -Direct $Direct

    $allTimestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $allLabel = if ($RunLabel) { $RunLabel } else { 'all' }
    $allSanitizedLabel = ($allLabel -replace '[^a-zA-Z0-9_-]', '-')
    $allRunId = "$allTimestamp-$allSanitizedLabel"
    $allResultRoot = Join-Path -Path $OutputRoot -ChildPath $allRunId

    Write-FioConsoleBanner -Title 'fio SQL Bench' -Subtitle 'Windows-first fio harness for SQL-like storage workloads'
    Write-FioStage -Title 'Resolved all-profile benchmark plan' -Status 'OK'
    Write-FioProperty -Name 'Profile' -Value 'All'
    Write-FioProperty -Name 'Target path' -Value $resolvedTarget.Path
    Write-FioProperty -Name 'Target type' -Value $resolvedTarget.Type
    Write-FioProperty -Name 'Batch run id' -Value $allRunId
    Write-FioProperty -Name 'Batch results' -Value $allResultRoot
    Write-FioAllProfilePlan -Plan $allPlan

    $allOptimizationPreview = $null
    if ($Optimized) {
        $allOptimizationPreview = Enable-FioOptimizedSmbClientState -TargetInfo $resolvedTarget -ResultDirectory $allResultRoot -DryRun
        Write-FioSmbOptimizationSummary -State $allOptimizationPreview -Title 'SMB optimization preview'
    }

    if ($DryRun) {
        Write-FioStage -Title 'Dry run complete: built all-profile execution plan without executing I/O' -Status 'OK'
        $dryRunResult = [pscustomobject]@{
            DryRun = $true
            RunId = $allRunId
            Profile = 'All'
            TargetType = $resolvedTarget.Type
            TargetPath = $resolvedTarget.Path
            ResultDirectory = $allResultRoot
            Plan = $allPlan
            OptimizationPreview = $allOptimizationPreview
        }
        if ($PassThru) {
            $dryRunResult
        }
        return
    }

    if ($Optimized -and -not (Test-FioAdministrator)) {
        Write-FioStage -Title 'Elevation required for SMB optimization' -Status 'RUN'
        Write-FioProperty -Name 'Reason' -Value 'Applying SMB client tuning and adapter RSS changes requires an elevated PowerShell session.'
        Start-FioElevatedSelf -ScriptPath $MyInvocation.MyCommand.Path -BoundParameters $PSBoundParameters
        return
    }

    New-Item -ItemType Directory -Path $allResultRoot -Force | Out-Null

    $childResults = New-Object System.Collections.Generic.List[object]
    $usedCacheGroups = New-Object System.Collections.Generic.HashSet[string]
    $historicalJsonPath = Join-Path -Path $allResultRoot -ChildPath 'historical-summary.json'
    $historicalCsvPath = Join-Path -Path $allResultRoot -ChildPath 'historical-summary.csv'
    $historicalHtmlPath = Join-Path -Path $allResultRoot -ChildPath 'historical-report.html'
    $batchOptimizationState = $null
    try {
        if ($Optimized) {
            $batchOptimizationState = Enable-FioOptimizedSmbClientState -TargetInfo $resolvedTarget -ResultDirectory $allResultRoot
            Write-FioSmbOptimizationSummary -State $batchOptimizationState -Title 'Applied SMB optimization'
        }

        foreach ($planItem in $allPlan) {
            Write-FioStage -Title ("Starting child profile {0}" -f $planItem.Profile) -Status 'RUN'

            $invokeParams = @{
                TargetPath = $TargetPath
                TargetType = $TargetType
                Profile = $planItem.Profile
                OutputRoot = $allResultRoot
                EnableLogs = $EnableLogs
                KeepJobFile = $KeepJobFile
                NoCleanup = $NoCleanup
                PassThru = $true
            }

            if ($PSBoundParameters.ContainsKey('FioPath')) {
                $invokeParams.FioPath = $FioPath
            }

            foreach ($name in 'FileSizeGB', 'RuntimeSec', 'RampSec', 'Iterations', 'QueueDepth', 'NumJobs', 'BlockSize', 'ReadMix', 'Fsync', 'ThroughputCapMBps', 'IopsCap', 'Direct') {
                if ($PSBoundParameters.ContainsKey($name)) {
                    $invokeParams[$name] = $PSBoundParameters[$name]
                }
            }

            if ($planItem.PreparationRequired) {
                $invokeParams.ReusePreparedFiles = $true
                $invokeParams.PreparationCacheGroup = $planItem.PreparationCacheGroup
                [void]$usedCacheGroups.Add([string]$planItem.PreparationCacheGroup)
            }

            $childResult = & $MyInvocation.MyCommand.Path @invokeParams
            if ($null -ne $childResult) {
                $childResults.Add($childResult)
            }
        }

        $historicalRuns = Import-FioSqlBenchHistory -ResultsRoot $allResultRoot
        $historicalRollups = Get-FioHistoricalRollup -Runs $historicalRuns

        [pscustomobject]@{
            GeneratedUtc = [DateTime]::UtcNow.ToString('o')
            ResultsRoot = $allResultRoot
            ProfileFilter = 'Any'
            TargetTypeFilter = 'Any'
            TargetPathLike = $null
            Newest = $null
            RunCount = $historicalRuns.Count
            RollupCount = $historicalRollups.Count
            Runs = $historicalRuns
            Rollups = $historicalRollups
        } | ConvertTo-Json -Depth 12 | Set-Content -Path $historicalJsonPath -Encoding utf8

        Export-FioSqlBenchHistoricalCsv -Runs $historicalRuns -Path $historicalCsvPath
        Export-FioSqlBenchHtmlReport -Runs $historicalRuns -Rollups $historicalRollups -Path $historicalHtmlPath -Title ("fio SQL Bench All Profile Report - {0}" -f $allRunId) -ResultsRoot $allResultRoot
    }
    finally {
        if ($Optimized) {
            Restore-FioOptimizedSmbClientState -State $batchOptimizationState
        }

        if (-not $ReusePreparedFiles) {
            foreach ($cacheGroup in $usedCacheGroups) {
                $cachePath = Join-Path -Path $resolvedTarget.Path -ChildPath (Join-Path -Path '.fio-sql-bench-cache' -ChildPath $cacheGroup)
                if (Test-Path -LiteralPath $cachePath) {
                    Remove-Item -LiteralPath $cachePath -Recurse -Force
                }
            }
        }
    }

    Write-FioStage -Title 'All-profile benchmark finished successfully' -Status 'OK'
    Write-FioProperty -Name 'Profiles executed' -Value $childResults.Count
    Write-FioProperty -Name 'Historical JSON' -Value $historicalJsonPath
    Write-FioProperty -Name 'Historical CSV' -Value $historicalCsvPath
    Write-FioProperty -Name 'Historical HTML' -Value $historicalHtmlPath

    if ($PassThru) {
        [pscustomobject]@{
            RunId = $allRunId
            Profile = 'All'
            TargetPath = $resolvedTarget.Path
            TargetType = $resolvedTarget.Type
            ResultDirectory = $allResultRoot
            Runs = @($childResults)
            HistoricalJsonPath = $historicalJsonPath
            HistoricalCsvPath = $historicalCsvPath
            HistoricalHtmlPath = $historicalHtmlPath
            Optimization = $batchOptimizationState
        }
    }

    return
}

$profileDefaults = Get-FioSqlBenchProfileDefaults -Profile $Profile
$resolvedWorkloadProfile = [string]$profileDefaults.ProfileName

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
    -ThroughputCapMBps $ThroughputCapMBps `
    -IopsCap $IopsCap `
    -Direct $Direct

$runContext = New-FioSqlBenchRunContext `
    -TargetInfo $resolvedTarget `
    -OutputRoot $OutputRoot `
    -RunLabel $RunLabel `
    -WorkloadProfile $resolvedWorkloadProfile `
    -Settings $effectiveSettings

$physicalMemoryBytes = Get-FioPhysicalMemoryBytes
$cacheAssessment = Get-FioCacheBypassAssessment -TargetType $resolvedTarget.Type -Settings $effectiveSettings -PhysicalMemoryBytes $physicalMemoryBytes

$targetWorkDirectory = $runContext.TargetRunDirectory
$preparationCacheKey = $null
if ($ReusePreparedFiles) {
    if (-not [string]::IsNullOrWhiteSpace($PreparationCacheGroup)) {
        $preparationCacheKey = $PreparationCacheGroup
        $targetWorkDirectory = Join-Path -Path $resolvedTarget.Path -ChildPath (Join-Path -Path '.fio-sql-bench-cache' -ChildPath $PreparationCacheGroup)
    }
    else {
        $preparationCacheKey = Get-FioPreparationCacheKey -WorkloadProfile $resolvedWorkloadProfile -TargetType $resolvedTarget.Type -Settings $effectiveSettings
        $targetWorkDirectory = Join-Path -Path $resolvedTarget.Path -ChildPath (Join-Path -Path '.fio-sql-bench-cache' -ChildPath ('{0}-{1}' -f $resolvedWorkloadProfile.ToLowerInvariant(), $preparationCacheKey))
    }
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
Write-FioSettingsBlock -TargetInfo $resolvedTarget -Settings $effectiveSettings -RunContext $runContext -WorkloadProfile $resolvedWorkloadProfile
Write-FioCacheAssessment -Assessment $cacheAssessment

$optimizationPreview = $null
if ($Optimized) {
    $optimizationPreview = Enable-FioOptimizedSmbClientState -TargetInfo $resolvedTarget -ResultDirectory $runContext.ResultDirectory -DryRun
    Write-FioSmbOptimizationSummary -State $optimizationPreview -Title 'SMB optimization preview'
}

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
        Profile = $resolvedWorkloadProfile
        TargetType = $resolvedTarget.Type
        TargetPath = $resolvedTarget.Path
        ResultDirectory = $runContext.ResultDirectory
        TargetRunDirectory = $runContext.TargetRunDirectory
        EffectiveSettings = $effectiveSettings
        PrepJobPreview = $prepJobContent
        JobPreview = $jobContent
        OptimizationPreview = $optimizationPreview
    }
    if ($PassThru) {
        $dryRunResult
    }
    return
}

if ($Optimized -and -not (Test-FioAdministrator)) {
    Write-FioStage -Title 'Elevation required for SMB optimization' -Status 'RUN'
    Write-FioProperty -Name 'Reason' -Value 'Applying SMB client tuning and adapter RSS changes requires an elevated PowerShell session.'
    Start-FioElevatedSelf -ScriptPath $MyInvocation.MyCommand.Path -BoundParameters $PSBoundParameters
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
$optimizationState = $null

try {
    if (-not $PSCmdlet.ShouldProcess($executionRunContext.TargetRunDirectory, "Run fio profile $resolvedWorkloadProfile")) {
        return
    }

    if ($Optimized) {
        $optimizationState = Enable-FioOptimizedSmbClientState -TargetInfo $resolvedTarget -ResultDirectory $runContext.ResultDirectory
        Write-FioSmbOptimizationSummary -State $optimizationState -Title 'Applied SMB optimization'
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
        $iterationPerfmonPath = Join-Path -Path $runContext.ResultDirectory -ChildPath ("$iterationPrefix-perfmon-smb.csv")
        $perfmonCapture = $null

        Write-FioStage -Title ("Running fio iteration {0} of {1}" -f $iteration, $effectiveSettings.Iterations) -Status 'RUN'
        Write-FioProperty -Name 'JSON output' -Value $iterationJsonPath
        Write-FioProperty -Name 'Console log' -Value $iterationConsolePath
        if ($EnableLogs -and $resolvedTarget.Type -eq 'Smb') {
            $perfmonCollectorSpec = Get-FioSmbPerfmonCollectorSpec -TargetInfo $resolvedTarget
            if ($null -ne $perfmonCollectorSpec) {
                $perfmonCapture = Start-FioSmbPerfmonCapture -CollectorSpec $perfmonCollectorSpec -OutputPath $iterationPerfmonPath
                Write-FioProperty -Name 'PerfMon log' -Value $iterationPerfmonPath
            }
        }

        # Keep raw fio artifacts per iteration so failures are diagnosable without
        # re-running the benchmark.
        try {
            Invoke-FioSqlBenchRun `
                -FioPath $fioBinary.Path `
                -JobFilePath $jobFilePath `
                -OutputJsonPath $iterationJsonPath `
                -ConsoleLogPath $iterationConsolePath
        }
        finally {
            Stop-FioSmbPerfmonCapture -Capture $perfmonCapture
        }

        $summary = ConvertFrom-FioJsonToSummary `
            -JsonPath $iterationJsonPath `
            -RunContext $executionRunContext `
            -Settings $effectiveSettings `
            -Iteration $iteration `
            -FioVersion $fioBinary.Version `
            -TargetInfo $resolvedTarget

        $summary.Diagnostics.Requested = [bool]$EnableLogs
        $iterationDiagnosticLogPaths = Move-FioIterationDiagnosticLogs -ResultDirectory $runContext.ResultDirectory -IterationPrefix $iterationPrefix
        $additionalDiagnosticFiles = New-Object System.Collections.Generic.List[string]
        if (Test-Path -LiteralPath $iterationPerfmonPath) {
            $additionalDiagnosticFiles.Add($iterationPerfmonPath)
        }
        if ($summary.PSObject.Properties['Diagnostics'] -and $summary.Diagnostics.Available) {
            $summary.Diagnostics.SourceFiles = @($iterationDiagnosticLogPaths + $additionalDiagnosticFiles)
            $iterationDiagnosticsCsvPath = Join-Path -Path $runContext.ResultDirectory -ChildPath ("$iterationPrefix-diagnostics.csv")
            Export-FioSqlBenchDiagnosticsCsv -Diagnostics $summary.Diagnostics -Path $iterationDiagnosticsCsvPath
        }
        elseif (-not $EnableLogs) {
            $summary.Diagnostics.Status = 'Disabled'
            $summary.Diagnostics.Message = 'Diagnostics were not requested for this run.'
            $summary.Diagnostics.SourceFiles = @()
        }
        else {
            $summary.Diagnostics.SourceFiles = @($iterationDiagnosticLogPaths + $additionalDiagnosticFiles)
            if ([string]::IsNullOrWhiteSpace([string]$summary.Diagnostics.Message)) {
                $summary.Diagnostics.Message = 'Diagnostics were requested, but no chartable fio or SMB perf counter telemetry was captured for this iteration.'
            }
        }

        $summaryPath = Join-Path -Path $runContext.ResultDirectory -ChildPath ("$iterationPrefix-summary.json")
        $summary | ConvertTo-Json -Depth 12 | Set-Content -Path $summaryPath -Encoding utf8
        $iterationSummaries.Add($summary)

        Write-FioStage -Title ("Completed iteration {0}" -f $iteration) -Status 'OK'
        Write-FioIterationSummary -WorkloadProfile $resolvedWorkloadProfile -TargetType $resolvedTarget.Type -Summary $summary
        Write-FioProperty -Name 'Iteration summary' -Value $summaryPath
    }

    $runSucceeded = $true
}
finally {
    if ($Optimized) {
        Restore-FioOptimizedSmbClientState -State $optimizationState
    }

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
    Profile = $resolvedWorkloadProfile
    TargetPath = $resolvedTarget.Path
    TargetType = $resolvedTarget.Type
    ResultDirectory = $runContext.ResultDirectory
    DiagnosticsEnabled = [bool]$EnableLogs
    CpuAffinity = Get-FioCpuAffinitySummary -Settings $effectiveSettings
    Optimization = $optimizationState
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
        -WorkloadProfile $resolvedWorkloadProfile `
        -TargetPath $resolvedTarget.Path `
        -TargetType $resolvedTarget.Type `
        -ResultDirectory $runContext.ResultDirectory `
        -Summaries $iterationSummaries

    Write-FioStage -Title 'Console rendering of aggregated results' -Status 'OK'
    Write-FioPerformanceTable -WorkloadProfile $resolvedWorkloadProfile -TargetType $resolvedTarget.Type -Summary $aggregateRender
    Write-FioSqlInterpretation -WorkloadProfile $resolvedWorkloadProfile -TargetType $resolvedTarget.Type -Summary $aggregateRender
    Write-FioRecommendations -WorkloadProfile $resolvedWorkloadProfile -TargetType $resolvedTarget.Type -Summary $aggregateRender
}

if ($PassThru) {
    $aggregate
}
