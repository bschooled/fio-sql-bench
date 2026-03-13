# fio-sql-bench

`fio-sql-bench` is a Windows-first PowerShell harness for running file-based `fio` benchmarks that approximate common SQL Server storage patterns on either local disks or SMB shares.

## Scope

Version 1 stays intentionally narrow:

- File-based tests only
- Missing target directories are created automatically
- Local paths, UNC paths, and mapped SMB drives supported
- Five built-in workload profiles: `Data`, `Log`, `Tempdb`, `BackupRestore`, `DbccScan`
- Structured JSON, CSV, and HTML output per run
- Historical rollup reporting across multiple result folders

Raw device benchmarking is intentionally blocked to reduce destructive risk.

## Prerequisites

- PowerShell 7 or Windows PowerShell 5.1
- `fio.exe` installed and available on `PATH`, or passed with `-FioPath`
  - You can find *fio* from the releases page of [Fio](https://github.com/axboe/fio)
- Permission to create a dedicated test directory on the target volume or SMB share

The script looks for `fio.exe` in:

- `PATH`
- `C:\Program Files\fio\fio.exe`
- `C:\Program Files (x86)\fio\fio.exe`

## Workload Profiles

### Data

Approximates OLTP data file traffic.
Default working set: `32 GB` to push past common filesystem and memory-cache effects.

- `rw=randrw`
- `bs=8k`
- `rwmixread=70`
- `iodepth=32`
- `numjobs=4`

### Log

Approximates log writer behavior.
Default working set: `8 GB`.

- `rw=write`
- `bs=64k`
- `iodepth=1`
- `numjobs=1`
- `fsync=1`
- direct I/O is enabled by default so client-side buffering is less likely to hide commit latency

### Tempdb

Approximates scratch-heavy tempdb activity.
Default working set: `16 GB` to reduce the chance that results are dominated by cache residency.

- `rw=randrw`
- `bs=8k`
- `rwmixread=50`
- `iodepth=32`
- `numjobs=8`

### BackupRestore

Approximates large-block backup or restore transfer behavior.
Default working set: `64 GB` so sequential throughput is measured with a realistic transfer size.

- `rw=rw`
- `bs=1m`
- `rwmixread=50`
- `iodepth=8`
- `numjobs=2`

### DbccScan

Approximates DBCC-style large-block scan reads.
Default working set: `32 GB`.

- `rw=read`
- `bs=256k`
- `iodepth=8`
- `numjobs=2`

## Usage

Show built-in help:

```powershell
./scripts/Invoke-FioSqlBench.ps1 -Help
```

If you want the script to emit the structured PowerShell result object in addition to the friendly console output, add `-PassThru`.

Dry-run the generated job and effective settings:

```powershell
.\scripts\Invoke-FioSqlBench.ps1 `
  -TargetPath 'C:\SqlBench' `
  -Profile Data `
  -DryRun
```

Run against a local folder and keep the generated workload files for inspection:

```powershell
.\scripts\Invoke-FioSqlBench.ps1 `
  -TargetPath 'D:\SqlBench' `
  -Profile Data `
  -Iterations 3 `
  -KeepJobFile `
  -NoCleanup
```

If `-TargetPath` does not exist yet, the script creates that directory before generating benchmark files.

Target detection is automatic by default. UNC paths and mapped network drives such as `Z:` are treated as SMB targets, while ordinary drive-letter paths on local volumes are treated as local storage.

Benchmark file preparation is now less expensive than the original implementation:

- Prepared files are built once per run and then reused across all iterations in that run.
- The prep phase now uses larger sequential writes than the measured workload so SMB and other remote targets do not spend excessive time creating files with 8K synchronous I/O.
- Pure write workloads such as the built-in `Log` profile skip the prep phase entirely because no pre-existing read surface is required.
- If you are repeatedly testing the same target with the same settings, `-ReusePreparedFiles` keeps a validated prep cache under the target path and reuses it on later runs. This is especially helpful for slower SMB targets.

The built-in `Data` and `Tempdb` profiles now default to larger working sets (`32 GB` and `16 GB`) so short benches are less likely to be dominated by RAM or filesystem cache effects.

The harness now prints a cache-bypass assessment before execution. This is important because `direct=1` can bypass the Windows page cache for local `windowsaio` runs, and the built-in profiles now request direct I/O by default for SMB as well to reduce client-side cache effects. It still cannot generically bypass SSD controller DRAM, RAID cache, or SMB server-side cache. Very small working sets can still produce inflated numbers even when local page cache bypass is active.

Run against an SMB share with explicit buffered I/O:

```powershell
.\scripts\Invoke-FioSqlBench.ps1 `
  -TargetPath '\\fileserver\sqlbench' `
  -Profile Log `
  -Direct Off
```

Run against a mapped SMB drive and let the script auto-detect it as SMB:

```powershell
.\scripts\Invoke-FioSqlBench.ps1 `
  -TargetPath 'Z:\SqlBench' `
  -Profile Data
```

Reuse previously prepared files for repeated runs against the same target and settings:

```powershell
.\scripts\Invoke-FioSqlBench.ps1 `
  -TargetPath 'Z:\SqlBench' `
  -Profile Data `
  -ReusePreparedFiles
```

Override defaults for a custom SQL-like test while still using a built-in profile baseline:

```powershell
.\scripts\Invoke-FioSqlBench.ps1 `
  -TargetPath 'D:\SqlBench' `
  -Profile Tempdb `
  -FileSizeGB 32 `
  -RuntimeSec 120 `
  -QueueDepth 64 `
  -NumJobs 16
```

Dry-run the newer large-block profiles:

```powershell
.\scripts\Invoke-FioSqlBench.ps1 `
  -TargetPath 'D:\SqlBench' `
  -Profile BackupRestore `
  -DryRun

.\scripts\Invoke-FioSqlBench.ps1 `
  -TargetPath 'D:\SqlBench' `
  -Profile DbccScan `
  -DryRun
```

Build a historical report across existing result folders:

```powershell
.\scripts\Export-FioSqlBenchReport.ps1 `
  -ResultsRoot '.\results'
```

Emit the aggregated historical object for automation while still writing the HTML, JSON, and CSV artifacts:

```powershell
.\scripts\Export-FioSqlBenchReport.ps1 `
  -ResultsRoot '.\results' `
  -PassThru
```

Optionally filter historical output to a subset of runs:

```powershell
.\scripts\Export-FioSqlBenchReport.ps1 `
  -ResultsRoot '.\results' `
  -Profile Data `
  -TargetType Smb `
  -Newest 10
```

## Output Layout

Each invocation writes results under `results\<timestamp>-<label>\`.

For larger runs, the harness now performs an explicit file-preparation phase before the timed benchmark phase and verifies that each benchmark file reached the expected size before random reads begin.

To reduce benchmark skew on storage that benefits from repeated or compressible payloads, generated fio jobs also enable `refill_buffers=1` so each submission refills the I/O buffer instead of reusing a more compression-friendly pattern.

The console also renders a SQL-oriented summary table with color-coded latency interpretation:

- `Excellent` and `Very good` indicate latency comfortably inside common SQL guidance.
- `Watch` indicates tail latency or sustained latency that is approaching or exceeding Microsoft's `10-15 ms` investigation threshold.
- `Poor` or worse indicates storage latency that should be treated as a SQL bottleneck candidate.
- The console now includes `P99.9` latency in addition to mean, `P95`, and `P99`, because deep tail stalls are often what surface first in SQL workloads.
- When multiple fio workers are active, the summary also tracks the worst worker `P99` so uneven latency across files or queues is easier to spot.
- Bandwidth stability is surfaced as a coefficient of variation (`CV`) so bursty or throttled paths are easier to distinguish from stable baselines.
- For multi-iteration runs, the console prints a `min / avg / max` rollup across iterations.
- The console also prints profile-specific recommendations so the results read more like an operator report than a raw benchmark dump.

The script uses Microsoft guidance as its interpretation baseline:

- General SQL investigation threshold: sustained `10-15 ms`
- Log-oriented writes: typically best in the `1-5 ms` range
- Data/tempdb-oriented I/O: healthiest under `10 ms`, with `4-20 ms` as a common tuned range
- SMB-backed targets are interpreted separately because network and file-server effects are part of the path. The script keeps the same `10-15 ms` SQL escalation rule, but it presents SMB guidance with slightly more forgiving healthy bands and adds SMB-specific recommendation text.
- For SMB runs, the console also reports the resolved server/share, negotiated dialect, continuous-availability flag, encryption flag, visible multichannel path count, and RDMA-capable path count when Windows can resolve them.

- `iter-01-fio.json`: raw `fio` JSON output
- `iter-01-summary.json`: normalized summary for that iteration
- `summary.json`: aggregate wrapper containing all iterations
- `summary.csv`: flat iteration table for spreadsheets and diffing
- `summary.html`: self-contained operator report with inline bar charts for throughput and latency
- `iter-01-console.log`: non-JSON `fio` console output and errors

The historical export script writes these additional artifacts under the chosen results root:

- `historical-summary.json`: aggregated run-level data model across result folders
- `historical-summary.csv`: flat run-level table for spreadsheets and diffing
- `historical-report.html`: self-contained historical dashboard with rollup tables and inline charts

The CSV exports now retain additional SQL-relevant tail and stability fields, including `P99.9`, worst-worker `P99`, and bandwidth variation, so external analysis can distinguish average health from deep-tail or burst-driven problems.

The historical HTML report is intended to be comparative rather than just archival:

- Recent runs are grouped by workload profile so `Data`, `Log`, `Tempdb`, `BackupRestore`, and `DbccScan` do not blur together.
- Each row is compared against the previous run in the same profile, with deltas shown for read/write throughput and read/write `P99` latency.
- Effective run settings are rendered as compact badges so changes in block size, queue depth, job count, direct I/O mode, runtime, and related knobs are visible at a glance.
- When a setting changed relative to the previous run, the badge is highlighted and a short `Settings changed:` summary is printed under that row.

The benchmark data files are created under the target directory in a unique subfolder. By default that target subfolder is removed after the run. Use `-NoCleanup` to keep it.
If a run fails, the target work folder is preserved automatically so the generated files can be inspected.

## SMB Notes

- UNC paths are classified as SMB automatically.
- Mapped network drives such as `Z:` are also classified as SMB automatically.
- When a mapped drive is detected, the console output also shows the backing remote SMB path.
- SMB tests measure storage, network, client cache, and protocol behavior together.
- The built-in profiles default to direct I/O for SMB targets as well as local targets. Use `-Direct Off` if you intentionally want a buffered run.
- If `Get-SmbConnection` can resolve the share, the summary includes connection metadata such as dialect, open handles, encryption, and continuous availability.
- If `Get-SmbMultichannelConnection` can resolve the server, the summary also includes visible channel and RDMA-capable path counts.
- SMB console recommendations call out Microsoft guidance for SQL over SMB: ensure enough network bandwidth, prefer SMB Multichannel, and use SMB Direct/RDMA where available.
- Historical and console interpretation for SMB should be read with the network path in mind. Microsoft’s SQL-over-SMB guidance assumes enough bandwidth for the workload and recommends SMB 3 features such as Multichannel, SMB Direct/RDMA, and continuous availability where applicable.
- A direct-I/O setting on SMB reduces client-side caching risk only if the SMB path honors it. It does not guarantee bypass of server-side or storage-device cache.

## Safety Notes

- Use a dedicated test directory.
- Do not point the script at a production data directory.
- The script requires a directory path and rejects raw device syntax.
- Local free space is checked before execution with 10% headroom.

## Next Gaps

These items are not implemented yet:

- Automatic `fio` download and checksum validation
- Exporting charts as image files instead of self-contained HTML
- Additional SQL profiles such as checkpoint, index rebuild, or bulk-load patterns
