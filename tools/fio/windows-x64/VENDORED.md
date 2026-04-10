# Vendored fio

This directory contains the repo-local fio runtime that `fio-sql-bench` will try first before using PATH or machine-wide installs.

Contents:
- `fio.exe`
- `COPYING.txt`
- `README.txt`
- `HOWTO.txt`

Refresh process:
1. Replace these files from a known-good fio Windows package or installation.
2. Keep the supporting license and documentation files with the executable.
3. Commit the updated files so the repo remains self-contained.
