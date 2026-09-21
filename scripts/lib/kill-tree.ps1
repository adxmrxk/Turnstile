# Stops every process whose command line mentions -Match, and everything those
# processes started. Used by mutation-check.sh on Windows, where a hung Maven
# leaves its forked test JVM spinning after Maven itself is killed, and where
# Surefire's own fork timeout was observed not to fire.
param([Parameter(Mandatory = $true)][string]$Match)

function Stop-Tree($id) {
  Get-CimInstance Win32_Process -Filter "ParentProcessId=$id" |
    ForEach-Object { Stop-Tree $_.ProcessId }
  Stop-Process -Id $id -Force -ErrorAction SilentlyContinue
}

Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -like "*$Match*" -and $_.ProcessId -ne $PID } |
  ForEach-Object { Stop-Tree $_.ProcessId }
