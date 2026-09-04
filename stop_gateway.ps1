# Model Gateway 停止脚本：优先按端口找进程，兼容 pid 文件
$conn = Get-NetTCPConnection -LocalPort 8650 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
$target = $null
if ($conn) { $target = $conn.OwningProcess }
if (-not $target) {
    $pidFile = 'D:\AIcoding\model-gateway\gateway.pid'
    if (Test-Path $pidFile) { $target = Get-Content $pidFile }
}
if (-not $target) { Write-Host '未发现运行中的 Model Gateway（8650 端口无监听）'; exit 0 }
Stop-Process -Id $target -Force
Remove-Item 'D:\AIcoding\model-gateway\gateway.pid' -ErrorAction SilentlyContinue
Write-Host "已停止 Model Gateway（PID $target）"
