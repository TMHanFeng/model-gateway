# Model Gateway 启动器（开机自启/手动均可）
# 行为：开窗显示启动进度 -> 健康检查通过后本窗口自动关闭，主程序转入后台静默运行
#       60 秒未通过健康检查则窗口停留并提示日志位置，按回车才关闭
$ErrorActionPreference = 'Stop'
$dir  = 'D:\AIcoding\model-gateway'
$py   = 'D:\miniconda\python.exe'
$base = 'http://127.0.0.1:8650'

Set-Location $dir
function Test-Health {
    try {
        $r = Invoke-WebRequest -Uri "$base/health" -UseBasicParsing -TimeoutSec 2
        return ($r.StatusCode -eq 200)
    } catch { return $false }
}

if (Test-Health) {
    Write-Host '[Model Gateway] 已在运行，无需重复启动，窗口即将关闭...'
    Start-Sleep -Seconds 2
    exit 0
}

Write-Host '[Model Gateway] 正在启动...'
$out = Join-Path $dir 'logs\gateway_stdout.log'
$err = Join-Path $dir 'logs\gateway_stderr.log'
$proc = Start-Process -FilePath $py -ArgumentList 'main.py' -WorkingDirectory $dir `
        -WindowStyle Hidden -RedirectStandardOutput $out -RedirectStandardError $err -PassThru
Write-Host "[Model Gateway] 进程已拉起 PID $($proc.Id)，等待健康检查..."

$ok = $false
for ($i = 1; $i -le 60; $i++) {
    if (Test-Health) { $ok = $true; break }
    Start-Sleep -Seconds 1
    Write-Host ("  等待中 {0}s" -f $i)
}

if ($ok) {
    Set-Content -Path (Join-Path $dir 'gateway.pid') -Value $proc.Id
    Write-Host "[Model Gateway] 启动成功（PID $($proc.Id)），窗口即将关闭，转入后台静默运行"
    Start-Sleep -Seconds 2
    exit 0
} else {
    Write-Host '[Model Gateway] 60 秒内未通过健康检查！进程未终止，可查看日志排查：'
    Write-Host "  $err"
    Read-Host '按回车关闭本窗口'
    exit 1
}
