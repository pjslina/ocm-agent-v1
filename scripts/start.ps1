# ============================================================
# MasterAgent 本地启动脚本 (Windows PowerShell)
# 用法: .\scripts\start.ps1
# ============================================================

$ErrorActionPreference = "Stop"

# 项目根目录（脚本所在目录的上一级）
$ProjectRoot = Split-Path -Parent $PSScriptRoot

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  MasterAgent 本地启动" -ForegroundColor Cyan
Write-Host "  项目根目录: $ProjectRoot" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# ── 1. 检查 .env 文件 ──────────────────────────────────────
$EnvFile = Join-Path $ProjectRoot ".env"
if (-not (Test-Path $EnvFile)) {
    Write-Host "[WARN] .env 文件不存在，请从 .env.example 复制并配置" -ForegroundColor Yellow
    Write-Host "       Copy-Item .env.example .env" -ForegroundColor Yellow
    exit 1
}
Write-Host "[OK] .env 文件已找到" -ForegroundColor Green

# ── 2. 检查虚拟环境 ────────────────────────────────────────
$VenvDir = Join-Path $ProjectRoot ".venv"
$VenvPython = Join-Path $VenvDir "Scripts\python.exe"

if (-not (Test-Path $VenvPython)) {
    Write-Host "[ERROR] 虚拟环境不存在: $VenvDir" -ForegroundColor Red
    Write-Host "        请先执行: uv venv  或  python -m venv .venv" -ForegroundColor Yellow
    exit 1
}
Write-Host "[OK] 虚拟环境已找到" -ForegroundColor Green

# ── 3. 激活虚拟环境 ────────────────────────────────────────
$ActivateScript = Join-Path $VenvDir "Scripts\Activate.ps1"
. $ActivateScript
Write-Host "[OK] 虚拟环境已激活: $(python --version)" -ForegroundColor Green

# ── 4. 检查数据库连接（可选） ──────────────────────────────
$Dsn = Select-String -Path $EnvFile -Pattern "^MA_PG_DSN_RW=" | ForEach-Object { ($_ -split "=", 2)[1] }
if ($Dsn) {
    Write-Host "[OK] 数据库 DSN 已配置" -ForegroundColor Green
} else {
    Write-Host "[WARN] 未配置 MA_PG_DSN_RW，服务将以 smoke 模式启动（无数据库）" -ForegroundColor Yellow
}

# ── 5. 启动服务 ────────────────────────────────────────────
Write-Host ""
Write-Host "正在启动 MasterAgent 服务..." -ForegroundColor Cyan
Write-Host "  地址: http://0.0.0.0:8000" -ForegroundColor Cyan
Write-Host "  文档: http://localhost:8000/docs" -ForegroundColor Cyan
Write-Host "  健康: http://localhost:8000/api/v1/health" -ForegroundColor Cyan
Write-Host ""

Set-Location $ProjectRoot
& $VenvPython -m ma.main
