# 初動メール自動送信 - タスクスケジューラ登録スクリプト
# 管理者権限の PowerShell で実行してください
#
# 使い方:
#   .\タスク登録.ps1                  # デフォルト（5分間隔）
#   .\タスク登録.ps1 -IntervalMinutes 10  # 10分間隔に変更

param(
    [int]$IntervalMinutes = 5
)

$taskName = "初動メール自動送信"
$basePath = Split-Path -Parent $MyInvocation.MyCommand.Path
$batPath  = Join-Path $basePath "実行.bat"

# 実行ファイルの存在確認
if (-not (Test-Path $batPath)) {
    Write-Host "[エラー] 実行.bat が見つかりません: $batPath" -ForegroundColor Red
    Write-Host "  → 初期セットアップ.bat を先に実行してください" -ForegroundColor Yellow
    exit 1
}

# 既存タスクがあれば削除
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Host "[更新] 既存タスク '$taskName' を削除しました" -ForegroundColor Yellow
}

# トリガー: 毎日0:00開始、指定間隔で繰り返し（無期限）
$trigger = New-ScheduledTaskTrigger -Daily -At "00:00"
$trigger.Repetition = (New-ScheduledTaskTrigger -Once -At "00:00" `
    -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes)).Repetition

# 操作: 実行.bat を実行
$action = New-ScheduledTaskAction -Execute $batPath -WorkingDirectory $basePath

# 設定
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10)

# 登録（SYSTEM権限で実行 = ログオフ中も動作）
Register-ScheduledTask `
    -TaskName $taskName `
    -Trigger $trigger `
    -Action $action `
    -Settings $settings `
    -User "SYSTEM" `
    -RunLevel Highest `
    -Description "応募者シートの未送信行を検出し、年齢に応じたテンプレートで自動返信メールを送信（${IntervalMinutes}分間隔）"

Write-Host ""
Write-Host "[完了] タスク '$taskName' を登録しました" -ForegroundColor Green
Write-Host "  実行間隔: ${IntervalMinutes}分ごと" -ForegroundColor Cyan
Write-Host "  実行ファイル: $batPath" -ForegroundColor Cyan
Write-Host "  作業フォルダ: $basePath" -ForegroundColor Cyan
Write-Host ""

# 確認表示
Get-ScheduledTask -TaskName $taskName | Format-List TaskName, State, Description
