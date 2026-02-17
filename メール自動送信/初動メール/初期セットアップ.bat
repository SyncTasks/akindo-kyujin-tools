@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo === 初動メール自動送信 セットアップ開始 ===
echo.

echo [1/3] Python仮想環境を作成中...
where py >nul 2>&1
if %errorlevel%==0 (
    set PYTHON_CMD=py
) else (
    where python >nul 2>&1
    if %errorlevel%==0 (
        set PYTHON_CMD=python
    ) else (
        echo エラー: Pythonが見つかりません
        echo https://www.python.org/downloads/ からインストールしてください
        pause
        exit /b 1
    )
)

%PYTHON_CMD% -m venv venv
call venv\Scripts\activate

echo [2/3] 依存パッケージをインストール中...
pip install -r 必要パッケージ.txt

echo [3/3] .envファイルを確認中...
if not exist .env (
    copy .env.example .env
    echo.
    echo .envファイルを作成しました。中身を編集してください。
) else (
    echo .envファイルは既に存在します。
)

echo.
echo === セットアップ完了 ===
echo.
echo 次のステップ:
echo   1. .env ファイルに GOOGLE_CREDENTIALS を設定
echo   2. credentials.json を配置（ローカル開発用、任意）
echo   3. 実行.bat で動作確認（--dry-run で送信なし確認）
echo   4. タスク登録.ps1 でタスクスケジューラに登録
pause
