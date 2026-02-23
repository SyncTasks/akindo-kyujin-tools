"""設定・定数モジュール"""

import os

# ===== スプレッドシート設定 =====
# 設定スプレッドシート（ユーザ情報・ログイン情報）
CONFIG_SPREADSHEET_ID = os.environ.get(
    'CONFIG_SPREADSHEET_ID',
    '1HzSM76jUtUOzHiy1zg3Ivqg_-nTn0iFwVwrzG82hQzU',
)
CONFIG_SHEET_NAME = 'ユーザ'

# 応募者シート名（テンプレートSSの中のシート）
APPLICANT_SHEET_NAME = '応募者シート_メールテスト'

# メール管理シート名（テンプレートSSの中のシート）
MAIL_TEMPLATE_SHEET_NAME = 'メール管理'

# ===== SMTP設定 =====
SMTP_TIMEOUT = 30  # 接続タイムアウト（秒）
SMTP_MAX_RETRIES = 3  # 最大リトライ回数
SMTP_RETRY_INTERVAL = 5  # リトライ間隔ベース（秒）

# デフォルトSMTPサーバー（プロバイダー判定不能時のフォールバック）
SMTP_DEFAULT_SERVER = 'smtp.muumuu-mail.com'
SMTP_DEFAULT_PORT = 587

# IMAPサーバー → SMTPサーバーのマッピング
IMAP_TO_SMTP_MAP = {
    'imap4.muumuu-mail.com': ('smtp.muumuu-mail.com', 587),
    'imap.muumuu-mail.com': ('smtp.muumuu-mail.com', 587),
    'imap.gmail.com': ('smtp.gmail.com', 587),
    'imap.googlemail.com': ('smtp.gmail.com', 587),
    'imap.onamae.com': ('smtp.onamae.com', 587),
    'imap.lolipop.jp': ('smtp.lolipop.jp', 587),
}

# メールドメイン → SMTPサーバーのマッピング（IMAP列が空の場合のフォールバック）
DOMAIN_TO_SMTP_MAP = {
    'gmail.com': ('smtp.gmail.com', 587),
    'googlemail.com': ('smtp.gmail.com', 587),
    'muumuu-mail.com': ('smtp.muumuu-mail.com', 587),
    'onamae.com': ('smtp.onamae.com', 587),
    'lolipop.jp': ('smtp.lolipop.jp', 587),
}

# ===== 処理設定 =====
# 応募日時の検索対象期間（日数）
SEARCH_DAYS = 1

# API呼び出し後の待機時間（秒）— レート制限対策
API_WAIT_INTERVAL = 2

# Google Sheets API レート制限リトライ
SHEETS_API_MAX_RETRIES = 3
SHEETS_API_RETRY_INTERVAL = 5  # リトライ間隔ベース（秒）

# ===== ログ設定 =====
# ログ保管期間（日数）: 今日と昨日を保持、一昨日以前を削除
LOG_RETENTION_DAYS = 1
