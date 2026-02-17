"""Google Spreadsheet 操作モジュール

スプレッドシートの読み書きを担当する。
- 設定SS（ユーザシート）からアカウント情報を取得
- 応募者シートから未送信の応募者を取得
- メール管理シートからテンプレートを取得
- 送信済みフラグの更新
"""

import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse

import gspread
from google.oauth2.service_account import Credentials

from config import (
    CONFIG_SPREADSHEET_ID,
    CONFIG_SHEET_NAME,
    APPLICANT_SHEET_NAME,
    MAIL_TEMPLATE_SHEET_NAME,
    SEARCH_DAYS,
    SHEETS_API_MAX_RETRIES,
    SHEETS_API_RETRY_INTERVAL,
    IMAP_TO_SMTP_MAP,
    DOMAIN_TO_SMTP_MAP,
    SMTP_DEFAULT_SERVER,
    SMTP_DEFAULT_PORT,
)

JST = timezone(timedelta(hours=9))


# ===== Google認証 =====

def get_sheets_client() -> Optional[gspread.Client]:
    """Google Sheets クライアントを取得する

    認証方法（優先順）:
      1. credentials.json ファイル（ローカル開発用）
      2. GOOGLE_CREDENTIALS 環境変数（GitHub Actions / サーバー用）

    Returns:
        gspread.Client or None（認証失敗時）
    """
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]

    # 1. credentials.json ファイルを探す
    script_dir = os.path.dirname(os.path.abspath(__file__))
    creds_file = os.path.join(script_dir, 'credentials.json')

    if os.path.exists(creds_file):
        try:
            creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
            print('認証: credentials.json を使用')
            return gspread.authorize(creds)
        except Exception as e:
            print(f'credentials.json 読み込みエラー: {e}')

    # 2. 環境変数から取得
    creds_json = os.environ.get('GOOGLE_CREDENTIALS', '')
    if creds_json:
        try:
            creds_dict = json.loads(creds_json)
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            print('認証: GOOGLE_CREDENTIALS 環境変数を使用')
            return gspread.authorize(creds)
        except Exception as e:
            print(f'GOOGLE_CREDENTIALS 解析エラー: {e}')

    print('エラー: Google認証情報が見つかりません')
    print('  → credentials.json を配置するか、GOOGLE_CREDENTIALS 環境変数を設定してください')
    return None


# ===== 設定SS読み込み =====

def get_active_accounts(client: gspread.Client) -> List[dict]:
    """設定SSの「ユーザ」シートから、メール送信=TRUE のアカウント一覧を取得する

    Returns:
        アカウント情報のリスト。各要素は以下のキーを持つ:
        - email: メールアドレス（SMTP認証 & 送信元）
        - password: パスワード（SMTP認証）
        - client_name: クライアント名
        - template_spreadsheet_id: メール文面列のスプレッドシートID
    """
    try:
        spreadsheet = client.open_by_key(CONFIG_SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(CONFIG_SHEET_NAME)
        records = worksheet.get_all_records()
    except Exception as e:
        print(f'設定SS読み込みエラー: {e}')
        return []

    accounts = []
    for record in records:
        # メール送信列が TRUE のみ対象
        mail_send_flag = record.get('メール送信', False)
        is_mail_send = mail_send_flag is True or str(mail_send_flag).upper() == 'TRUE'
        if not is_mail_send:
            continue

        email = str(record.get('メール', '')).strip()
        password = str(record.get('パス', '')).strip()
        client_name = _normalize_name(str(record.get('クライアント名', '')))
        template_ss_id = _extract_spreadsheet_id(str(record.get('メール文面', '')).strip())
        imap_server = str(record.get('IMAP', '')).strip()

        if not email or not password:
            print(f'  警告: {client_name} のメール/パスが未設定、スキップ')
            continue

        if not template_ss_id:
            print(f'  警告: {client_name} のメール文面（スプレッドシートID）が未設定、スキップ')
            continue

        # SMTPサーバーを判定（IMAP列 → メールドメイン → デフォルト）
        smtp_server, smtp_port = _resolve_smtp(imap_server, email)

        accounts.append({
            'email': email,
            'password': password,
            'client_name': client_name,
            'template_spreadsheet_id': template_ss_id,
            'smtp_server': smtp_server,
            'smtp_port': smtp_port,
        })

    print(f'メール送信対象アカウント: {len(accounts)}件')
    for i, acc in enumerate(accounts):
        print(f'  {i + 1}. {acc["client_name"]} ({acc["email"]}) → SMTP: {acc["smtp_server"]}:{acc["smtp_port"]}')

    return accounts


# ===== 応募者シート読み込み =====

def get_unsent_applicants(
    client: gspread.Client,
    spreadsheet_id: str,
) -> Tuple[Optional[gspread.Worksheet], List[dict]]:
    """応募者シートから未送信 & 直近N日以内の応募者を取得する

    Args:
        client: gspread クライアント
        spreadsheet_id: テンプレート＆応募者スプレッドシートのID

    Returns:
        (worksheet, applicants) のタプル
        - worksheet: 応募者シートの Worksheet オブジェクト（送信済み更新用）
        - applicants: 未送信応募者のリスト
    """
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(APPLICANT_SHEET_NAME)
        all_values = worksheet.get_all_values()
    except Exception as e:
        print(f'応募者シート読み込みエラー (SS ID: {spreadsheet_id}): {type(e).__name__}: {e}')
        return None, []

    if len(all_values) < 2:
        print(f'応募者シート: データ行がありません')
        return worksheet, []

    # ヘッダー行から必要な列のインデックスを特定
    headers = all_values[0]
    required_cols = ['メール送信済', '応募日時', '名前', '年齢', 'メールアドレス', 'クライアント名', 'クライアント', 'タイトル']
    col_map = {}
    for col_name in required_cols:
        try:
            col_map[col_name] = headers.index(col_name)
        except ValueError:
            col_map[col_name] = -1

    # 必須列の存在チェック
    if col_map['メールアドレス'] < 0:
        print(f'応募者シート: 「メールアドレス」列が見つかりません')
        return worksheet, []

    data_rows = all_values[1:]

    # 直近N日の判定基準: 今日の0:00からSEARCH_DAYS日前の0:00まで
    now = datetime.now(JST)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = today_start - timedelta(days=SEARCH_DAYS)

    applicants = []
    skipped_sent = 0
    skipped_old = 0
    skipped_no_email = 0

    def _get(row, col_name):
        i = col_map[col_name]
        if i < 0 or i >= len(row):
            return ''
        return str(row[i]).strip()

    for i, row in enumerate(data_rows):
        row_index = i + 2  # ヘッダー行(1) + 0-indexed → 1-indexed

        # メール送信済 が空でないものはスキップ
        sent_flag = _get(row, 'メール送信済')
        if sent_flag:
            skipped_sent += 1
            continue

        # 応募日時が直近N日以内かチェック
        date_str = _get(row, '応募日時')
        if not date_str:
            skipped_old += 1
            continue

        application_date = _parse_date(date_str)
        if application_date is None:
            print(f'  行{row_index}: 応募日時のパースに失敗 ({date_str})')
            skipped_old += 1
            continue

        if application_date < cutoff:
            skipped_old += 1
            continue

        # メールアドレスが必要
        email_address = _get(row, 'メールアドレス')
        if not email_address:
            skipped_no_email += 1
            print(f'  行{row_index}: メールアドレスが空、スキップ ({_get(row, "名前")})')
            continue

        # 年齢を取得
        age = _parse_age(_get(row, '年齢'))

        applicants.append({
            'row_index': row_index,
            'name': _get(row, '名前'),
            'age': age,
            'email_address': email_address,
            'client_name': _normalize_name(_get(row, 'クライアント名') or _get(row, 'クライアント')),
            'title': _get(row, 'タイトル'),
            'application_date': date_str,
        })

    print(f'応募者シート読み込み完了 (SS ID: {spreadsheet_id})')
    print(f'  全{len(data_rows)}件 → 未送信&直近{SEARCH_DAYS}日: {len(applicants)}件')
    print(f'  スキップ内訳: 送信済={skipped_sent}, 期間外={skipped_old}, メールなし={skipped_no_email}')

    return worksheet, applicants


# ===== メール管理シート読み込み =====

def get_mail_templates(
    client: gspread.Client,
    spreadsheet_id: str,
) -> Dict[str, dict]:
    """メール管理シートからクライアント別テンプレートを取得する

    Args:
        client: gspread クライアント
        spreadsheet_id: テンプレート＆応募者スプレッドシートのID

    Returns:
        正規化されたクライアント名をキーとした辞書。各値は以下のキーを持つ:
        - subject: 件名テンプレート（「件名」列。なければデフォルト使用）
        - under_35: 34歳以下向けテンプレート文面
        - over_35: 35歳以上向けテンプレート文面
    """
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(MAIL_TEMPLATE_SHEET_NAME)
        all_values = worksheet.get_all_values()
    except Exception as e:
        print(f'メール管理シート読み込みエラー (SS ID: {spreadsheet_id}): {e}')
        return {}

    if len(all_values) < 2:
        print(f'メール管理シート: データ行がありません')
        return {}

    # ヘッダー行を自動検出（先頭5行以内で「クライアント名」を含む行を探す）
    header_row_idx = -1
    for idx, row in enumerate(all_values[:5]):
        if 'クライアント名' in row:
            header_row_idx = idx
            break

    if header_row_idx < 0:
        print(f'メール管理シート: 「クライアント名」列が見つかりません')
        print(f'  先頭行の内容: {all_values[0][:5]}')
        return {}

    headers = all_values[header_row_idx]
    data_start = header_row_idx + 1

    if data_start >= len(all_values):
        print(f'メール管理シート: データ行がありません')
        return {}

    # ヘッダー行から必要な列のインデックスを特定
    col_map = {}
    for col_name in ['クライアント名', '件名', '34歳以下', '35歳以上']:
        try:
            col_map[col_name] = headers.index(col_name)
        except ValueError:
            col_map[col_name] = -1  # 見つからない列は -1

    templates = {}
    for row in all_values[data_start:]:
        idx = col_map['クライアント名']
        client_name = _normalize_name(row[idx] if idx < len(row) else '')
        if not client_name:
            continue

        def _get_cell(col_name):
            i = col_map[col_name]
            if i < 0 or i >= len(row):
                return ''
            return str(row[i]).strip()

        subject = _get_cell('件名')
        under_35 = _get_cell('34歳以下')
        over_35 = _get_cell('35歳以上')

        templates[client_name] = {
            'subject': subject,
            'under_35': under_35,
            'over_35': over_35,
        }

    print(f'メール管理シート読み込み完了: {len(templates)}件のテンプレート')
    for name in templates:
        has_subject = '○' if templates[name]['subject'] else '×'
        has_under = '○' if templates[name]['under_35'] else '×'
        has_over = '○' if templates[name]['over_35'] else '×'
        print(f'  {name}: 件名={has_subject}, 34歳以下={has_under}, 35歳以上={has_over}')

    return templates


# ===== 送信済み更新 =====

def mark_as_sent(
    worksheet: gspread.Worksheet,
    row_index: int,
    headers: List[str],
) -> bool:
    """応募者シートの「メール送信済」列を更新する（リトライ付き）

    Args:
        worksheet: 応募者シートの Worksheet オブジェクト
        row_index: 更新する行番号（1-indexed）
        headers: ヘッダー行のリスト（列名の順序を把握するため）

    Returns:
        True: 更新成功, False: 更新失敗
    """
    now_str = datetime.now(JST).strftime('%Y/%m/%d %H:%M:%S')

    try:
        col_index = headers.index('メール送信済') + 1  # gspread は 1-indexed
    except ValueError:
        print(f'  エラー: 「メール送信済」列がヘッダーに見つかりません')
        return False

    for attempt in range(SHEETS_API_MAX_RETRIES):
        try:
            worksheet.update_cell(row_index, col_index, now_str)
            return True
        except gspread.exceptions.APIError as e:
            if '429' in str(e) or 'RATE_LIMIT' in str(e):
                wait = (attempt + 1) * SHEETS_API_RETRY_INTERVAL
                print(f'  Sheets APIレート制限: {wait}秒後にリトライ ({attempt + 1}/{SHEETS_API_MAX_RETRIES})')
                time.sleep(wait)
            else:
                print(f'  エラー: 行{row_index}のメール送信済更新に失敗 (API): {e}')
                return False
        except Exception as e:
            print(f'  エラー: 行{row_index}のメール送信済更新に失敗: {e}')
            return False

    print(f'  エラー: 行{row_index}のメール送信済更新がリトライ上限に達しました')
    return False


# ===== テンプレート選択 =====

def select_template(age: Optional[int], templates: dict) -> Optional[str]:
    """年齢に応じたテンプレートを選択する

    Args:
        age: 応募者の年齢（None の場合は判定不可）
        templates: テンプレート辞書（under_35, over_35）

    Returns:
        テンプレート文面。選択不可の場合は None。
    """
    if age is None:
        # 年齢不明の場合は 34歳以下をデフォルトとする
        print(f'    警告: 年齢不明のため「34歳以下」テンプレートをデフォルト使用')
        return templates.get('under_35') or None

    if age <= 34:
        return templates.get('under_35') or None
    else:
        return templates.get('over_35') or None


# ===== ユーティリティ =====

def _resolve_smtp(imap_server: str, email: str) -> Tuple[str, int]:
    """IMAP列またはメールドメインからSMTPサーバーを判定する

    判定順序:
      1. IMAP列の値で IMAP_TO_SMTP_MAP を完全一致検索
      2. IMAP列の値にマッピングキーが含まれるか部分一致検索
      3. メールアドレスのドメインで DOMAIN_TO_SMTP_MAP を検索
      4. いずれもヒットしなければデフォルト値

    Args:
        imap_server: IMAP列の値（サーバー名など）
        email: メールアドレス（ドメインからの推定用）

    Returns:
        (smtp_server, smtp_port) のタプル
    """
    imap_lower = imap_server.lower().strip()

    # 1. IMAP列で完全一致
    if imap_lower in IMAP_TO_SMTP_MAP:
        return IMAP_TO_SMTP_MAP[imap_lower]

    # 2. IMAP列で部分一致（例: "imap4.muumuu-mail.com:993" のようにポート付きの場合）
    for key, value in IMAP_TO_SMTP_MAP.items():
        if key in imap_lower:
            return value

    # 3. IMAP列に "gmail" 等のキーワードが含まれる場合
    if 'gmail' in imap_lower or 'google' in imap_lower:
        return ('smtp.gmail.com', 587)
    if 'muumuu' in imap_lower:
        return ('smtp.muumuu-mail.com', 587)
    if 'onamae' in imap_lower:
        return ('smtp.onamae.com', 587)
    if 'lolipop' in imap_lower:
        return ('smtp.lolipop.jp', 587)
    if 'xserver' in imap_lower or 'xsrv' in imap_lower:
        # xserver は各ドメインごとにSMTPサーバーが異なる
        # メールアドレスのドメインをそのまま使う
        domain = email.split('@')[-1] if '@' in email else ''
        if domain:
            return (domain, 587)

    # 4. メールアドレスのドメインで判定
    if '@' in email:
        domain = email.split('@')[-1].lower()
        if domain in DOMAIN_TO_SMTP_MAP:
            return DOMAIN_TO_SMTP_MAP[domain]

        # Google Workspace: ドメインは独自だがGmail SMTPを使う場合がある
        # → ここでは判定不能なのでデフォルトにフォールバック

    # 5. デフォルト
    return (SMTP_DEFAULT_SERVER, SMTP_DEFAULT_PORT)


def _extract_spreadsheet_id(value: str) -> str:
    """スプレッドシートIDを抽出する

    URLが渡された場合はIDを抽出し、IDだけの場合はそのまま返す。
    例:
      'https://docs.google.com/spreadsheets/d/1xBcDeFgH.../edit#gid=0' → '1xBcDeFgH...'
      '1xBcDeFgH...' → '1xBcDeFgH...'
    """
    if not value:
        return ''

    # URLの場合、/d/ と次の / の間がスプレッドシートID
    match = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', value)
    if match:
        return match.group(1)

    # URLでなければそのまま返す（IDとみなす）
    return value.strip()


def _normalize_name(name: str) -> str:
    """クライアント名を正規化する

    全角スペース→半角、連続スペース→単一、前後の空白を除去。
    スプレッドシート間でのクライアント名照合時の不一致を防ぐ。
    """
    name = name.replace('\u3000', ' ')  # 全角スペース→半角
    name = re.sub(r'\s+', ' ', name)    # 連続スペース→単一
    return name.strip()


def _parse_date(date_str: str) -> Optional[datetime]:
    """日付文字列をパースする（複数フォーマット対応）"""
    formats = [
        '%Y/%m/%d %H:%M:%S',
        '%Y/%m/%d %H:%M',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y/%m/%d',
        '%Y-%m-%d',
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.replace(tzinfo=JST)
        except ValueError:
            continue
    return None


def _parse_age(age_value) -> Optional[int]:
    """年齢を整数にパースする"""
    if age_value is None or str(age_value).strip() == '':
        return None
    try:
        return int(float(str(age_value).strip()))
    except (ValueError, TypeError):
        return None
