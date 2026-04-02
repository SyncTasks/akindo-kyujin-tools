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
    OMIOKURI_DAYS,
    SHEETS_API_MAX_RETRIES,
    SHEETS_API_RETRY_INTERVAL,
    IMAP_TO_SMTP_MAP,
    DOMAIN_TO_SMTP_MAP,
    SMTP_DEFAULT_SERVER,
    SMTP_DEFAULT_PORT,
    ACCOUNT_WAIT_INTERVAL,
)

JST = timezone(timedelta(hours=9))

# ===== 応募者シートキャッシュ =====
# SS ID → (worksheet, all_values) を保持し、同じシートの再読み込みを防ぐ
_applicant_sheet_cache: Dict[str, Tuple[gspread.Worksheet, list]] = {}


def _get_applicant_sheet(client: gspread.Client, spreadsheet_id: str, description: str = "") -> Tuple[Optional[gspread.Worksheet], list]:
    """応募者シートを取得する（キャッシュあり）

    同じスプレッドシートIDに対しては1回だけAPIを呼び出し、
    2回目以降はキャッシュから返す。

    Args:
        client: gspread クライアント
        spreadsheet_id: スプレッドシートID
        description: ログ用の処理名

    Returns:
        (worksheet, all_values) のタプル。失敗時は (None, [])
    """
    if spreadsheet_id in _applicant_sheet_cache:
        print(f'  応募者シート: キャッシュ使用 (SS: {spreadsheet_id[:8]}...)')
        return _applicant_sheet_cache[spreadsheet_id]

    try:
        def _read():
            spreadsheet = client.open_by_key(spreadsheet_id)
            ws = spreadsheet.worksheet(APPLICANT_SHEET_NAME)
            return ws, ws.get_all_values()

        desc = description or f"応募者シート読み込み (SS: {spreadsheet_id})"
        worksheet, all_values = _retry_on_quota(_read, description=desc)
        _applicant_sheet_cache[spreadsheet_id] = (worksheet, all_values)
        return worksheet, all_values
    except Exception as e:
        print(f'応募者シート読み込みエラー (SS ID: {spreadsheet_id}): {type(e).__name__}: {e}')
        return None, []


def clear_applicant_sheet_cache():
    """応募者シートキャッシュをクリアする"""
    _applicant_sheet_cache.clear()


# ===== Sheets API リトライヘルパー =====

def _retry_on_quota(func, *args, description="Sheets API呼び出し", **kwargs):
    """Google Sheets API のレート制限(429)エラー時にリトライする汎用ヘルパー

    Args:
        func: 実行する関数
        *args: 関数に渡す引数
        description: ログに表示する処理名
        **kwargs: 関数に渡すキーワード引数

    Returns:
        関数の戻り値

    Raises:
        最後のリトライでも失敗した場合は元の例外をそのまま送出
    """
    last_exception = None
    for attempt in range(SHEETS_API_MAX_RETRIES):
        try:
            return func(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            if '429' in str(e) or 'RATE_LIMIT' in str(e) or 'Quota exceeded' in str(e):
                last_exception = e
                wait = (attempt + 1) * SHEETS_API_RETRY_INTERVAL
                print(f'  Sheets APIレート制限 ({description}): {wait}秒後にリトライ ({attempt + 1}/{SHEETS_API_MAX_RETRIES})')
                time.sleep(wait)
            else:
                raise
    raise last_exception


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
        def _read_config():
            spreadsheet = client.open_by_key(CONFIG_SPREADSHEET_ID)
            worksheet = spreadsheet.worksheet(CONFIG_SHEET_NAME)
            return worksheet.get_all_records()

        records = _retry_on_quota(_read_config, description="設定SS読み込み")
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

        email = str(record.get('メール送信アドレス', '')).strip()
        password = str(record.get('メール送信アドレス_パス', '')).strip()
        mail_password = str(record.get('メールパス', '')).strip()
        client_name = _normalize_name(str(record.get('クライアント名', '')))
        template_ss_id = _extract_spreadsheet_id(str(record.get('メール文面', '')).strip())
        imap_server = str(record.get('IMAP', '')).strip()

        if not email or (not password and not mail_password):
            print(f'  警告: {client_name} のメール/パスが未設定、スキップ')
            continue

        if not template_ss_id:
            print(f'  警告: {client_name} のメール文面（スプレッドシートID）が未設定、スキップ')
            continue

        media_name = _normalize_media_name(str(record.get('媒体名', '')))

        # SMTPサーバーを判定（IMAP列 → メールドメイン → デフォルト）
        smtp_server, smtp_port = _resolve_smtp(imap_server, email)

        accounts.append({
            'email': email,
            'password': password,
            'mail_password': mail_password,
            'client_name': client_name,
            'media_name': media_name,
            'template_spreadsheet_id': template_ss_id,
            'smtp_server': smtp_server,
            'smtp_port': smtp_port,
        })

    print(f'メール送信対象アカウント: {len(accounts)}件')
    for i, acc in enumerate(accounts):
        print(f'  {i + 1}. {acc["client_name"]} ({acc["email"]}) → SMTP: {acc["smtp_server"]}:{acc["smtp_port"]}')

    return accounts


# ===== 送信済みアドレス事前収集 =====

def collect_all_sent_emails(
    client: gspread.Client,
    accounts: List[dict],
) -> set:
    """全アカウントの応募者シートから送信済み・お見送り済みのメールアドレスを収集する

    実行をまたぐクライアント横断の重複送信防止に使用する。

    Args:
        client: gspread クライアント
        accounts: アカウント情報のリスト

    Returns:
        送信済みメールアドレスのセット（小文字正規化済み）
    """
    sent_emails = set()
    # 同じSSを重複して読まないようにする
    seen_ss_ids = set()

    for account in accounts:
        ss_id = account['template_spreadsheet_id']
        if ss_id in seen_ss_ids:
            continue
        seen_ss_ids.add(ss_id)

        worksheet, all_values = _get_applicant_sheet(
            client, ss_id, description=f"送信済み収集 (SS: {ss_id})"
        )
        if worksheet is None or len(all_values) < 2:
            continue

        headers = all_values[0]
        email_col = -1
        sent_col = -1
        omiokuri_col = -1
        try:
            email_col = headers.index('メールアドレス')
        except ValueError:
            continue
        try:
            sent_col = headers.index('メール送信済')
        except ValueError:
            pass
        try:
            omiokuri_col = headers.index('お見送り')
        except ValueError:
            pass

        count = 0
        for row in all_values[1:]:
            email_addr = str(row[email_col]).strip().lower() if email_col < len(row) else ''
            if not email_addr:
                continue
            sent_flag = str(row[sent_col]).strip() if sent_col >= 0 and sent_col < len(row) else ''
            omiokuri_flag = str(row[omiokuri_col]).strip() if omiokuri_col >= 0 and omiokuri_col < len(row) else ''
            if sent_flag or omiokuri_flag:
                sent_emails.add(email_addr)
                count += 1

        print(f'  SS {ss_id[:8]}...: 送信済み/お見送り {count}件')
        time.sleep(ACCOUNT_WAIT_INTERVAL)

    print(f'  全クライアント合計: 送信済みアドレス {len(sent_emails)}件')
    return sent_emails


# ===== 応募者シート読み込み =====

def get_unsent_applicants(
    client: gspread.Client,
    spreadsheet_id: str,
    global_sent_emails: Optional[set] = None,
) -> Tuple[Optional[gspread.Worksheet], List[dict], List[str], set]:
    """応募者シートから未送信 & 直近N日以内の応募者を取得する

    Args:
        client: gspread クライアント
        spreadsheet_id: テンプレート＆応募者スプレッドシートのID

    Returns:
        (worksheet, applicants, headers) のタプル
        - worksheet: 応募者シートの Worksheet オブジェクト（送信済み更新用）
        - applicants: 未送信応募者のリスト
        - headers: ヘッダー行のリスト
    """
    worksheet, all_values = _get_applicant_sheet(
        client, spreadsheet_id, description=f"応募者シート読み込み (SS: {spreadsheet_id})"
    )
    if worksheet is None:
        return None, [], [], set(), []

    if len(all_values) < 2:
        print(f'応募者シート: データ行がありません')
        return worksheet, [], [], set(), []

    # ヘッダー行から必要な列のインデックスを特定
    headers = all_values[0]
    required_cols = ['メール送信済', '応募日時', '名前', '年齢', 'メールアドレス', 'クライアント名', 'クライアント', 'タイトル', '媒体', 'お見送り']
    col_map = {}
    for col_name in required_cols:
        try:
            col_map[col_name] = headers.index(col_name)
        except ValueError:
            col_map[col_name] = -1

    # 必須列の存在チェック
    if col_map['メールアドレス'] < 0:
        print(f'応募者シート: 「メールアドレス」列が見つかりません')
        return worksheet, [], headers, set(), []

    data_rows = all_values[1:]

    # 直近N日の判定基準: 現在時刻からSEARCH_DAYS×24時間前
    now = datetime.now(JST)
    cutoff = now - timedelta(days=SEARCH_DAYS)

    applicants = []
    duplicate_rows = []  # 重複と判定された行（お見送り○マーク用）
    skipped_sent = 0
    skipped_old = 0
    skipped_no_email = 0
    skipped_already_contacted = 0

    def _get(row, col_name):
        i = col_map[col_name]
        if i < 0 or i >= len(row):
            return ''
        return str(row[i]).strip()

    # 過去に送信済み or お見送り済みのメールアドレスを収集（重複送信防止）
    sent_emails = set(global_sent_emails) if global_sent_emails else set()
    local_sent_count = 0
    email_col = col_map['メールアドレス']
    sent_col = col_map['メール送信済']
    omiokuri_col = col_map.get('お見送り', -1)
    if email_col >= 0:
        for row in data_rows:
            sent_flag = str(row[sent_col]).strip() if sent_col >= 0 and sent_col < len(row) else ''
            omiokuri_flag = str(row[omiokuri_col]).strip() if omiokuri_col >= 0 and omiokuri_col < len(row) else ''
            email_addr = str(row[email_col]).strip().lower() if email_col < len(row) else ''
            # メール送信済 or お見送り（○/済）があるアドレスは重複対象
            if email_addr and (sent_flag or omiokuri_flag):
                sent_emails.add(email_addr)
                local_sent_count += 1
    if sent_emails:
        cross_count = len(sent_emails) - local_sent_count
        msg = f'  過去送信済み/お見送りメールアドレス: {local_sent_count}件（このシート）'
        if cross_count > 0:
            msg += f' + {cross_count}件（他クライアント）'
        msg += '（これらには送信しません）'
        print(msg)

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

        # 過去に送信済みのメールアドレスはスキップ（再応募の重複送信防止）
        if email_address.lower() in sent_emails:
            skipped_already_contacted += 1
            duplicate_rows.append({'row_index': row_index, 'name': _get(row, '名前'), 'email': email_address})
            print(f'  行{row_index}: 過去送信済みアドレス、スキップ ({_get(row, "名前")}: {email_address})')
            continue

        # 年齢を取得
        age = _parse_age(_get(row, '年齢'))

        # 全列のデータを列名→値の辞書として格納
        columns = {}
        for col_idx, col_name in enumerate(headers):
            if col_name and col_idx < len(row):
                columns[col_name] = str(row[col_idx]).strip()

        applicants.append({
            'row_index': row_index,
            'name': _get(row, '名前'),
            'age': age,
            'email_address': email_address,
            'client_name': _normalize_name(_get(row, 'クライアント')),
            'media_name': _normalize_media_name(_get(row, '媒体')),
            'title': _get(row, 'タイトル'),
            'application_date': date_str,
            'columns': columns,
        })

    print(f'応募者シート読み込み完了 (SS ID: {spreadsheet_id})')
    print(f'  全{len(data_rows)}件 → 未送信&直近{SEARCH_DAYS}日: {len(applicants)}件')
    print(f'  スキップ内訳: 送信済={skipped_sent}, 過去送信済アドレス={skipped_already_contacted}, 期間外={skipped_old}, メールなし={skipped_no_email}')

    return worksheet, applicants, headers, sent_emails, duplicate_rows


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
        - sender_name: 送信者名（「送信者名」列。なければ空）
        - subject: 件名テンプレート（「件名」列。なければデフォルト使用）
        - under_35: 34歳以下向けテンプレート文面
        - over_35: 35歳以上向けテンプレート文面
    """
    try:
        def _read_templates():
            spreadsheet = client.open_by_key(spreadsheet_id)
            ws = spreadsheet.worksheet(MAIL_TEMPLATE_SHEET_NAME)
            return ws.get_all_values()

        all_values = _retry_on_quota(
            _read_templates, description=f"メール管理シート読み込み (SS: {spreadsheet_id})"
        )
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
    for col_name in ['クライアント名', '送信者名', '件名', '35歳以下', '36歳以上', 'お見送り', '35歳以下男性']:
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

        sender_name = _get_cell('送信者名')
        subject = _get_cell('件名')
        under_35 = _get_cell('35歳以下')
        over_35 = _get_cell('36歳以上')
        omiokuri = _get_cell('お見送り')
        under_35_male = _get_cell('35歳以下男性')

        templates[client_name] = {
            'sender_name': sender_name,
            'subject': subject,
            'under_35': under_35,
            'over_35': over_35,
            'omiokuri': omiokuri,
            'under_35_male': under_35_male,
        }

    print(f'メール管理シート読み込み完了: {len(templates)}件のテンプレート')
    for name in templates:
        has_subject = '○' if templates[name]['subject'] else '×'
        has_under = '○' if templates[name]['under_35'] else '×'
        has_over = '○' if templates[name]['over_35'] else '×'
        has_omiokuri = '○' if templates[name]['omiokuri'] else '×'
        print(f'  {name}: 件名={has_subject}, 35歳以下={has_under}, 36歳以上={has_over}, お見送り={has_omiokuri}')

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


def mark_as_omiokuri(
    worksheet: gspread.Worksheet,
    row_index: int,
    headers: List[str],
) -> bool:
    """応募者シートの「お見送り」列に○を書き込む（リトライ付き）

    他クライアントで既に送信済みのアドレスなど、重複と判定された場合に使用。

    Args:
        worksheet: 応募者シートの Worksheet オブジェクト
        row_index: 更新する行番号（1-indexed）
        headers: ヘッダー行のリスト

    Returns:
        True: 更新成功, False: 更新失敗
    """
    try:
        col_index = headers.index('お見送り') + 1  # gspread は 1-indexed
    except ValueError:
        print(f'  警告: 「お見送り」列がヘッダーに見つかりません（お見送り更新をスキップ）')
        return False

    for attempt in range(SHEETS_API_MAX_RETRIES):
        try:
            worksheet.update_cell(row_index, col_index, '○')
            return True
        except gspread.exceptions.APIError as e:
            if '429' in str(e) or 'RATE_LIMIT' in str(e):
                wait = (attempt + 1) * SHEETS_API_RETRY_INTERVAL
                print(f'  Sheets APIレート制限 (お見送り更新): {wait}秒後にリトライ ({attempt + 1}/{SHEETS_API_MAX_RETRIES})')
                time.sleep(wait)
            else:
                print(f'  エラー: 行{row_index}のお見送り更新に失敗 (API): {e}')
                return False
        except Exception as e:
            print(f'  エラー: 行{row_index}のお見送り更新に失敗: {e}')
            return False

    print(f'  エラー: 行{row_index}のお見送り更新がリトライ上限に達しました')
    return False


def mark_omiokuri_sent(
    worksheet: gspread.Worksheet,
    row_index: int,
    headers: List[str],
) -> bool:
    """応募者シートの「お見送り」列を○→済に更新する（リトライ付き）

    お見送りメール送信成功後に呼び出す。

    Args:
        worksheet: 応募者シートの Worksheet オブジェクト
        row_index: 更新する行番号（1-indexed）
        headers: ヘッダー行のリスト

    Returns:
        True: 更新成功, False: 更新失敗
    """
    try:
        col_index = headers.index('お見送り') + 1
    except ValueError:
        print(f'  エラー: 「お見送り」列がヘッダーに見つかりません')
        return False

    for attempt in range(SHEETS_API_MAX_RETRIES):
        try:
            worksheet.update_cell(row_index, col_index, '済')
            return True
        except gspread.exceptions.APIError as e:
            if '429' in str(e) or 'RATE_LIMIT' in str(e):
                wait = (attempt + 1) * SHEETS_API_RETRY_INTERVAL
                print(f'  Sheets APIレート制限 (お見送り済更新): {wait}秒後にリトライ ({attempt + 1}/{SHEETS_API_MAX_RETRIES})')
                time.sleep(wait)
            else:
                print(f'  エラー: 行{row_index}のお見送り済更新に失敗 (API): {e}')
                return False
        except Exception as e:
            print(f'  エラー: 行{row_index}のお見送り済更新に失敗: {e}')
            return False

    print(f'  エラー: 行{row_index}のお見送り済更新がリトライ上限に達しました')
    return False


def get_omiokuri_applicants(
    client: gspread.Client,
    spreadsheet_id: str,
) -> Tuple[Optional[gspread.Worksheet], List[dict], List[str]]:
    """応募者シートから お見送り=○ かつ 応募日時から2日以上経過した行を取得する

    Args:
        client: gspread クライアント
        spreadsheet_id: スプレッドシートのID

    Returns:
        (worksheet, applicants, headers) のタプル
    """
    worksheet, all_values = _get_applicant_sheet(
        client, spreadsheet_id, description=f"応募者シート読み込み/お見送り (SS: {spreadsheet_id})"
    )
    if worksheet is None:
        return None, [], []

    if len(all_values) < 2:
        return worksheet, [], []

    headers = all_values[0]
    col_map = {}
    for col_name in ['お見送り', '応募日時', '名前', '年齢', 'メールアドレス', 'クライアント', 'タイトル', '媒体']:
        try:
            col_map[col_name] = headers.index(col_name)
        except ValueError:
            col_map[col_name] = -1

    if col_map['お見送り'] < 0:
        print(f'  応募者シート: 「お見送り」列が見つかりません')
        return worksheet, [], headers

    data_rows = all_values[1:]
    now = datetime.now(JST)
    cutoff = now - timedelta(days=OMIOKURI_DAYS)

    def _get(row, col_name):
        i = col_map[col_name]
        if i < 0 or i >= len(row):
            return ''
        return str(row[i]).strip()

    applicants = []
    for i, row in enumerate(data_rows):
        row_index = i + 2

        # お見送り=○ のみ対象（済やそれ以外はスキップ）
        omiokuri_flag = _get(row, 'お見送り')
        if omiokuri_flag != '○':
            continue

        # 応募日時から2日以上経過しているか
        date_str = _get(row, '応募日時')
        if not date_str:
            continue
        app_date = _parse_date(date_str)
        if app_date is None:
            continue
        if app_date > cutoff:
            continue  # まだ2日経っていない

        email_address = _get(row, 'メールアドレス')
        if not email_address:
            continue

        columns = {}
        for col_idx, col_name in enumerate(headers):
            if col_name and col_idx < len(row):
                columns[col_name] = str(row[col_idx]).strip()

        applicants.append({
            'row_index': row_index,
            'name': _get(row, '名前'),
            'age': _parse_age(_get(row, '年齢')),
            'email_address': email_address,
            'client_name': _normalize_name(_get(row, 'クライアント')),
            'media_name': _normalize_media_name(_get(row, '媒体')),
            'title': _get(row, 'タイトル'),
            'application_date': date_str,
            'columns': columns,
        })

    if applicants:
        print(f'  お見送りメール対象: {len(applicants)}件（お見送り=○ & 応募から{OMIOKURI_DAYS}日以上経過）')

    return worksheet, applicants, headers


# ===== テンプレート選択 =====

def select_template(age: Optional[int], templates: dict, gender: str = '') -> Optional[str]:
    """年齢・性別に応じたテンプレートを選択する

    Args:
        age: 応募者の年齢（None の場合は判定不可）
        templates: テンプレート辞書（under_35, over_35, under_35_male）
        gender: 応募者の性別（「男性」など）

    Returns:
        テンプレート文面。選択不可の場合は None。
    """
    if age is None:
        # 年齢不明の場合は 34歳以下をデフォルトとする
        print(f'    警告: 年齢不明のため「34歳以下」テンプレートをデフォルト使用')
        return templates.get('under_35') or None

    if age <= 35:
        if gender == '男性' and templates.get('under_35_male'):
            return templates['under_35_male']
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


# 媒体名の表記揺れマッピング（キーは小文字で統一）
_MEDIA_NAME_MAP = {
    'airwork': 'エアワーク',
    'エアワーク': 'エアワーク',
    'engage': 'engage',
    'ind': 'Indeed',
    'indeed': 'Indeed',
    'kbx': '求人ボックス',
    '求人ボックス': '求人ボックス',
    'ジョブオレ': 'ジョブオレ',
    'ジモティ': 'ジモティ',
}


def _normalize_media_name(name: str) -> str:
    """媒体名を正規化する（略称・表記揺れを統一）

    応募者シート（AirWork, IND, KBX 等）と設定シート（エアワーク, Indeed, 求人ボックス 等）
    の表記差を吸収する。
    """
    name = name.strip()
    if not name:
        return ''
    return _MEDIA_NAME_MAP.get(name.lower(), _MEDIA_NAME_MAP.get(name, name))


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
