"""初動メール自動送信スクリプト

応募者シートの未送信行を検出し、年齢に応じたテンプレートで
自動返信メールを送信する。

フロー:
  1. 設定SSから メール送信=TRUE のアカウント一覧を取得
  2. 各アカウントの「メール文面」列 → テンプレート&応募者SS へ
  3. 応募者シートから メール送信済=空 & 応募日時が直近1日以内 の行を取得
  4. メール管理シートから クライアント名で照合 → 文面を取得
  5. 年齢で「34歳以下」or「35歳以上」テンプレートを選択
  6. SMTP送信
  7. 応募者シートの「メール送信済」列を更新
"""

import argparse
import os
import sys
import time
import traceback
from datetime import datetime, timezone, timedelta

from config import SEARCH_DAYS, API_WAIT_INTERVAL
from logger import setup_logging, teardown_logging, JST
from sheets import (
    get_sheets_client,
    get_active_accounts,
    get_unsent_applicants,
    get_mail_templates,
    mark_as_sent,
    select_template,
)
from mailer import send_email, build_email_body


def process_account(
    sheets_client,
    account: dict,
    dry_run: bool = False,
) -> dict:
    """1つのアカウントを処理する

    Args:
        sheets_client: gspread クライアント
        account: アカウント情報
        dry_run: True の場合、メール送信をスキップ

    Returns:
        処理結果の辞書:
        - sent: 送信成功件数
        - skipped_no_template: テンプレートなしでスキップした件数
        - skipped_empty_body: 本文が空でスキップした件数
        - failed: 送信失敗件数
        - update_failed: 送信成功だがSS更新失敗の件数
    """
    client_name = account['client_name']
    email = account['email']
    password = account['password']
    ss_id = account['template_spreadsheet_id']
    smtp_server = account.get('smtp_server', 'smtp.muumuu-mail.com')
    smtp_port = account.get('smtp_port', 587)

    result = {
        'sent': 0,
        'skipped_no_template': 0,
        'skipped_empty_body': 0,
        'failed': 0,
        'update_failed': 0,
    }

    print(f'\n{"=" * 60}')
    print(f'アカウント処理開始: {client_name} ({email})')
    print(f'テンプレートSS: {ss_id}')
    print(f'SMTPサーバー: {smtp_server}:{smtp_port}')
    print(f'{"=" * 60}')

    # 応募者シートから未送信行を取得
    worksheet, applicants = get_unsent_applicants(sheets_client, ss_id)
    if worksheet is None:
        print(f'  応募者シートの読み込みに失敗しました')
        return result

    if not applicants:
        print(f'  未送信の応募者はいません')
        return result

    # ヘッダー行を取得（送信済み更新で列位置を特定するため）
    headers = worksheet.row_values(1)

    # メール管理シートからテンプレートを取得
    templates = get_mail_templates(sheets_client, ss_id)
    if not templates:
        print(f'  メール管理シートにテンプレートが見つかりません')
        result['skipped_no_template'] = len(applicants)
        return result

    # 応募者ごとに処理
    for applicant in applicants:
        row = applicant['row_index']
        name = applicant['name']
        age = applicant['age']
        to_address = applicant['email_address']
        applicant_client = applicant['client_name']
        title = applicant['title']
        app_date = applicant['application_date']

        age_label = f'{age}歳' if age is not None else '不明'
        print(f'\n  --- 行{row}: {name} ({age_label}) ---')
        print(f'  宛先: {to_address}')
        print(f'  クライアント名: {applicant_client}')
        print(f'  求人タイトル: {title}')
        print(f'  応募日時: {app_date}')

        # クライアント名でテンプレートを照合
        client_templates = templates.get(applicant_client)
        if not client_templates:
            print(f'    テンプレートなし: クライアント名「{applicant_client}」が'
                  f'メール管理シートに見つかりません → スキップ')
            result['skipped_no_template'] += 1
            continue

        # 年齢に応じたテンプレートを選択
        age_category = '34歳以下' if (age is None or age <= 34) else '35歳以上'
        template_text = select_template(age, client_templates)

        if not template_text:
            print(f'    テンプレートが空: {age_category}列が空です → スキップ')
            result['skipped_empty_body'] += 1
            continue

        print(f'    テンプレート選択: {age_category}')

        # 本文を構築
        body = build_email_body(template_text, applicant)
        if not body:
            print(f'    本文構築失敗 → スキップ')
            result['skipped_empty_body'] += 1
            continue

        # 件名を構築（メール管理シートの「件名」列 → なければデフォルト）
        subject = _build_subject(applicant, client_templates.get('subject', ''))

        print(f'    件名: {subject}')
        body_preview = body[:100] + '...' if len(body) > 100 else body
        print(f'    本文プレビュー: {body_preview}')

        # dry-run モード
        if dry_run:
            print(f'    [DRY-RUN] メール送信をスキップ')
            result['sent'] += 1
            continue

        # メール送信
        success = send_email(
            smtp_user=email,
            smtp_password=password,
            to_address=to_address,
            subject=subject,
            body=body,
            smtp_server=smtp_server,
            smtp_port=smtp_port,
        )

        if success:
            # 送信済みフラグを更新
            update_ok = mark_as_sent(worksheet, row, headers)
            if update_ok:
                print(f'    送信完了 & 送信済み更新')
                result['sent'] += 1
            else:
                print(f'    警告: メール送信済みだが、スプレッドシート更新に失敗')
                print(f'    → 次回実行時に重複送信の可能性があります')
                result['update_failed'] += 1

            # API レート制限対策（1通ごとに待機）
            time.sleep(API_WAIT_INTERVAL)
        else:
            print(f'    送信失敗')
            result['failed'] += 1

    return result


def _build_subject(applicant: dict, subject_template: str = '') -> str:
    """メール件名を構築する

    メール管理シートの「件名」列にテンプレートがあればそれを使い、
    なければデフォルト件名を使用する。

    件名テンプレート内でも $name, $title 等のプレースホルダーが使用可能。

    Args:
        applicant: 応募者情報
        subject_template: メール管理シートの件名テンプレート（空ならデフォルト）

    Returns:
        件名文字列
    """
    if subject_template:
        from string import Template
        try:
            t = Template(subject_template)
            return t.safe_substitute(
                name=applicant.get('name', ''),
                title=applicant.get('title', ''),
                age=str(applicant.get('age', '')) if applicant.get('age') is not None else '',
                client_name=applicant.get('client_name', ''),
            )
        except Exception:
            pass  # テンプレート展開失敗時はデフォルトにフォールバック

    # デフォルト件名
    title = applicant.get('title', '')
    if title:
        return f'ご応募ありがとうございます【{title}】'
    return 'ご応募ありがとうございます'


def main():
    """メインエントリーポイント"""
    # コマンドライン引数の解析
    parser = argparse.ArgumentParser(
        description='初動メール自動送信 - 応募者シートの未送信行にメールを自動送信',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='実際にメールを送信せず、処理内容を表示のみ行う',
    )
    parser.add_argument(
        '--account',
        type=str,
        default=None,
        help='処理対象のクライアント名（指定しない場合は全アカウント）',
    )
    args = parser.parse_args()

    main_start = time.time()

    # ヘッダー表示
    print('=' * 60)
    print('初動メール自動送信')
    print(f'実行日時: {datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S")} (JST)')
    print(f'対象期間: 直近{SEARCH_DAYS}日以内の応募')
    if args.dry_run:
        print('モード: DRY-RUN（メール送信なし）')
    if args.account:
        print(f'対象アカウント: {args.account}')
    print('=' * 60)

    # Google Sheets 認証
    print('\n[初期化] Google Sheets 認証中...')
    sheets_client = get_sheets_client()
    if not sheets_client:
        print('エラー: Google Sheets 認証に失敗しました')
        return

    # アカウント一覧を取得
    print('\n[初期化] アカウント一覧取得中...')
    accounts = get_active_accounts(sheets_client)
    if not accounts:
        print('エラー: メール送信対象のアカウントが見つかりません')
        return

    # 特定アカウントのみ処理する場合
    if args.account:
        accounts = [a for a in accounts if a['client_name'] == args.account]
        if not accounts:
            print(f'エラー: クライアント名「{args.account}」が見つかりません')
            return

    init_elapsed = time.time() - main_start
    print(f'\n[初期化完了] ({init_elapsed:.1f}秒)')

    # アカウントごとに処理
    total_sent = 0
    total_skipped_template = 0
    total_skipped_body = 0
    total_failed = 0
    total_update_failed = 0

    for account in accounts:
        try:
            result = process_account(sheets_client, account, dry_run=args.dry_run)
            total_sent += result['sent']
            total_skipped_template += result['skipped_no_template']
            total_skipped_body += result['skipped_empty_body']
            total_failed += result['failed']
            total_update_failed += result['update_failed']
        except Exception as e:
            print(f'\n[{account["client_name"]}] 致命的エラー:')
            print(traceback.format_exc())

    # サマリー表示
    total_elapsed = time.time() - main_start
    print(f'\n{"=" * 60}')
    print('全処理完了')
    print(f'  送信成功: {total_sent}件')
    if total_update_failed:
        print(f'  送信済み&SS更新失敗: {total_update_failed}件 ← 要確認')
    print(f'  テンプレートなし: {total_skipped_template}件')
    print(f'  本文なし: {total_skipped_body}件')
    print(f'  送信失敗: {total_failed}件')
    print(f'  総実行時間: {total_elapsed:.1f}秒')
    print(f'  終了時刻: {datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S")} (JST)')
    print(f'{"=" * 60}')


if __name__ == '__main__':
    _script_dir = os.path.dirname(os.path.abspath(__file__))

    # .env ファイルを読み込み
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(_script_dir, '.env'))
    except ImportError:
        pass  # python-dotenv がなくても環境変数で動作可能

    # ログ設定
    tee = None
    try:
        tee = setup_logging(_script_dir)
        main()
    except Exception:
        print(f'\n予期しないエラー:')
        print(traceback.format_exc())
    finally:
        if tee:
            teardown_logging(tee)
