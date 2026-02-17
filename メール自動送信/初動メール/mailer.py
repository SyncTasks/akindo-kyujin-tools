"""SMTP メール送信モジュール

アカウントごとのSMTPサーバー経由でメールを送信する。
日本語テキストメール対応。
"""

import smtplib
import time
from email.mime.text import MIMEText
from email.header import Header
from typing import Optional

from config import SMTP_TIMEOUT, SMTP_MAX_RETRIES, SMTP_RETRY_INTERVAL


def send_email(
    smtp_user: str,
    smtp_password: str,
    to_address: str,
    subject: str,
    body: str,
    smtp_server: str = 'smtp.muumuu-mail.com',
    smtp_port: int = 587,
    fallback_password: str = '',
    sender_name: str = '',
) -> bool:
    """メールを送信する

    Args:
        smtp_user: SMTP認証ユーザ（= 送信元アドレス）
        smtp_password: SMTPパスワード（「パス」列）
        to_address: 送信先アドレス
        subject: 件名
        body: 本文（プレーンテキスト）
        smtp_server: SMTPサーバーアドレス
        smtp_port: SMTPポート番号
        fallback_password: 認証失敗時に試す代替パスワード（「メールパス」列）
        sender_name: 送信者名（空の場合はメールアドレスのみ）

    Returns:
        True: 送信成功, False: 送信失敗
    """
    # メッセージ作成（日本語対応）
    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = Header(subject, 'utf-8')
    if sender_name:
        msg['From'] = f'{Header(sender_name, "utf-8").encode()} <{smtp_user}>'
    else:
        msg['From'] = smtp_user
    msg['To'] = to_address

    # 試行するパスワードのリスト（パス列 → メールパス列）
    passwords = [smtp_password]
    if fallback_password and fallback_password != smtp_password:
        passwords.append(fallback_password)

    for pw_idx, password in enumerate(passwords):
        pw_label = '「パス」列' if pw_idx == 0 else '「メールパス」列'

        for attempt in range(SMTP_MAX_RETRIES):
            try:
                with smtplib.SMTP(smtp_server, smtp_port, timeout=SMTP_TIMEOUT) as server:
                    server.starttls()
                    server.login(smtp_user, password)
                    server.send_message(msg)

                if pw_idx > 0:
                    print(f'    {pw_label}で認証成功')
                print(f'    メール送信成功: {to_address}')
                return True

            except smtplib.SMTPAuthenticationError as e:
                print(f'    SMTP認証エラー ({pw_label}): {e}')
                break  # この パスワードでの認証は諦めて次のパスワードへ

            except smtplib.SMTPRecipientsRefused as e:
                print(f'    送信先拒否エラー: {to_address} - {e}')
                return False  # 宛先エラーはリトライしない

            except smtplib.SMTPServerDisconnected:
                # 認証失敗後にサーバーが切断するケース
                print(f'    SMTP認証エラー ({pw_label}): サーバーが接続を切断')
                break  # 次のパスワードへ

            except Exception as e:
                wait_time = (attempt + 1) * SMTP_RETRY_INTERVAL
                if attempt < SMTP_MAX_RETRIES - 1:
                    print(f'    送信エラー ({attempt + 1}/{SMTP_MAX_RETRIES}): {e}')
                    print(f'    {wait_time}秒後にリトライ...')
                    time.sleep(wait_time)
                else:
                    print(f'    送信失敗（全{SMTP_MAX_RETRIES}回リトライ済み）: {e}')
                    return False

    print(f'    → 全てのパスワードで認証失敗。スプレッドシートの認証情報を確認してください')
    return False


def build_email_body(template: str, applicant: dict) -> Optional[str]:
    """テンプレートに応募者情報を差し込む

    テンプレート内の {列名} を応募者シートの対応する列の値で置換する。
    応募者シートの全列が使用可能。未定義の変数はそのまま残る。

    使用例:
      {名前}様 ご応募ありがとうございます
      {タイトル} に応募された {名前}様（{年齢}歳）

    テンプレート内の \\n リテラルは実際の改行に変換される。

    Args:
        template: テンプレート文面
        applicant: 応募者情報の辞書（columns キーに全列データ）

    Returns:
        差し込み済みの本文。テンプレートが空の場合は None。
    """
    if not template:
        return None

    # スプレッドシート上の \\n リテラルを実際の改行に変換
    body = template.replace('\\n', '\n')

    # {列名} を応募者シートの値で置換
    columns = applicant.get('columns', {})
    for col_name, col_value in columns.items():
        body = body.replace('{' + col_name + '}', col_value)

    return body
