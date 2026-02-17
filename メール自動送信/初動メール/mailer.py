"""SMTP メール送信モジュール

アカウントごとのSMTPサーバー経由でメールを送信する。
日本語テキストメール対応。
"""

import smtplib
import time
from email.mime.text import MIMEText
from email.header import Header
from string import Template
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
) -> bool:
    """メールを送信する

    Args:
        smtp_user: SMTP認証ユーザ（= 送信元アドレス）
        smtp_password: SMTPパスワード
        to_address: 送信先アドレス
        subject: 件名
        body: 本文（プレーンテキスト）
        smtp_server: SMTPサーバーアドレス
        smtp_port: SMTPポート番号

    Returns:
        True: 送信成功, False: 送信失敗
    """
    # メッセージ作成（日本語対応）
    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = Header(subject, 'utf-8')
    msg['From'] = smtp_user
    msg['To'] = to_address

    for attempt in range(SMTP_MAX_RETRIES):
        try:
            with smtplib.SMTP(smtp_server, smtp_port, timeout=SMTP_TIMEOUT) as server:
                server.starttls()
                server.login(smtp_user, smtp_password)
                server.send_message(msg)

            print(f'    メール送信成功: {to_address}')
            return True

        except smtplib.SMTPAuthenticationError as e:
            print(f'    SMTP認証エラー: {e}')
            print(f'    → メールアドレスまたはパスワードを確認してください')
            return False  # 認証エラーはリトライしない

        except smtplib.SMTPRecipientsRefused as e:
            print(f'    送信先拒否エラー: {to_address} - {e}')
            return False  # 宛先エラーはリトライしない

        except Exception as e:
            wait_time = (attempt + 1) * SMTP_RETRY_INTERVAL
            if attempt < SMTP_MAX_RETRIES - 1:
                print(f'    送信エラー ({attempt + 1}/{SMTP_MAX_RETRIES}): {e}')
                print(f'    {wait_time}秒後にリトライ...')
                time.sleep(wait_time)
            else:
                print(f'    送信失敗（全{SMTP_MAX_RETRIES}回リトライ済み）: {e}')
                return False

    return False


def build_email_body(template: str, applicant: dict) -> Optional[str]:
    """テンプレートに応募者情報を差し込む

    テンプレート内のプレースホルダー（$変数名）を応募者情報で置換する。
    未定義の変数はそのまま残る（safe_substitute）。

    使用可能なプレースホルダー:
      $name       - 応募者名
      $title      - 求人タイトル
      $age        - 年齢
      $client_name - クライアント名

    テンプレート内の \\n リテラルは実際の改行に変換される。

    Args:
        template: テンプレート文面
        applicant: 応募者情報の辞書

    Returns:
        差し込み済みの本文。テンプレートが空の場合は None。
    """
    if not template:
        return None

    # スプレッドシート上の \\n リテラルを実際の改行に変換
    body = template.replace('\\n', '\n')

    # プレースホルダー置換（$name, $title 等）
    # safe_substitute: 未定義の変数はそのまま残す（KeyError にならない）
    try:
        t = Template(body)
        body = t.safe_substitute(
            name=applicant.get('name', ''),
            title=applicant.get('title', ''),
            age=str(applicant.get('age', '')) if applicant.get('age') is not None else '',
            client_name=applicant.get('client_name', ''),
        )
    except Exception as e:
        print(f'    警告: テンプレート変数置換エラー: {e}')

    return body
