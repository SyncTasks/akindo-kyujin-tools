"""ログ出力モジュール

画面とファイルの両方にログを出力する。
ログファイルは日付ごとに作成し、保管期間を超えたものは自動削除する。
"""

import os
import sys
from datetime import datetime, timezone, timedelta

from config import LOG_RETENTION_DAYS

JST = timezone(timedelta(hours=9))


class TeeWriter:
    """stdoutとログファイルの両方に書き込む"""

    def __init__(self, log_path: str):
        self._stdout = sys.stdout
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        self._file = open(log_path, 'a', encoding='utf-8')

    def write(self, text: str):
        self._stdout.write(text)
        self._stdout.flush()
        self._file.write(text)
        self._file.flush()

    def flush(self):
        self._stdout.flush()
        self._file.flush()

    def close(self):
        self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


def setup_logging(base_dir: str) -> TeeWriter:
    """ログ出力を初期化する

    Args:
        base_dir: スクリプトのベースディレクトリ

    Returns:
        TeeWriter インスタンス
    """
    log_dir = os.path.join(base_dir, 'logs')
    now = datetime.now(JST)
    log_path = os.path.join(log_dir, f'{now.strftime("%Y%m%d")}.log')

    # 保管期間を超えたログを削除
    cleanup_old_logs(log_dir)

    # TeeWriter を設定（同日は追記）
    tee = TeeWriter(log_path)
    sys.stdout = tee
    sys.stderr = tee

    # 実行ごとの区切り線をログファイルに追記
    tee._file.write(f'\n{"─" * 60}\n')
    tee._file.write(f'実行開始: {now.strftime("%Y/%m/%d %H:%M:%S")}\n')
    tee._file.write(f'{"─" * 60}\n')
    tee._file.flush()

    return tee


def cleanup_old_logs(log_dir: str):
    """保管期間を超えたログファイルを削除する

    保管期間の定義: 今日と昨日のログを保持し、一昨日以前を削除する。
    （LOG_RETENTION_DAYS=1 の場合）

    Args:
        log_dir: ログディレクトリ
    """
    if not os.path.isdir(log_dir):
        return

    now = datetime.now(JST)
    # 日付の境界を 0:00 に揃える（時刻に依存しない判定）
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = today - timedelta(days=LOG_RETENTION_DAYS)

    for filename in os.listdir(log_dir):
        if not filename.endswith('.log'):
            continue
        # ファイル名の先頭8文字を日付として解釈
        file_date_str = filename[:8]
        try:
            file_date = datetime.strptime(file_date_str, '%Y%m%d').replace(tzinfo=JST)
            if file_date < cutoff:
                os.remove(os.path.join(log_dir, filename))
        except ValueError:
            continue


def teardown_logging(tee: TeeWriter):
    """ログ出力を終了する

    Args:
        tee: TeeWriter インスタンス
    """
    sys.stdout = tee._stdout
    sys.stderr = tee._stdout
    tee.close()
