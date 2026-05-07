import os
import requests
import pymysql
import hashlib
import threading
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from ftp_tool import FtpUtil

urllib3.disable_warnings()

# ====================== 数据库配置 ======================
db_config = {
    'host': '192.168.120.45',
    'user': 'arrow',
    'password': 'arrowq1w2Q!W@',
    'database': 'file_info',
    'charset': 'utf8mb4',
    'autocommit': False,
}

# ====================== 基础配置 ======================
SAVE_DIR = "test_pdfs"
os.makedirs(SAVE_DIR, exist_ok=True)

MAX_WORKERS = 6
BATCH_SIZE = 100

CONNECT_TIMEOUT = 5
READ_TIMEOUT = 30

MAX_PDF_SIZE = 30 * 1024 * 1024  # 30MB

FTP_SEMAPHORE = threading.Semaphore(3)

SKIP_DOMAINS = [
    "wpc.ac62.edgecastcdn.net",
]

# ====================== 线程局部变量 ======================
_thread_local = threading.local()
_ftp_local = threading.local()

# ====================== Session ======================
def get_session():

    if not hasattr(_thread_local, "session"):

        session = requests.Session()

        adapter = HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=0,
            pool_block=False
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "Chrome/124.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Connection": "keep-alive",
        })

        session.trust_env = False

        _thread_local.session = session

    return _thread_local.session

# ====================== FTP ======================
def get_ftp():

    try:

        if not hasattr(_ftp_local, 'conn') or _ftp_local.conn is None:

            ftp = FtpUtil(
                "192.168.10.222",
                "王建雷",
                "Aa123123"
            )

            _ftp_local.conn = ftp

        return _ftp_local.conn

    except Exception:

        _ftp_local.conn = None

        return None

# ====================== FTP 上传 ======================
def upload_with_retry(local_path, remote_path):

    try:

        with FTP_SEMAPHORE:

            ftp = get_ftp()

            if not ftp:
                return False

            ftp.upload_file(local_path, remote_path)

        return True

    except Exception as e:

        print(f"❌ FTP失败: {e}")

        _ftp_local.conn = None

        return False

# ====================== 下载 worker ======================
def download_and_upload_worker(url):

    url = url.strip()

    if not url.startswith(("http://", "https://")):
        return {'status': 2}

    lower_url = url.lower()

    try:

        # 跳过域名
        if any(domain in lower_url for domain in SKIP_DOMAINS):
            return {'status': 2}

        # 跳过 html
        if ".html" in lower_url or ".xhtml" in lower_url:
            return {'status': 2}

        session = get_session()

        # ====================== HEAD 检测 ======================
        try:

            head = session.head(
                url,
                timeout=(5, 10),
                verify=False,
                allow_redirects=True
            )

            content_length = int(
                head.headers.get("Content-Length", 0)
            )

            # 文件过大直接跳过
            if content_length > MAX_PDF_SIZE:

                print(f"⚠️ 文件过大跳过: {url}")

                return {'status': 2}

        except:
            pass

        # ====================== GET 下载 ======================
        with session.get(
            url,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            verify=False,
            stream=True,
            allow_redirects=True
        ) as res:

            if res.status_code != 200:

                print(f"❌ 状态码异常: {res.status_code} -> {url}")

                return {'status': 2}

            content = bytearray()

            for chunk in res.iter_content(chunk_size=8192):

                if not chunk:
                    continue

                content.extend(chunk)

                # 超过大小限制
                if len(content) > MAX_PDF_SIZE:

                    print(f"⚠️ 下载超限: {url}")

                    return {'status': 2}

            # 校验 PDF 文件头
            if not content.startswith(b'%PDF'):

                print(f"⚠️ 非PDF文件: {url}")

                return {'status': 2}

        # ====================== 保存文件 ======================
        fname = hashlib.md5(url.encode()).hexdigest() + ".pdf"

        local_path = os.path.join(SAVE_DIR, fname)

        with open(local_path, "wb") as f:
            f.write(content)

        # ====================== FTP上传 ======================
        remote_path = f"/WJL/2026/PDF/{fname}"

        upload_ok = upload_with_retry(
            local_path,
            remote_path
        )

        if not upload_ok:
            return {'status': 2}

        return {
            'status': 1,
            'fname': fname,
            'url': url
        }

    except requests.exceptions.Timeout:

        print(f"⏰ 超时: {url}")

        return {'status': 2}

    except requests.exceptions.ConnectionError:

        print(f"🔌 连接失败: {url}")

        return {'status': 2}

    except Exception as e:

        print(f"❌ 下载异常: {url} -> {e}")

        return {'status': 2}

# ====================== 更新数据库 ======================
def batch_update_db(success_items, conn):

    if not success_items:
        return 0

    try:

        with conn.cursor() as cur:

            cur.executemany("""
                UPDATE `未匹配pdf的数据`
                SET name_pdf = %s,
                    是否下载pdf = '1'
                WHERE digikey_pdf_url = %s
            """, success_items)

        conn.commit()

        return len(success_items)

    except Exception as e:

        print(f"❌ 数据库更新失败: {e}")

        conn.rollback()

        return 0

# ====================== 主程序 ======================
def start_task():

    conn = pymysql.connect(**db_config)

    print("✅ 数据库连接成功")

    try:

        with conn.cursor(pymysql.cursors.DictCursor) as cur:

            cur.execute("""
                SELECT DISTINCT digikey_pdf_url
                FROM `未匹配pdf的数据`
                WHERE 是否下载pdf = '2'
                  AND digikey_品牌 != 'Fischer Elektronik'
                  AND digikey_pdf_url IS NOT NULL
                  AND digikey_pdf_url != ''
            """)

            all_urls = [
                row["digikey_pdf_url"]
                for row in cur.fetchall()
            ]

        total = len(all_urls)

        print(f"📊 待处理：{total} 条")

        success = 0
        fail = 0

        for i in range(0, total, BATCH_SIZE):

            batch = all_urls[i:i + BATCH_SIZE]

            current_batch = i // BATCH_SIZE + 1

            print(
                f"\n🚀 批次 {current_batch}："
                f"{i+1}~{i+len(batch)} / {total}"
            )

            ok_list = []

            with ThreadPoolExecutor(
                max_workers=MAX_WORKERS
            ) as executor:

                future_map = {
                    executor.submit(
                        download_and_upload_worker,
                        url
                    ): url
                    for url in batch
                }

                for future in as_completed(future_map):

                    url = future_map[future]

                    try:

                        item = future.result(timeout=40)

                        if item and item.get("status") == 1:

                            ok_list.append(
                                (
                                    item["fname"],
                                    item["url"]
                                )
                            )

                            success += 1

                        else:

                            fail += 1

                    except Exception as e:

                        print(f"❌ Future异常: {url} -> {e}")

                        fail += 1

            # 更新数据库
            batch_update_db(ok_list, conn)

            print(
                f"✅ 本批成功: {len(ok_list)} | "
                f"累计成功: {success} | "
                f"累计失败: {fail}"
            )

        print("\n🏁 全部完成")
        print(f"✅ 成功: {success}")
        print(f"❌ 失败: {fail}")

    except Exception as e:

        print(f"❌ 主程序异常: {e}")

        conn.rollback()

    finally:

        conn.close()

        print("🔒 数据库连接关闭")

# ====================== 启动 ======================
if __name__ == "__main__":
    start_task()
