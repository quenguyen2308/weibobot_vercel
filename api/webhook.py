# api/webhook.py — Vercel serverless entry point
# Telegram gửi POST đến /api/webhook, Vercel gọi handler()

import os
import re
import io
import json
import httpx
import asyncio
import hashlib
from telegram import (
    Update, InputMediaPhoto, InlineKeyboardButton, InlineKeyboardMarkup, Bot
)
from telegram.request import HTTPXRequest
from http.server import BaseHTTPRequestHandler

BOT_TOKEN = os.environ.get("BOT_TOKEN")

# ─── TIMEOUT AN TOÀN CHO TÁC VỤ NẶNG ───────────────────────────────────────────
# Vercel sẽ KILL THẲNG tiến trình (không phải exception Python) nếu function
# chạy quá `maxDuration` cấu hình trong vercel.json — lúc đó bot không kịp gửi
# tin báo lỗi, chat sẽ treo im lặng mãi ở message cuối (đúng là bug đã gặp).
#
# Giải pháp: tự đặt timeout NỘI BỘ nhỏ hơn maxDuration vài chục giây, để nếu
# sắp chạm giới hạn, bot kịp gửi tin báo lỗi cho user trước khi Vercel kill.
#
# Đặt giá trị này khớp với maxDuration trong vercel.json (xem ghi chú dưới đó):
#   - Hobby (không Fluid Compute): maxDuration=60   → HEAVY_TASK_TIMEOUT_SEC=40
#   - Pro / Enterprise:             maxDuration=300  → HEAVY_TASK_TIMEOUT_SEC=270
#   - Pro / Enterprise (Fluid, GA): maxDuration=800  → HEAVY_TASK_TIMEOUT_SEC=760
HEAVY_TASK_TIMEOUT_SEC = int(os.environ.get("HEAVY_TASK_TIMEOUT_SEC", "270"))

HEADERS_API = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15",
    "Referer": "https://m.weibo.cn/",
    "Accept": "application/json, text/plain, */*",
    "MWeibo-Pwa": "1",
    "X-Requested-With": "XMLHttpRequest",
}

HEADERS_IMG = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://weibo.com/",
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cache-Control": "no-cache",
    "Cookie": "",
}

# ─── CALLBACK DATA ENCODING ────────────────────────────────────────────────────
# Vercel serverless không có shared memory giữa các invocation.
# Giải pháp: encode URL trực tiếp vào callback_data (max 64 bytes của Telegram).
# Vì URL sinaimg dài hơn 64 bytes, ta dùng prefix ngắn + index, và lưu URL
# vào Telegram message text (ẩn trong caption) để retrieve lại.
#
# Cách đơn giản hơn: khi user bấm Download All / Download One,
# bot sẽ scrape lại URL từ post_id (lưu trong callback_data).
# Callback_data format:
#   "dl_all:<post_id>"
#   "dl_one:<post_id>:<index>"

def make_cb_all(post_id: str) -> str:
    return f"dl_all:{post_id}"

def make_cb_one(post_id: str, idx: int) -> str:
    return f"dl_one:{post_id}:{idx}"

# ─── SCRAPER ──────────────────────────────────────────────────────────────────

def extract_weibo_id(url: str) -> str | None:
    patterns = [
        r"weibo\.com/\d+/(\w+)",
        r"weibo\.com/detail/(\w+)",
        r"m\.weibo\.cn/detail/(\w+)",
        r"m\.weibo\.cn/\d+/(\w+)",
        r"m\.weibo\.cn/status/(\w+)",
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None

IMAGE_SIZES = ["orj1080", "mw2000", "orj480", "large", "orj360"]
# 'large' gần như luôn là bản gốc full-size (lớn nhất) — chỉ dò 1 size này cho
# mỗi ảnh; KHÔNG THẤY (thiếu / lỗi mạng) mới dò tiếp lần lượt các size còn lại,
# dừng ngay khi gặp size đầu tiên tải được. Kết hợp với chunking theo
# CHUNK_SIZE ảnh (xem _gather_in_chunks) để tránh dí quá nhiều request đồng
# thời vào CDN Weibo khi album nhiều ảnh — đây là nguồn chính khiến việc tải
# album nhiều ảnh chậm hẳn/403 hàng loạt và vượt timeout webhook Telegram,
# khiến Telegram tự retry cả update → xử lý/upload trùng lặp.
PRIMARY_SIZE = "large"
FALLBACK_SIZES = [s for s in IMAGE_SIZES if s != PRIMARY_SIZE]

# Số ảnh xử lý song song tối đa cùng lúc (probe + download) — album nhiều ảnh
# sẽ được chia thành từng đợt CHUNK_SIZE ảnh, đợt sau chỉ chạy khi đợt trước
# xong, thay vì bắn hết N ảnh song song một lúc. Trước đây để 3 vì còn dò cả 5
# size x N ảnh cùng lúc (dễ 403 hàng loạt); giờ mỗi ảnh chỉ dò 1 HEAD ('large')
# ở best-case nên nới lên 8 vẫn an toàn, giảm số đợt tuần tự.
CHUNK_SIZE = 8

async def _gather_in_chunks(coros: list, chunk_size: int = CHUNK_SIZE) -> list:
    """Chạy danh sách coroutine theo từng đợt tối đa `chunk_size` cái song
    song một lúc, đợi xong đợt này mới chạy đợt tiếp theo."""
    results = []
    for i in range(0, len(coros), chunk_size):
        batch = coros[i:i + chunk_size]
        results.extend(await asyncio.gather(*batch))
    return results

async def _probe_size(client: httpx.AsyncClient, pid: str, s: str) -> tuple[str, int]:
    """HEAD 1 size cụ thể, tự fallback sang wx1/wx3/wx4 nếu wx2 trả 403.
    Chạy độc lập với các size khác nên có thể gọi song song qua asyncio.gather."""
    url = f"https://wx2.sinaimg.cn/{s}/{pid}.jpg"
    try:
        resp = await client.head(url, timeout=8)
        if resp.status_code == 200:
            return url, int(resp.headers.get("content-length", 0))
        elif resp.status_code == 403:
            for sub in ["wx1", "wx3", "wx4"]:
                alt = f"https://{sub}.sinaimg.cn/{s}/{pid}.jpg"
                try:
                    resp2 = await client.head(alt, timeout=8)
                    if resp2.status_code == 200:
                        return alt, int(resp2.headers.get("content-length", 0))
                except Exception:
                    pass
    except Exception:
        pass
    return "", 0

async def _find_best_size(client: httpx.AsyncClient, pid: str) -> tuple[str, int]:
    """Thử 'large' trước — chỉ 1 HEAD. Không thấy mới dò tiếp lần lượt các size
    còn lại, dừng ngay khi gặp size đầu tiên tải được (không cần dò hết để so
    sánh, vì 'large' vốn đã là ưu tiên số 1)."""
    url, size = await _probe_size(client, pid, PRIMARY_SIZE)
    if url:
        return url, size

    for s in FALLBACK_SIZES:
        url, size = await _probe_size(client, pid, s)
        if url:
            return url, size

    return "", 0

async def get_best_url(pid: str, client: httpx.AsyncClient | None = None) -> tuple[str, int]:
    """Tìm URL ảnh gốc tốt nhất (dùng HEAD, không tải nội dung). Logic phải
    đồng nhất với get_best_url_with_content (dùng cho /all) để size hiển thị
    trên nút không bị lệch với size thật sự tải.

    Nhận `client` dùng chung từ ngoài (khi dò nhiều ảnh liên tiếp) để tái sử
    dụng kết nối keep-alive, tránh bắt tay TLS lại cho mỗi ảnh; không truyền
    vào thì tự tạo client riêng."""
    if client is not None:
        return await _find_best_size(client, pid)
    async with httpx.AsyncClient(headers=HEADERS_IMG, follow_redirects=True) as own_client:
        return await _find_best_size(own_client, pid)

async def get_best_url_with_content(pid: str, client: httpx.AsyncClient | None = None) -> tuple[str, int, bytes | None]:
    """Dùng cho /all — HEAD để tìm URL tốt nhất, rồi GET 1 lần duy nhất.

    Nhận `client` dùng chung từ ngoài (xem get_best_url) để tránh mỗi ảnh tự
    bắt tay TLS riêng khi dò+tải cả loạt ảnh."""
    async def _run(c: httpx.AsyncClient) -> tuple[str, int, bytes | None]:
        best_url, best_size = await _find_best_size(c, pid)
        if not best_url:
            return "", 0, None
        # Bước 2: GET 1 lần duy nhất URL tốt nhất
        try:
            resp = await c.get(best_url, timeout=60)
            if resp.status_code == 200:
                data = resp.content
                return best_url, len(data), data
        except Exception as e:
            print(f"[get_best_url_with_content] GET failed: {best_url} — {e}")
        return "", 0, None

    if client is not None:
        return await _run(client)
    async with httpx.AsyncClient(headers=HEADERS_IMG, follow_redirects=True) as own_client:
        return await _run(own_client)

async def download_image(
    url: str,
    timeout: int = 30,
    retries: int = 3,
    client: httpx.AsyncClient | None = None,
) -> bytes | None:
    """1 client dùng chung cho cả các lần retry — tránh bắt tay TLS lại từ đầu
    mỗi lần thử, tận dụng connection keep-alive của httpx.

    Nhận `client` dùng chung từ ngoài (khi tải nhiều ảnh cùng lúc) để tái sử
    dụng connection pool thay vì mỗi ảnh tự bắt tay TLS riêng; không truyền
    vào thì tự tạo client riêng."""
    async def _run(c: httpx.AsyncClient) -> bytes | None:
        for attempt in range(retries):
            try:
                resp = await c.get(url, timeout=timeout)
                if resp.status_code == 200:
                    return resp.content
                elif resp.status_code == 403:
                    for sub in ["wx1", "wx2", "wx3", "wx4"]:
                        alt_url = re.sub(r"wx\d\.sinaimg\.cn", f"{sub}.sinaimg.cn", url)
                        if alt_url == url:
                            continue
                        resp2 = await c.get(alt_url, timeout=timeout)
                        if resp2.status_code == 200:
                            return resp2.content
            except (httpx.ReadTimeout, httpx.ConnectTimeout):
                print(f"[Timeout] attempt {attempt+1}/{retries}: {url}")
                await asyncio.sleep(2)
            except Exception as e:
                print(f"[Download Error] {url} — {e}")
                break
        return None

    if client is not None:
        return await _run(client)
    async with httpx.AsyncClient(headers=HEADERS_IMG, follow_redirects=True) as own_client:
        return await _run(own_client)

def _build_full_pic_list(post_data: dict) -> list[dict]:
    """API mobile Weibo (`statuses/show`) đôi khi cắt field `pics` xuống tối đa
    9 phần tử — đặc biệt với bài trộn ảnh+video (2 slot bị chiếm bởi cover
    video), và `pics_more` cũng không được điền bù trong trường hợp đó, khiến
    ảnh thật bị thiếu dù `pic_num`/`pic_ids` báo tổng số ảnh lớn hơn hẳn.

    `pic_ids` mới là danh sách pid ẢNH THẬT đầy đủ, đáng tin cậy — dùng nó làm
    nguồn chân lý. Với mỗi pid, mượn dữ liệu chi tiết (thumbnail, geo...) từ
    `pics`/`pics_more` nếu có; pid nào bị thiếu do `pics` bị cắt thì tự dựng
    thumbnail URL theo pattern chuẩn của sinaimg.cn (đã kiểm chứng qua các pid
    có sẵn: https://wx<N>.sinaimg.cn/orj360/<pid>.jpg, subdomain N sai lệch
    thì download_image/get_best_url đã tự fallback wx1/wx2/wx3/wx4)."""
    pics = post_data.get("pics", [])
    pics_more = post_data.get("pics_more", [])
    by_pid = {p.get("pid"): p for p in pics + pics_more if p.get("pid")}

    pic_ids = post_data.get("pic_ids") or list(by_pid.keys())
    return [
        by_pid.get(pid) or {"pid": pid, "url": f"https://wx2.sinaimg.cn/orj360/{pid}.jpg"}
        for pid in pic_ids
    ]

async def get_raw_images(post_id: str) -> tuple[list[str], list[str], list[int]]:
    """Dùng cho preview — chỉ cần URL + size."""
    thumb_urls = []
    raw_urls = []
    api_url = f"https://m.weibo.cn/statuses/show?id={post_id}"

    async with httpx.AsyncClient(headers=HEADERS_API, follow_redirects=True) as client:
        try:
            resp = await client.get(api_url, timeout=15)
            data = resp.json()
            post_data = data.get("data", {})

            all_pics = _build_full_pic_list(post_data)

            # 1 client dùng chung cho toàn bộ N ảnh khi dò size — tránh bắt
            # tay TLS lại từ đầu cho mỗi ảnh (trước đây get_best_url tự mở
            # client riêng mỗi ảnh).
            async with httpx.AsyncClient(headers=HEADERS_IMG, follow_redirects=True) as img_client:
                tasks = []
                for pic in all_pics:
                    pid = pic.get("pid", "")
                    thumb = pic.get("url", "")
                    thumb_urls.append(thumb)
                    if pid:
                        tasks.append(get_best_url(pid, client=img_client))
                    else:
                        fallback = pic.get("large", {}).get("url") or pic.get("url", "")
                        async def _fallback(u=fallback):
                            return (u, 0)
                        tasks.append(_fallback())

                results = await _gather_in_chunks(tasks)
            raw_urls = [u for u, s in results]
            raw_sizes = [s for u, s in results]

        except Exception as e:
            print(f"[Scraper Error] {e}")
            import traceback
            traceback.print_exc()
            return [], [], []

    return thumb_urls, raw_urls, raw_sizes

async def fetch_pics_meta(post_id: str) -> list[dict]:
    """Chỉ gọi API Weibo để lấy danh sách metadata ẢNH ĐẦY ĐỦ (dựa trên
    pic_ids, xem _build_full_pic_list), KHÔNG tải nội dung ảnh nào — dùng để
    biết tổng số ảnh và validate số thứ tự user chọn trước khi tải nặng."""
    api_url = f"https://m.weibo.cn/statuses/show?id={post_id}"

    async with httpx.AsyncClient(headers=HEADERS_API, follow_redirects=True) as client:
        try:
            resp = await client.get(api_url, timeout=15)
            data = resp.json()
            post_data = data.get("data", {})
            return _build_full_pic_list(post_data)
        except Exception as e:
            print(f"[Scraper Error] {e}")
            import traceback
            traceback.print_exc()
            return []

async def fetch_content_for_pics(
    selected_pics: list[tuple[int, dict]]
) -> tuple[list[int], list[str], list[int], list[bytes | None]]:
    """Dùng cho /a — GET ảnh full size cho đúng các pic đã chọn, cache content,
    không download lại lần 2. `selected_pics` là list (index_gốc, pic_dict).
    Trả về orig_indices song song với url/size/content để giữ đúng số thứ tự
    hiển thị (#N) dù chỉ tải một phần album."""
    # 1 client dùng chung cho toàn bộ N ảnh (pool connection tối đa =
    # CHUNK_SIZE) thay vì mỗi ảnh tự bắt tay TLS riêng cho cả bước dò size lẫn
    # bước GET nội dung thật.
    async with httpx.AsyncClient(
        headers=HEADERS_IMG,
        follow_redirects=True,
        limits=httpx.Limits(max_connections=CHUNK_SIZE, max_keepalive_connections=CHUNK_SIZE),
    ) as client:
        tasks = []
        for _, pic in selected_pics:
            pid = pic.get("pid", "")
            if pid:
                tasks.append(get_best_url_with_content(pid, client=client))
            else:
                fallback = pic.get("large", {}).get("url") or pic.get("url", "")
                async def _fallback(u=fallback, c=client):
                    b = await download_image(u, client=c)
                    return (u, len(b) if b else 0, b)
                tasks.append(_fallback())

        results = await _gather_in_chunks(tasks)
    orig_indices = [i for i, _ in selected_pics]
    raw_urls     = [u for u, s, b in results]
    raw_sizes    = [s for u, s, b in results]
    raw_contents = [b for u, s, b in results]
    return orig_indices, raw_urls, raw_sizes, raw_contents

def get_filename_from_url(url: str) -> str:
    match = re.search(r"/([^/]+\.(?:jpg|jpeg|png|gif|webp))$", url, re.IGNORECASE)
    if match:
        return match.group(1)
    return "weibo_image.jpg"

def format_size(size_bytes: int) -> str:
    if size_bytes == 0:
        return "?"
    if size_bytes >= 1024 * 1024:
        return f"{size_bytes / 1024 / 1024:.1f}MB"
    return f"{size_bytes / 1024:.0f}KB"

def parse_indices(spec: str, max_n: int) -> list[int]:
    """Parse chuỗi kiểu '1,3,5-7' (1-based, 2 đầu đều đóng) thành list index
    0-based đã sort + dedup. Index ngoài phạm vi [1, max_n] bị bỏ qua âm thầm.
    Raise ValueError nếu sai định dạng (không phải số / không phải khoảng N-M)."""
    spec = spec.strip()
    if not spec:
        return []

    result = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            bounds = part.split("-")
            if len(bounds) != 2 or not all(b.strip().isdigit() for b in bounds):
                raise ValueError(f"Sai định dạng khoảng: '{part}' (vd đúng: 5-7)")
            a, b = int(bounds[0]), int(bounds[1])
            if a > b:
                a, b = b, a
            for n in range(a, b + 1):
                if 1 <= n <= max_n:
                    result.add(n - 1)
        else:
            if not part.isdigit():
                raise ValueError(f"Sai định dạng số: '{part}'")
            n = int(part)
            if 1 <= n <= max_n:
                result.add(n - 1)

    return sorted(result)

# ─── BOT LOGIC ────────────────────────────────────────────────────────────────

async def send_as_file(bot: Bot, chat_id: int, data: bytes, filename: str, caption: str = ""):
    for attempt in range(1, 4):
        try:
            await bot.send_document(
                chat_id=chat_id,
                document=io.BytesIO(data),
                filename=filename,
                caption=caption,
                parse_mode="Markdown",
                write_timeout=180,
                read_timeout=60,
                connect_timeout=30,
            )
            return
        except Exception as e:
            print(f"[Upload Error] attempt {attempt}/3 — {filename} — {e}")
            if attempt < 3:
                await asyncio.sleep(3 * attempt)
            else:
                raise

async def show_preview(bot: Bot, chat_id: int, reply_to_message_id: int, url: str):
    msg = await bot.send_message(chat_id=chat_id, text="🔍 Đang scrape...")

    post_id = extract_weibo_id(url)
    if not post_id:
        await bot.edit_message_text(chat_id=chat_id, message_id=msg.message_id, text="❌ Không nhận ra link Weibo.")
        return

    thumb_urls, raw_urls, raw_sizes = await get_raw_images(post_id)
    if not raw_urls:
        await bot.edit_message_text(chat_id=chat_id, message_id=msg.message_id, text="❌ Không tìm thấy ảnh nào.")
        return

    await bot.edit_message_text(chat_id=chat_id, message_id=msg.message_id, text=f"📥 Đang tải preview {len(raw_urls)} ảnh...")

    # 1 client dùng chung cho toàn bộ N thumbnail thay vì mỗi ảnh tự bắt tay
    # TLS riêng.
    async with httpx.AsyncClient(headers=HEADERS_IMG, follow_redirects=True) as thumb_client:
        thumb_bytes = await asyncio.gather(
            *[download_image(u, client=thumb_client) for u in thumb_urls]
        )

    await bot.edit_message_text(chat_id=chat_id, message_id=msg.message_id, text=f"🖼 {len(raw_urls)} ảnh — bấm nút bên dưới mỗi ảnh để tải:")

    for i, (thumb, size) in enumerate(zip(thumb_bytes, raw_sizes)):
        if not thumb:
            continue
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                f"⬇️ #{i+1} — {format_size(size)}",
                callback_data=make_cb_one(post_id, i)
            )
        ]])
        await bot.send_photo(
            chat_id=chat_id,
            photo=io.BytesIO(thumb),
            reply_markup=keyboard
        )
        await asyncio.sleep(0.3)

    keyboard_all = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            f"⬇️ Download All ({len(raw_urls)} ảnh — {format_size(sum(raw_sizes))})",
            callback_data=make_cb_all(post_id)
        )
    ]])
    await bot.send_message(
        chat_id=chat_id,
        text="👇 Hoặc tải tất cả:",
        reply_markup=keyboard_all
    )

async def download_and_send_all(
    bot: Bot,
    chat_id: int,
    orig_indices: list,
    raw_urls: list,
    raw_sizes: list,
    raw_contents: list | None = None,
    concurrency: int = 3,
):
    """Đảm bảo có nội dung từng ảnh song song (dùng cache nếu có, download nếu
    chưa), nhưng GỬI lên Telegram tuần tự theo đúng thứ tự #1, #2, #3...
    Lý do tách riêng: nếu để send_as_file chạy song song (như bản trước), ảnh
    nhẹ upload xong trước, ảnh nặng xong sau — thứ tự hiện trong chat bị lộn,
    dù nội dung đã tải/cache xong cùng lúc. Gửi tuần tự ở đây gần như không
    tốn thêm thời gian vì content thường đã sẵn sàng trước khi đến lượt.

    `orig_indices[k]` là số thứ tự gốc (0-based) của raw_urls[k]/raw_sizes[k] —
    dùng để caption đúng #N kể cả khi chỉ tải một phần album đã chọn."""
    semaphore = asyncio.Semaphore(concurrency)
    content_ready: list[bytes | None] = list(raw_contents) if raw_contents else [None] * len(raw_urls)

    async def _ensure_one(i: int, img_url: str):
        if content_ready[i] is None:
            async with semaphore:
                content_ready[i] = await download_image(img_url)

    # Đảm bảo nội dung cho tất cả ảnh ngay (chạy nền song song nếu cần download thêm)
    ensure_tasks = [asyncio.create_task(_ensure_one(i, url)) for i, url in enumerate(raw_urls)]

    success_count = 0
    for k, (img_url, size) in enumerate(zip(raw_urls, raw_sizes)):
        orig_i = orig_indices[k]
        await ensure_tasks[k]  # thường đã xong sẵn, ít khi phải chờ thêm
        b = content_ready[k]
        if b:
            try:
                await send_as_file(
                    bot, chat_id, b,
                    filename=get_filename_from_url(img_url),
                    caption=f"#{orig_i+1} — {format_size(size)}"
                )
                success_count += 1
            except Exception as e:
                await bot.send_message(chat_id=chat_id, text=f"⚠️ Không upload được ảnh #{orig_i+1}: {e}")
        else:
            await bot.send_message(chat_id=chat_id, text=f"⚠️ Không tải được ảnh #{orig_i+1}: {img_url}")

    await bot.send_message(chat_id=chat_id, text=f"✅ Hoàn tất: {success_count}/{len(raw_urls)} ảnh")

async def run_download_all_flow(bot: Bot, chat_id: int, post_id: str, index_spec: str = ""):
    """Dùng chung cho cả /a và nút Download All. Bọc timeout NỘI BỘ
    (HEAVY_TASK_TIMEOUT_SEC) quanh các bước nặng nhất — nếu sắp chạm giới hạn
    maxDuration của Vercel, bot sẽ báo lỗi rõ ràng thay vì bị kill im lặng.

    `index_spec`: chuỗi số thứ tự user chọn (vd '1,3,5-7'), rỗng = tải hết."""
    try:
        all_pics = await asyncio.wait_for(fetch_pics_meta(post_id), timeout=HEAVY_TASK_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        await bot.send_message(
            chat_id=chat_id,
            text="⏱ Quá thời gian khi lấy danh sách ảnh từ Weibo. Thử lại sau."
        )
        return

    total = len(all_pics)
    if total == 0:
        await bot.send_message(chat_id=chat_id, text="❌ Không tìm thấy ảnh nào.")
        return

    indices = None
    if index_spec:
        try:
            indices = parse_indices(index_spec, total)
        except ValueError as e:
            await bot.send_message(chat_id=chat_id, text=f"❌ {e}")
            return
        if not indices:
            await bot.send_message(chat_id=chat_id, text="❌ Không có ảnh nào khớp với số thứ tự đã chọn.")
            return

    selected_pics = [(i, all_pics[i]) for i in indices] if indices is not None else list(enumerate(all_pics))

    await bot.send_message(chat_id=chat_id, text=f"📥 Đang tải {len(selected_pics)}/{total} ảnh...")

    try:
        orig_indices, raw_urls, raw_sizes, raw_contents = await asyncio.wait_for(
            fetch_content_for_pics(selected_pics), timeout=HEAVY_TASK_TIMEOUT_SEC
        )
    except asyncio.TimeoutError:
        await bot.send_message(
            chat_id=chat_id,
            text="⏱ Quá thời gian khi tải ảnh gốc từ Weibo (hàm serverless bị giới hạn thời gian chạy). Thử lại sau hoặc tải từng ảnh bằng nút riêng."
        )
        return

    try:
        await asyncio.wait_for(
            download_and_send_all(bot, chat_id, orig_indices, raw_urls, raw_sizes, raw_contents),
            timeout=HEAVY_TASK_TIMEOUT_SEC
        )
    except asyncio.TimeoutError:
        await bot.send_message(
            chat_id=chat_id,
            text="⏱ Quá thời gian upload (hàm serverless bị Vercel giới hạn maxDuration). Album quá nhiều/nặng ảnh — hãy thử tải từng ảnh bằng nút riêng, hoặc tăng maxDuration nếu plan cho phép."
        )

async def process_update(update_data: dict):
    request = HTTPXRequest(
        connection_pool_size=8,
        read_timeout=60,
        write_timeout=180,
        connect_timeout=30,
        pool_timeout=30,
    )
    bot = Bot(token=BOT_TOKEN, request=request)

    update = Update.de_json(update_data, bot)

    # /start
    if update.message and update.message.text == "/start":
        await bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                "🖼 Weibo Image Bot\n\n"
                "Paste link bài post Weibo → bot hiện preview album\n"
                "→ Bấm Download All hoặc chọn từng ảnh\n\n"
                "/a <url> — Download tất cả ảnh\n"
                "/a <url> 1,3,5-7 — Chỉ download các ảnh số 1, 3, 5, 6, 7"
            )
        )
        return

    # /a <url> [số thứ tự ảnh]
    if update.message and update.message.text and update.message.text.startswith("/a"):
        parts = update.message.text.split(maxsplit=2)
        if len(parts) < 2:
            await bot.send_message(
                chat_id=update.effective_chat.id,
                text="❌ Dùng: /a <weibo_url> [số thứ tự ảnh]\nVD: /a https://weibo.com/... 1,3,5-7"
            )
            return
        url = parts[1].strip()
        index_spec = parts[2].strip() if len(parts) > 2 else ""
        post_id = extract_weibo_id(url)
        if not post_id:
            await bot.send_message(chat_id=update.effective_chat.id, text="❌ Không nhận ra link Weibo.")
            return
        await bot.send_message(chat_id=update.effective_chat.id, text="⬇️ Đang xử lý...")
        await run_download_all_flow(bot, update.effective_chat.id, post_id, index_spec=index_spec)
        return

    # URL message
    if update.message and update.message.text:
        text = update.message.text.strip()
        if "weibo.com" in text or "weibo.cn" in text:
            await show_preview(bot, update.effective_chat.id, update.message.message_id, text)
        return

    # Callback query
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        chat_id = update.effective_chat.id
        cb = query.data

        if cb.startswith("dl_all:"):
            post_id = cb[len("dl_all:"):]
            await run_download_all_flow(bot, chat_id, post_id)

        elif cb.startswith("dl_one:"):
            parts = cb.split(":")
            # format: dl_one:<post_id>:<index>
            post_id = parts[1]
            idx = int(parts[2])
            await bot.send_message(chat_id=chat_id, text=f"📥 Đang tải ảnh #{idx+1}...")
            _, raw_urls, raw_sizes = await get_raw_images(post_id)
            if idx >= len(raw_urls):
                await bot.send_message(chat_id=chat_id, text="❌ Index không hợp lệ.")
                return
            b = await download_image(raw_urls[idx])
            if b:
                await send_as_file(
                    bot, chat_id, b,
                    filename=get_filename_from_url(raw_urls[idx]),
                    caption=f"#{idx+1} — {format_size(raw_sizes[idx])}"
                )
            else:
                await bot.send_message(chat_id=chat_id, text=f"❌ Không tải được ảnh #{idx+1}")

# ─── VERCEL HANDLER ───────────────────────────────────────────────────────────
# /api/webhook (Telegram gọi) chỉ ACK NGAY rồi bắn update sang /api/process —
# một invocation Vercel riêng, có trọn HEAVY_TASK_TIMEOUT_SEC để xử lý nặng.
# Lý do: nếu xử lý nặng ngay trong /api/webhook, khi vượt quá thời gian
# Telegram chịu chờ webhook phản hồi, Telegram sẽ tự động gửi lại NGUYÊN update
# đó → chạy lại process_update() lần 2 → ảnh/tin nhắn bị gửi trùng lặp (xem
# ghi chú ở IMAGE_SIZES). Tách endpoint giúp /api/webhook luôn phản hồi trong
# vài trăm ms bất kể tác vụ nặng mất bao lâu.
INTERNAL_DISPATCH_SECRET = hashlib.sha256(f"dispatch:{BOT_TOKEN}".encode()).hexdigest()

async def dispatch_to_worker(update_data: dict):
    base_url = os.environ.get("VERCEL_URL")
    if not base_url:
        # Không có base URL public (vd chạy local) → xử lý luôn tại chỗ.
        await process_update(update_data)
        return
    if not base_url.startswith("http"):
        base_url = f"https://{base_url}"
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            await client.post(
                f"{base_url}/api/process",
                json=update_data,
                headers={"X-Internal-Secret": INTERNAL_DISPATCH_SECRET},
            )
    except (httpx.ReadTimeout, httpx.ConnectTimeout):
        # Request đã được Vercel nhận và bắt đầu invocation /api/process rồi
        # — không cần chờ nó xử lý/upload ảnh xong mới trả lời Telegram.
        pass


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path not in ("/api/webhook", "/api/process"):
            self.send_response(404)
            self.end_headers()
            return

        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)

        try:
            update_data = json.loads(body)
            if self.path == "/api/webhook":
                asyncio.run(dispatch_to_worker(update_data))
            else:
                if self.headers.get("X-Internal-Secret") != INTERNAL_DISPATCH_SECRET:
                    self.send_response(403)
                    self.end_headers()
                    return
                asyncio.run(process_update(update_data))
        except Exception as e:
            print(f"[Handler Error] {e}")
            import traceback
            traceback.print_exc()

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Weibo Bot is running on Vercel.")

    def log_message(self, format, *args):
        pass  # tắt access log mặc định
