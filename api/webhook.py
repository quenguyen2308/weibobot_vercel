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
from telegram.ext import Application
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

async def get_best_url(pid: str) -> tuple[str, int]:
    """Tìm URL có content-length LỚN NHẤT trong các size khả dụng (dùng HEAD,
    không tải nội dung). Logic phải đồng nhất với get_best_url_with_content
    (dùng cho /all) — nếu không, size hiển thị trên nút sẽ sai vì sẽ trả về
    ngay khi gặp variant đầu tiên (orj1080 — bản resize nhỏ) thay vì so sánh
    để tìm bản gốc 'large' (size thật, thường lớn hơn nhiều)."""
    sizes = ["orj1080", "mw2000", "orj480", "large", "orj360"]
    best_url, best_size = "", 0

    async with httpx.AsyncClient(headers=HEADERS_IMG, follow_redirects=True) as client:
        for s in sizes:
            url = f"https://wx2.sinaimg.cn/{s}/{pid}.jpg"
            try:
                resp = await client.head(url, timeout=8)
                if resp.status_code == 200:
                    size = int(resp.headers.get("content-length", 0))
                    if size > best_size:
                        best_size, best_url = size, url
                elif resp.status_code == 403:
                    for sub in ["wx1", "wx3", "wx4"]:
                        alt = f"https://{sub}.sinaimg.cn/{s}/{pid}.jpg"
                        resp2 = await client.head(alt, timeout=8)
                        if resp2.status_code == 200:
                            size = int(resp2.headers.get("content-length", 0))
                            if size > best_size:
                                best_size, best_url = size, alt
                            break
            except:
                pass

    return best_url, best_size

async def get_best_url_with_content(pid: str) -> tuple[str, int, bytes | None]:
    """Dùng cho /all — HEAD để tìm URL lớn nhất, rồi GET 1 lần duy nhất."""
    sizes = ["orj1080", "mw2000", "orj480", "large", "orj360"]
    best_url, best_size = "", 0

    async with httpx.AsyncClient(headers=HEADERS_IMG, follow_redirects=True) as client:
        # Bước 1: HEAD để tìm URL lớn nhất
        for s in sizes:
            url = f"https://wx2.sinaimg.cn/{s}/{pid}.jpg"
            try:
                resp = await client.head(url, timeout=8)
                if resp.status_code == 200:
                    size = int(resp.headers.get("content-length", 0))
                    if size > best_size:
                        best_size, best_url = size, url
                elif resp.status_code == 403:
                    for sub in ["wx1", "wx3", "wx4"]:
                        alt = f"https://{sub}.sinaimg.cn/{s}/{pid}.jpg"
                        resp2 = await client.head(alt, timeout=8)
                        if resp2.status_code == 200:
                            size = int(resp2.headers.get("content-length", 0))
                            if size > best_size:
                                best_size, best_url = size, alt
                            break
            except:
                pass

        if not best_url:
            return "", 0, None

        # Bước 2: GET 1 lần duy nhất URL tốt nhất
        try:
            resp = await client.get(best_url, timeout=60)
            if resp.status_code == 200:
                data = resp.content
                return best_url, len(data), data
        except Exception as e:
            print(f"[get_best_url_with_content] GET failed: {best_url} — {e}")

    return "", 0, None

async def download_image(url: str, timeout: int = 30, retries: int = 3) -> bytes | None:
    for attempt in range(retries):
        async with httpx.AsyncClient(headers=HEADERS_IMG, follow_redirects=True) as client:
            try:
                resp = await client.get(url, timeout=timeout)
                if resp.status_code == 200:
                    return resp.content
                elif resp.status_code == 403:
                    for sub in ["wx1", "wx2", "wx3", "wx4"]:
                        alt_url = re.sub(r"wx\d\.sinaimg\.cn", f"{sub}.sinaimg.cn", url)
                        if alt_url == url:
                            continue
                        resp2 = await client.get(alt_url, timeout=timeout)
                        if resp2.status_code == 200:
                            return resp2.content
            except (httpx.ReadTimeout, httpx.ConnectTimeout):
                print(f"[Timeout] attempt {attempt+1}/{retries}: {url}")
                await asyncio.sleep(2)
            except Exception as e:
                print(f"[Download Error] {url} — {e}")
                break
    return None

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

            pics = post_data.get("pics", [])
            pics_more = post_data.get("pics_more", [])
            all_pics = pics + pics_more

            tasks = []
            for pic in all_pics:
                pid = pic.get("pid", "")
                thumb = pic.get("url", "")
                thumb_urls.append(thumb)
                if pid:
                    tasks.append(get_best_url(pid))
                else:
                    fallback = pic.get("large", {}).get("url") or pic.get("url", "")
                    async def _fallback(u=fallback):
                        return (u, 0)
                    tasks.append(_fallback())

            results = await asyncio.gather(*tasks)
            raw_urls = [u for u, s in results]
            raw_sizes = [s for u, s in results]

        except Exception as e:
            print(f"[Scraper Error] {e}")
            import traceback
            traceback.print_exc()
            return [], [], []

    return thumb_urls, raw_urls, raw_sizes

async def get_raw_images_for_download(post_id: str) -> tuple[list[str], list[int], list[bytes | None]]:
    """Dùng cho /all — GET ảnh full size luôn, cache content, không download lại lần 2."""
    api_url = f"https://m.weibo.cn/statuses/show?id={post_id}"

    async with httpx.AsyncClient(headers=HEADERS_API, follow_redirects=True) as client:
        try:
            resp = await client.get(api_url, timeout=15)
            data = resp.json()
            post_data = data.get("data", {})

            pics = post_data.get("pics", [])
            pics_more = post_data.get("pics_more", [])
            all_pics = pics + pics_more

            tasks = []
            for pic in all_pics:
                pid = pic.get("pid", "")
                if pid:
                    tasks.append(get_best_url_with_content(pid))
                else:
                    fallback = pic.get("large", {}).get("url") or pic.get("url", "")
                    async def _fallback(u=fallback):
                        b = await download_image(u)
                        return (u, len(b) if b else 0, b)
                    tasks.append(_fallback())

            results = await asyncio.gather(*tasks)
            raw_urls     = [u for u, s, b in results]
            raw_sizes    = [s for u, s, b in results]
            raw_contents = [b for u, s, b in results]

        except Exception as e:
            print(f"[Scraper Error] {e}")
            import traceback
            traceback.print_exc()
            return [], [], []

    return raw_urls, raw_sizes, raw_contents

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

    thumb_bytes = await asyncio.gather(*[download_image(u) for u in thumb_urls])

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

async def download_and_send_all(bot: Bot, chat_id: int, raw_urls: list, raw_sizes: list, raw_contents: list | None = None, concurrency: int = 3):
    """Đảm bảo có nội dung từng ảnh song song (dùng cache nếu có, download nếu
    chưa), nhưng GỬI lên Telegram tuần tự theo đúng thứ tự #1, #2, #3...
    Lý do tách riêng: nếu để send_as_file chạy song song (như bản trước), ảnh
    nhẹ upload xong trước, ảnh nặng xong sau — thứ tự hiện trong chat bị lộn,
    dù nội dung đã tải/cache xong cùng lúc. Gửi tuần tự ở đây gần như không
    tốn thêm thời gian vì content thường đã sẵn sàng trước khi đến lượt."""
    semaphore = asyncio.Semaphore(concurrency)
    content_ready: list[bytes | None] = list(raw_contents) if raw_contents else [None] * len(raw_urls)

    async def _ensure_one(i: int, img_url: str):
        if content_ready[i] is None:
            async with semaphore:
                content_ready[i] = await download_image(img_url)

    # Đảm bảo nội dung cho tất cả ảnh ngay (chạy nền song song nếu cần download thêm)
    ensure_tasks = [asyncio.create_task(_ensure_one(i, url)) for i, url in enumerate(raw_urls)]

    success_count = 0
    for i, (img_url, size) in enumerate(zip(raw_urls, raw_sizes)):
        await ensure_tasks[i]  # thường đã xong sẵn, ít khi phải chờ thêm
        b = content_ready[i]
        if b:
            try:
                await send_as_file(
                    bot, chat_id, b,
                    filename=get_filename_from_url(img_url),
                    caption=f"#{i+1} — {format_size(size)}"
                )
                success_count += 1
            except Exception as e:
                await bot.send_message(chat_id=chat_id, text=f"⚠️ Không upload được ảnh #{i+1}: {e}")
        else:
            await bot.send_message(chat_id=chat_id, text=f"⚠️ Không tải được ảnh #{i+1}: {img_url}")

    await bot.send_message(chat_id=chat_id, text=f"✅ Hoàn tất: {success_count}/{len(raw_urls)} ảnh")

async def run_download_all_flow(bot: Bot, chat_id: int, post_id: str):
    """Dùng chung cho cả /all và nút Download All. Bọc timeout NỘI BỘ
    (HEAVY_TASK_TIMEOUT_SEC) quanh 2 bước nặng nhất — nếu sắp chạm giới hạn
    maxDuration của Vercel, bot sẽ báo lỗi rõ ràng thay vì bị kill im lặng."""
    try:
        raw_urls, raw_sizes, raw_contents = await asyncio.wait_for(
            get_raw_images_for_download(post_id), timeout=HEAVY_TASK_TIMEOUT_SEC
        )
    except asyncio.TimeoutError:
        await bot.send_message(
            chat_id=chat_id,
            text="⏱ Quá thời gian khi tải ảnh gốc từ Weibo (hàm serverless bị giới hạn thời gian chạy). Thử lại sau hoặc tải từng ảnh bằng nút riêng."
        )
        return

    if not raw_urls:
        await bot.send_message(chat_id=chat_id, text="❌ Không tìm thấy ảnh nào.")
        return

    await bot.send_message(chat_id=chat_id, text=f"📥 Đang tải {len(raw_urls)} ảnh...")

    try:
        await asyncio.wait_for(
            download_and_send_all(bot, chat_id, raw_urls, raw_sizes, raw_contents),
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
    app = Application.builder().token(BOT_TOKEN).request(request).build()
    bot = app.bot

    update = Update.de_json(update_data, bot)

    # /start
    if update.message and update.message.text == "/start":
        await bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                "🖼 Weibo Image Bot\n\n"
                "Paste link bài post Weibo → bot hiện preview album\n"
                "→ Bấm Download All hoặc chọn từng ảnh\n\n"
                "/a <url> — Download All Files"
            )
        )
        return

    # /a <url>
    if update.message and update.message.text and update.message.text.startswith("/a"):
        parts = update.message.text.split(maxsplit=1)
        if len(parts) < 2:
            await bot.send_message(chat_id=update.effective_chat.id, text="❌ Dùng: /a <weibo_url>")
            return
        url = parts[1].strip()
        post_id = extract_weibo_id(url)
        if not post_id:
            await bot.send_message(chat_id=update.effective_chat.id, text="❌ Không nhận ra link Weibo.")
            return
        msg = await bot.send_message(chat_id=update.effective_chat.id, text="⬇️ Đang xử lý...")
        await run_download_all_flow(bot, update.effective_chat.id, post_id)
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

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/api/webhook":
            self.send_response(404)
            self.end_headers()
            return

        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)

        try:
            update_data = json.loads(body)
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
