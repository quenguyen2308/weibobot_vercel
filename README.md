# Weibo Image Bot (Vercel)

Telegram bot cho phép người dùng dán link bài post Weibo, bot scrape ảnh gốc (full-size) và cho tải về qua Telegram — chạy hoàn toàn dưới dạng **serverless function trên Vercel**, không cần server chạy liên tục.

## Kiến trúc

- **Runtime**: Vercel Serverless Function (Python), entry point duy nhất là `api/webhook.py`.
- **Giao tiếp**: Telegram gửi update (message/callback query) bằng **webhook** — không dùng polling — tới endpoint `POST /api/webhook`.
- **Không có state/shared memory** giữa các lần gọi function (đặc trưng serverless): mọi thông tin cần thiết cho bước tiếp theo (post_id, index ảnh...) được nhúng vào `callback_data` của nút Telegram, không lưu ở biến toàn cục hay DB.
- **2 endpoint tách biệt** để tránh Telegram tự retry update khi xử lý lâu:
  - `/api/webhook`: Telegram gọi vào đây, chỉ dispatch update sang `/api/process` rồi ACK ngay (vài trăm ms), không chờ xử lý nặng.
  - `/api/process`: chạy `process_update()` thật sự (scrape, tải, upload ảnh) ở 1 invocation Vercel riêng, có trọn `HEAVY_TASK_TIMEOUT_SEC` để xử lý. Chỉ nhận request có header `X-Internal-Secret` hợp lệ (do `/api/webhook` tự sinh từ `BOT_TOKEN`), không expose công khai cho ai gọi trực tiếp được.

```
Telegram ──POST /api/webhook──▶ handler.do_POST ──▶ dispatch_to_worker() ──▶ ACK ngay cho Telegram
                                                              │
                                                              └─POST /api/process──▶ process_update() ──▶ gọi Weibo API + Telegram Bot API
```

### File chính

| File | Vai trò |
|---|---|
| `api/webhook.py` | Toàn bộ logic bot: nhận webhook, dispatch, scrape Weibo, tải/upload ảnh Telegram |
| `setup_webhook.py` | Script chạy tay 1 lần sau khi deploy, để đăng ký webhook URL với Telegram |
| `requirements.txt` | `python-telegram-bot[webhooks]`, `httpx`, `beautifulsoup4` |
| `vercel.json` | Định tuyến `/`, `/api/webhook`, `/api/process` đều trỏ vào `api/webhook.py` |

## Luồng xử lý (`process_update`)

Bot phản hồi 4 loại update Telegram gửi tới:

1. **`/start`** — gửi tin nhắn hướng dẫn sử dụng.
2. **`/a <weibo_url> [số thứ tự ảnh]`** — tải ảnh trực tiếp không qua preview.
   - VD: `/a https://weibo.com/... ` → tải hết
   - VD: `/a https://weibo.com/... 1,3,5-7` → chỉ tải ảnh số 1, 3, 5, 6, 7 (parse bởi `parse_indices`)
3. **Tin nhắn chứa `weibo.com`/`weibo.cn`** — gọi `show_preview()`: bot gửi thumbnail từng ảnh kèm nút "⬇️ #N — size" + 1 nút "Download All".
4. **Callback query** (khi bấm nút) — 2 dạng, encode trong `callback_data`:
   - `dl_all:<post_id>` → tải toàn bộ album
   - `dl_one:<post_id>:<index>` → tải đúng 1 ảnh theo index

### Scrape ảnh từ Weibo

- Lấy metadata ảnh (`pics` + `pics_more`) từ API không chính thức: `https://m.weibo.cn/statuses/show?id=<post_id>`, giả header mobile Weibo (`HEADERS_API`).
- Với mỗi ảnh, thử các size theo thứ tự ưu tiên `orj1080 → mw2000 → orj480 → large → orj360` bằng `HEAD` request để tìm bản có `content-length` **lớn nhất** (ảnh gốc thật, không phải bản resize nhỏ).
- Nếu domain `wx2.sinaimg.cn` trả về 403, tự động fallback sang `wx1/wx3/wx4.sinaimg.cn`.
- `get_best_url` (chỉ tìm URL, dùng cho preview) và `get_best_url_with_content` (tìm URL **và** tải luôn nội dung trong 1 lượt GET, dùng cho `/a`) phải cùng logic chọn size để số hiển thị trên nút khớp với ảnh thực tải về.

### Gửi ảnh về Telegram

- Ảnh được gửi dưới dạng **document** (`send_document`, không phải `send_photo`) để giữ nguyên chất lượng gốc, không bị Telegram nén.
- `download_and_send_all`: tải ảnh song song (`asyncio.Semaphore(concurrency=3)`) nhưng **upload tuần tự** theo đúng thứ tự #1, #2, #3... để tránh ảnh nhẹ upload xong trước gây lộn thứ tự trong chat.
- `send_as_file` tự retry tối đa 3 lần khi upload lỗi (backoff `3 * attempt` giây).

## Timeout nội bộ — vấn đề cốt lõi của serverless

Vercel sẽ **kill thẳng tiến trình** (không phải raise exception) nếu function chạy quá `maxDuration` — lúc đó bot không kịp gửi tin báo lỗi, chat treo im lặng ở tin cuối cùng.

Giải pháp: `HEAVY_TASK_TIMEOUT_SEC` (env var, mặc định `270`) đặt **nhỏ hơn** `maxDuration` vài chục giây, bọc quanh từng bước nặng (`fetch_pics_meta`, `fetch_content_for_pics`, `download_and_send_all`) bằng `asyncio.wait_for`. Nếu sắp chạm giới hạn, bot kịp gửi tin báo lỗi rõ ràng trước khi bị Vercel kill.

Giá trị khuyến nghị khớp với `maxDuration` trong `vercel.json`:

| Plan | maxDuration | HEAVY_TASK_TIMEOUT_SEC |
|---|---|---|
| Hobby (không Fluid Compute) | 60 | 40 |
| Pro / Enterprise | 300 | 270 |
| Pro / Enterprise (Fluid, GA) | 800 | 760 |

> Lưu ý: `vercel.json` hiện tại chưa khai báo `functions.maxDuration` — cần thêm nếu muốn tăng quá mặc định của Vercel.

`HEAVY_TASK_TIMEOUT_SEC` chỉ bảo vệ khỏi bị **Vercel** kill; nó không giúp gì với timeout riêng của **Telegram** khi chờ webhook phản hồi (ngắn hơn nhiều — quan sát thực tế ~1 phút). Nếu `/api/webhook` tự xử lý nặng và không kịp trả lời trong khoảng đó, Telegram sẽ tự gửi lại nguyên update → chạy trùng, ảnh/tin nhắn gửi ra gấp đôi. Đây là lý do tách `/api/webhook` (chỉ ACK) khỏi `/api/process` (xử lý nặng) ở trên — `/api/webhook` luôn phản hồi rất nhanh bất kể `/api/process` chạy bao lâu.

> Giả định chưa kiểm chứng 100%: cách này dựa vào việc Vercel vẫn chạy tiếp `/api/process` tới khi xong dù client gọi nó (`dispatch_to_worker`) đã bỏ qua chờ response sau `timeout=20`. Nên theo dõi log sau khi deploy để chắc `/api/process` thật sự hoàn tất thay vì bị cắt giữa chừng.

## Callback data encoding

Vì serverless không giữ state giữa các lần gọi, và Telegram giới hạn `callback_data` ở 64 bytes (URL ảnh Weibo dài hơn), bot **không** nhúng URL trực tiếp vào nút mà chỉ nhúng `post_id` + index. Khi user bấm nút, bot scrape lại từ `post_id` để lấy URL ảnh (`make_cb_all`, `make_cb_one`).

## Biến môi trường

| Biến | Bắt buộc | Mô tả |
|---|---|---|
| `BOT_TOKEN` | Có | Token bot Telegram |
| `HEAVY_TASK_TIMEOUT_SEC` | Không (mặc định `270`) | Timeout nội bộ cho tác vụ nặng, xem bảng trên |

## Deploy & thiết lập webhook

1. Deploy project lên Vercel, set biến môi trường `BOT_TOKEN`.
2. Sau khi có URL production, chạy 1 lần để đăng ký webhook với Telegram:
   ```bash
   BOT_TOKEN=xxx VERCEL_URL=https://your-project.vercel.app python setup_webhook.py
   ```
   Script sẽ xóa webhook cũ (nếu có), đăng ký webhook mới trỏ tới `/api/webhook`, và in `getWebhookInfo` để kiểm tra.

## Giới hạn / lưu ý

- Dùng API không chính thức của Weibo mobile (`m.weibo.cn`) — có thể thay đổi/bị chặn bất cứ lúc nào, không có auth nên chỉ scrape được post công khai.
- `HEADERS_IMG["Cookie"]` để trống — nếu Weibo bắt đầu yêu cầu cookie để tải ảnh gốc, cần bổ sung thủ công.
- Mỗi request GET/HEAD lỗi mạng đều bị `except: pass` nuốt lỗi âm thầm (trong `get_best_url*`) — khó debug nếu Weibo đổi API mà không có log rõ ràng ở tầng này.
