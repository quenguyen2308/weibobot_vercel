#!/usr/bin/env python3
"""
Chạy script này SAU KHI deploy lên Vercel để đăng ký webhook với Telegram.
Usage: BOT_TOKEN=xxx VERCEL_URL=https://your-project.vercel.app python setup_webhook.py
"""

import os
import httpx
import asyncio

BOT_TOKEN = os.environ["BOT_TOKEN"]
VERCEL_URL = os.environ["VERCEL_URL"].rstrip("/")
WEBHOOK_URL = f"{VERCEL_URL}/api/webhook"

async def main():
    async with httpx.AsyncClient() as client:
        # Xóa webhook cũ (nếu có từ Railway)
        r = await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook",
            json={"drop_pending_updates": True}
        )
        print("deleteWebhook:", r.json())

        # Đăng ký webhook mới
        r = await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
            json={
                "url": WEBHOOK_URL,
                "allowed_updates": ["message", "callback_query"],
                "drop_pending_updates": True,
            }
        )
        print("setWebhook:", r.json())

        # Kiểm tra
        r = await client.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getWebhookInfo")
        print("getWebhookInfo:", r.json())

if __name__ == "__main__":
    asyncio.run(main())
