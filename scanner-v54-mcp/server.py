"""Read-only MCP bridge; no scanner strategy code lives here."""
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

import httpx
from jsonschema import Draft202012Validator, FormatChecker
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from starlette.responses import JSONResponse

ROOT = Path(__file__).resolve().parent
BASE = "https://binance-usdtp-scanner-production.up.railway.app"
INSTRUCTIONS = """使用者說「掃盤」時呼叫 scan_market_v54。僅依本次成功 feed 呈現 1H、4H 各自 entry 前10筆、wait_stop、wait_pullback、overextended 全部清單，以及 resonance。保留原始分類、順序與分數，不重新排名或重算共振。標示 generated_at_utc（可另換算台北時間）、scanner、strategy。null 顯示未提供；stop 是止跌K訊號，不是停損價。status 非 complete、格式錯誤或連線失敗時說明未取得完整掃盤，不使用舊結果、不自動重試、不呼叫 cache/init。generated_at_utc 是回應產生時間，不能當作行情K線新鮮度證明。"""
validator = Draft202012Validator(json.loads((ROOT / "schemas/feed.schema.json").read_text()), format_checker=FormatChecker())
hosts = ["localhost:*", "127.0.0.1:*", "[::1]:*"]
if os.environ.get("RAILWAY_PUBLIC_DOMAIN"):
    hosts.append(os.environ["RAILWAY_PUBLIC_DOMAIN"])
mcp = FastMCP(
    "Binance Scanner V5.4", instructions=INSTRUCTIONS,
    host="0.0.0.0", port=int(os.environ.get("PORT", "8000")),
    stateless_http=True, json_response=True,
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True, allowed_hosts=hosts,
        allowed_origins=["http://localhost:*", "http://127.0.0.1:*"],
    ),
)
scan_lock = asyncio.Lock()
annotations = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True)

async def fetch(path: str) -> dict:
    if path not in ("/health", "/scan/feed"):
        raise ValueError("Endpoint is not allowed")
    try:
        async with asyncio.timeout(90):
            async with httpx.AsyncClient(timeout=85, follow_redirects=False) as client:
                response = await client.get(BASE + path)
                response.raise_for_status()
                data = response.json()
        if not isinstance(data, dict):
            raise ValueError("Expected a JSON object")
        return data
    except (httpx.HTTPError, TimeoutError, ValueError) as exc:
        raise RuntimeError("Railway request failed; no complete scan available. Do not automatically retry or initialize cache.") from exc

@mcp.tool(annotations=annotations)
async def get_scanner_health() -> dict[str, Any]:
    """Check Railway scanner health without scanning or initializing cache."""
    return await fetch("/health")

@mcp.tool(annotations=annotations)
async def scan_market_v54() -> dict[str, Any]:
    """掃盤：read Railway /scan/feed once. Return locked 1H, 4H, watchlists and resonance unchanged. May take 41–90 seconds. No strategy inputs, no trading, no cache/init."""
    if scan_lock.locked():
        raise RuntimeError("A scan is already running. Wait for its result; do not retry concurrently.")
    async with scan_lock:
        data = await fetch("/scan/feed")
        validator.validate(data)
        return data

@mcp.custom_route("/health", methods=["GET"])
async def bridge_health(request):
    return JSONResponse({"status": "ok", "service": "scanner-v54-mcp-bridge"})

if __name__ == "__main__":
    mcp.run(transport="stdio" if "--stdio" in sys.argv else "streamable-http")
