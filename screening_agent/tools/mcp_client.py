"""Thin wrapper around the official `mcp` Python SDK.

Spawns one `stdio_client` per server defined in `mcp_servers.json`, lists the
tools each server exposes, and offers a single `call_tool(name, args)` method
that routes the call to the right server.

Designed to be called from synchronous agent code, so the async machinery is
hidden inside one persistent event loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any

from .. import config as cfg

logger = logging.getLogger(__name__)


class _MCPRouter:
    """Owns the asyncio loop + sessions. Use via `MCPClient` from sync code."""

    def __init__(self) -> None:
        self._sessions: dict[str, Any] = {}      # server_name -> ClientSession
        self._tools: dict[str, str] = {}         # tool_name   -> server_name
        self._tool_specs: list[dict[str, Any]] = []
        self._stack: AsyncExitStack | None = None
        self._started = False

    async def start(self, config_path: Path) -> None:
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client

        cfg_data = json.loads(config_path.read_text())
        servers = cfg_data.get("mcpServers", {})
        if not servers:
            logger.warning("No MCP servers configured in %s", config_path)
            return

        self._stack = AsyncExitStack()
        await self._stack.__aenter__()

        for name, spec in servers.items():
            try:
                params = StdioServerParameters(
                    command=spec["command"],
                    args=spec.get("args", []),
                    env=spec.get("env"),
                )
                read, write = await self._stack.enter_async_context(stdio_client(params))
                session = await self._stack.enter_async_context(ClientSession(read, write))
                await session.initialize()
                tools = await session.list_tools()
                for t in tools.tools:
                    qualified = f"{name}.{t.name}"
                    self._sessions[qualified] = session
                    self._tools[qualified] = name
                    self._tool_specs.append({
                        "server": name,
                        "name": qualified,
                        "raw_name": t.name,
                        "description": t.description,
                        "input_schema": t.inputSchema,
                    })
                logger.info("MCP server '%s' ready (%d tools)", name, len(tools.tools))
            except Exception as e:  # noqa: BLE001 - we want graceful fallback
                logger.error("Failed to start MCP server '%s': %s", name, e)
                if not cfg.MCP["graceful_fallback"]:
                    raise

        self._started = True

    async def call(self, qualified_tool_name: str, arguments: dict[str, Any]) -> Any:
        session = self._sessions.get(qualified_tool_name)
        if session is None:
            raise KeyError(f"Unknown MCP tool: {qualified_tool_name}")
        raw_name = qualified_tool_name.split(".", 1)[1]
        result = await session.call_tool(raw_name, arguments=arguments)
        # Result has .content (list of TextContent / etc). Stringify pragmatically.
        parts = []
        for item in result.content:
            text = getattr(item, "text", None)
            if text is not None:
                parts.append(text)
            else:
                parts.append(str(item))
        return "\n".join(parts) if parts else ""

    async def close(self) -> None:
        if self._stack is not None:
            await self._stack.__aexit__(None, None, None)
            self._stack = None
        self._started = False


class MCPClient:
    """Sync facade around _MCPRouter — runs an asyncio loop in a side thread."""

    def __init__(self) -> None:
        self._router = _MCPRouter()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()

    def start(self) -> None:
        if self._thread is not None:
            return

        def _runner():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            try:
                self._loop.run_until_complete(
                    self._router.start(Path(cfg.MCP["config_file"]))
                )
            finally:
                self._ready.set()
                self._loop.run_forever()

        self._thread = threading.Thread(target=_runner, daemon=True, name="mcp-loop")
        self._thread.start()
        self._ready.wait(timeout=60)

    def list_tools(self) -> list[dict[str, Any]]:
        return list(self._router._tool_specs)  # noqa: SLF001 - intentional

    def call_tool(self, name: str, arguments: dict[str, Any], *, timeout: float = 120) -> Any:
        if self._loop is None:
            raise RuntimeError("MCPClient not started")
        fut = asyncio.run_coroutine_threadsafe(
            self._router.call(name, arguments), self._loop,
        )
        return fut.result(timeout=timeout)

    def close(self) -> None:
        if self._loop is None:
            return
        asyncio.run_coroutine_threadsafe(self._router.close(), self._loop).result(30)
        self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=10)
        self._loop = None
        self._thread = None


# Module-level singleton — most agents only need one.
_singleton: MCPClient | None = None


def get_client() -> MCPClient:
    global _singleton
    if _singleton is None:
        _singleton = MCPClient()
        _singleton.start()
    return _singleton
