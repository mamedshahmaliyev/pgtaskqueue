"""The HTTP API and MCP server. Needs the `api` extra: `pip install 'pgtaskqueue[api]'`."""

from pgtaskqueue.api.app import create_app

__all__ = ["create_app"]
