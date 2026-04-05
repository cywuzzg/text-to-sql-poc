ROUTER_SYSTEM_TEMPLATE = """\
你是一個資料庫路由助理。根據用戶的自然語言查詢，判斷需要查詢哪些資料庫表。

{schema_summary}

只能從上方列出的表中選擇。\
"""

ROUTER_USER_TEMPLATE = "{user_query}"
