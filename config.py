import streamlit as st

PAGE_CONFIG = {
    "page_title": "Cortex Code Cost Analyzer",
    "page_icon": ":computer:",
    "layout": "wide",
}

DEFAULT_LOOKBACK_DAYS = 30

CORTEX_CODE_CLI_PRICING = {
    "claude-opus-4-5": {"input": 2.75, "cache_read_input": 0.28, "cache_write_input": 3.44, "output": 13.75},
    "claude-opus-4-6": {"input": 2.75, "cache_read_input": 0.28, "cache_write_input": 3.44, "output": 13.75},
    "claude-sonnet-4-5": {"input": 1.65, "cache_read_input": 0.17, "cache_write_input": 2.07, "output": 8.25},
    "claude-sonnet-4-6": {"input": 1.65, "cache_read_input": 0.17, "cache_write_input": 2.07, "output": 8.25},
    "claude-4-sonnet": {"input": 1.50, "cache_read_input": 0.15, "cache_write_input": 1.88, "output": 7.53},
    "openai-gpt-5.2": {"input": 0.97, "cache_read_input": 0.10, "cache_write_input": 1.21, "output": 7.74},
}

TOKEN_TYPES = ["input", "cache_read_input", "cache_write_input", "output"]

TOKEN_TYPE_LABELS = {
    "input": "Input",
    "cache_read_input": "Cache Read",
    "cache_write_input": "Cache Write",
    "output": "Output",
}
