import pandas as pd


def format_credits(value):
    if value is None:
        return "0.0000"
    try:
        if pd.isna(value):
            return "0.0000"
        if value < 0.0001:
            return f"{value:.6f}"
        return f"{value:.4f}"
    except (ValueError, TypeError):
        return "0.0000"


def format_tokens(value):
    if value is None:
        return "0"
    try:
        if pd.isna(value):
            return "0"
        return f"{int(value):,}"
    except (ValueError, TypeError):
        return "0"


def safe_float(value, default=0.0):
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value, default=0):
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
        return int(value)
    except (ValueError, TypeError):
        return default
