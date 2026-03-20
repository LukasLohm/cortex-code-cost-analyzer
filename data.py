import streamlit as st
from datetime import timedelta
import pandas as pd


def get_snowflake_session():
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except ImportError:
        return st.connection("snowflake").session()
    except RuntimeError:
        return st.connection("snowflake").session()


def run_query(sql: str) -> pd.DataFrame:
    session = get_snowflake_session()
    return session.sql(sql).to_pandas()


@st.cache_data(ttl=timedelta(minutes=5))
def get_cli_usage_overview(days: int = 30):
    query = f"""
    SELECT
        COUNT(*) AS TOTAL_REQUESTS,
        COUNT(DISTINCT USER_ID) AS DISTINCT_USERS,
        SUM(TOKEN_CREDITS) AS TOTAL_CREDITS,
        SUM(TOKENS) AS TOTAL_TOKENS,
        MIN(USAGE_TIME) AS EARLIEST_USAGE,
        MAX(USAGE_TIME) AS LATEST_USAGE,
        ROUND(AVG(TOKEN_CREDITS), 6) AS AVG_CREDITS_PER_REQUEST,
        ROUND(MEDIAN(TOKEN_CREDITS), 6) AS MEDIAN_CREDITS_PER_REQUEST
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
    WHERE USAGE_TIME > DATEADD('day', -{int(days)}, CURRENT_TIMESTAMP())
    """
    return run_query(query)


@st.cache_data(ttl=timedelta(minutes=5))
def get_daily_usage(days: int = 30):
    query = f"""
    SELECT
        DATE_TRUNC('day', USAGE_TIME) AS USAGE_DAY,
        COUNT(*) AS REQUESTS,
        SUM(TOKEN_CREDITS) AS DAILY_CREDITS,
        SUM(TOKENS) AS DAILY_TOKENS,
        COUNT(DISTINCT USER_ID) AS ACTIVE_USERS
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
    WHERE USAGE_TIME > DATEADD('day', -{int(days)}, CURRENT_TIMESTAMP())
    GROUP BY USAGE_DAY
    ORDER BY USAGE_DAY
    """
    return run_query(query)


@st.cache_data(ttl=timedelta(minutes=5))
def get_user_usage(days: int = 30):
    query = f"""
    SELECT
        h.USER_ID,
        u.NAME AS USER_NAME,
        u.LOGIN_NAME,
        COUNT(*) AS REQUESTS,
        SUM(h.TOKEN_CREDITS) AS TOTAL_CREDITS,
        SUM(h.TOKENS) AS TOTAL_TOKENS,
        ROUND(AVG(h.TOKEN_CREDITS), 6) AS AVG_CREDITS_PER_REQUEST,
        MIN(h.USAGE_TIME) AS FIRST_USAGE,
        MAX(h.USAGE_TIME) AS LAST_USAGE
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY h
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON h.USER_ID = u.USER_ID
    WHERE h.USAGE_TIME > DATEADD('day', -{int(days)}, CURRENT_TIMESTAMP())
    GROUP BY h.USER_ID, u.NAME, u.LOGIN_NAME
    ORDER BY TOTAL_CREDITS DESC
    """
    return run_query(query)


@st.cache_data(ttl=timedelta(minutes=5))
def get_model_usage(days: int = 30):
    query = f"""
    SELECT
        f.key AS MODEL_NAME,
        COUNT(*) AS REQUESTS,
        SUM(COALESCE(f.value:input::NUMBER, 0)) AS INPUT_TOKENS,
        SUM(COALESCE(f.value:cache_read_input::NUMBER, 0)) AS CACHE_READ_TOKENS,
        SUM(COALESCE(f.value:cache_write_input::NUMBER, 0)) AS CACHE_WRITE_TOKENS,
        SUM(COALESCE(f.value:output::NUMBER, 0)) AS OUTPUT_TOKENS,
        SUM(COALESCE(g.value:input::FLOAT, 0)) AS INPUT_CREDITS,
        SUM(COALESCE(g.value:cache_read_input::FLOAT, 0)) AS CACHE_READ_CREDITS,
        SUM(COALESCE(g.value:cache_write_input::FLOAT, 0)) AS CACHE_WRITE_CREDITS,
        SUM(COALESCE(g.value:output::FLOAT, 0)) AS OUTPUT_CREDITS,
        SUM(h.TOKEN_CREDITS) AS TOTAL_CREDITS,
        SUM(h.TOKENS) AS TOTAL_TOKENS,
        CASE WHEN SUM(COALESCE(f.value:input::NUMBER, 0)) > 0
             THEN ROUND(SUM(COALESCE(g.value:input::FLOAT, 0)) / SUM(COALESCE(f.value:input::NUMBER, 0)) * 1000000, 2)
             ELSE NULL END AS DERIVED_INPUT_RATE,
        CASE WHEN SUM(COALESCE(f.value:cache_read_input::NUMBER, 0)) > 0
             THEN ROUND(SUM(COALESCE(g.value:cache_read_input::FLOAT, 0)) / SUM(COALESCE(f.value:cache_read_input::NUMBER, 0)) * 1000000, 2)
             ELSE NULL END AS DERIVED_CACHE_READ_RATE,
        CASE WHEN SUM(COALESCE(f.value:cache_write_input::NUMBER, 0)) > 0
             THEN ROUND(SUM(COALESCE(g.value:cache_write_input::FLOAT, 0)) / SUM(COALESCE(f.value:cache_write_input::NUMBER, 0)) * 1000000, 2)
             ELSE NULL END AS DERIVED_CACHE_WRITE_RATE,
        CASE WHEN SUM(COALESCE(f.value:output::NUMBER, 0)) > 0
             THEN ROUND(SUM(COALESCE(g.value:output::FLOAT, 0)) / SUM(COALESCE(f.value:output::NUMBER, 0)) * 1000000, 2)
             ELSE NULL END AS DERIVED_OUTPUT_RATE
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY h,
        LATERAL FLATTEN(input => h.TOKENS_GRANULAR) f,
        LATERAL FLATTEN(input => h.CREDITS_GRANULAR) g
    WHERE f.key = g.key
      AND h.USAGE_TIME > DATEADD('day', -{int(days)}, CURRENT_TIMESTAMP())
    GROUP BY f.key
    ORDER BY TOTAL_CREDITS DESC
    """
    return run_query(query)


@st.cache_data(ttl=timedelta(minutes=5))
def get_token_type_breakdown(days: int = 30):
    query = f"""
    SELECT
        f.key AS MODEL_NAME,
        SUM(COALESCE(f.value:input::NUMBER, 0)) AS INPUT_TOKENS,
        SUM(COALESCE(f.value:cache_read_input::NUMBER, 0)) AS CACHE_READ_TOKENS,
        SUM(COALESCE(f.value:cache_write_input::NUMBER, 0)) AS CACHE_WRITE_TOKENS,
        SUM(COALESCE(f.value:output::NUMBER, 0)) AS OUTPUT_TOKENS,
        SUM(COALESCE(g.value:input::FLOAT, 0)) AS INPUT_CREDITS,
        SUM(COALESCE(g.value:cache_read_input::FLOAT, 0)) AS CACHE_READ_CREDITS,
        SUM(COALESCE(g.value:cache_write_input::FLOAT, 0)) AS CACHE_WRITE_CREDITS,
        SUM(COALESCE(g.value:output::FLOAT, 0)) AS OUTPUT_CREDITS
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY h,
        LATERAL FLATTEN(input => h.TOKENS_GRANULAR) f,
        LATERAL FLATTEN(input => h.CREDITS_GRANULAR) g
    WHERE f.key = g.key
      AND h.USAGE_TIME > DATEADD('day', -{int(days)}, CURRENT_TIMESTAMP())
    GROUP BY f.key
    ORDER BY INPUT_CREDITS + CACHE_READ_CREDITS + CACHE_WRITE_CREDITS + OUTPUT_CREDITS DESC
    """
    return run_query(query)


@st.cache_data(ttl=timedelta(minutes=5))
def get_cost_distribution(days: int = 30):
    query = f"""
    SELECT
        ROUND(MIN(TOKEN_CREDITS), 6) AS MIN_CREDITS,
        ROUND(AVG(TOKEN_CREDITS), 6) AS AVG_CREDITS,
        ROUND(MEDIAN(TOKEN_CREDITS), 6) AS MEDIAN_CREDITS,
        ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY TOKEN_CREDITS), 6) AS P90_CREDITS,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY TOKEN_CREDITS), 6) AS P95_CREDITS,
        ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY TOKEN_CREDITS), 6) AS P99_CREDITS,
        ROUND(MAX(TOKEN_CREDITS), 6) AS MAX_CREDITS,
        COUNT(*) AS TOTAL_REQUESTS
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY
    WHERE USAGE_TIME > DATEADD('day', -{int(days)}, CURRENT_TIMESTAMP())
    """
    return run_query(query)


@st.cache_data(ttl=timedelta(minutes=5))
def get_request_details(days: int = 30, limit: int = 100):
    query = f"""
    SELECT
        h.REQUEST_ID,
        h.USER_ID,
        u.NAME AS USER_NAME,
        h.USAGE_TIME,
        h.TOKEN_CREDITS,
        h.TOKENS,
        h.TOKENS_GRANULAR,
        h.CREDITS_GRANULAR
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY h
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON h.USER_ID = u.USER_ID
    WHERE h.USAGE_TIME > DATEADD('day', -{int(days)}, CURRENT_TIMESTAMP())
    ORDER BY h.TOKEN_CREDITS DESC
    LIMIT {int(limit)}
    """
    return run_query(query)


@st.cache_data(ttl=timedelta(minutes=5))
def get_daily_usage_by_model(days: int = 30):
    query = f"""
    SELECT
        DATE_TRUNC('day', h.USAGE_TIME) AS USAGE_DAY,
        f.key AS MODEL_NAME,
        COUNT(*) AS REQUESTS,
        SUM(h.TOKEN_CREDITS) AS DAILY_CREDITS,
        SUM(h.TOKENS) AS DAILY_TOKENS
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY h,
        LATERAL FLATTEN(input => h.TOKENS_GRANULAR) f
    WHERE h.USAGE_TIME > DATEADD('day', -{int(days)}, CURRENT_TIMESTAMP())
    GROUP BY USAGE_DAY, f.key
    ORDER BY USAGE_DAY, f.key
    """
    return run_query(query)
