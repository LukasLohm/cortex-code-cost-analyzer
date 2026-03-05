"""
Cortex Agent Cost Analyzer
==========================
A Streamlit app to analyze Cortex Agent execution traces and costs.
Provides detailed trace spans, Cortex Analyst SQL queries, and cost breakdowns.

Uses SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS for detailed agent observability.

Requirements:
- ACCOUNTADMIN role OR granted access to SNOWFLAKE.ACCOUNT_USAGE schema
- Access to the agent's database/schema for observability events
"""

import streamlit as st
from datetime import timedelta
import pandas as pd
import json

# Page config
st.set_page_config(
    page_title="Cortex Agent Cost Analyzer",
    page_icon=":robot_face:",
    layout="wide",
)


def get_snowflake_session():
    """Get Snowflake session - works in both SiS and local environments."""
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception:
        return st.connection("snowflake").session()


def run_query(sql: str) -> pd.DataFrame:
    """Execute a query and return results as DataFrame."""
    session = get_snowflake_session()
    return session.sql(sql).to_pandas()


def format_credits(value):
    """Format credit values for display."""
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
    """Format token counts for display."""
    if value is None:
        return "0"
    try:
        if pd.isna(value):
            return "0"
        return f"{int(value):,}"
    except (ValueError, TypeError):
        return "0"


# =============================================================================
# DATA FETCHING FUNCTIONS
# =============================================================================

@st.cache_data(ttl=timedelta(minutes=5))
def get_available_agents():
    """Get list of available Cortex Agents in the account."""
    query = """
    SELECT DISTINCT
        AGENT_DATABASE_NAME,
        AGENT_SCHEMA_NAME,
        AGENT_NAME,
        COUNT(*) as REQUEST_COUNT,
        SUM(TOKEN_CREDITS) as TOTAL_CREDITS
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
    WHERE AGENT_NAME IS NOT NULL
      AND START_TIME > DATEADD('day', -30, CURRENT_TIMESTAMP())
    GROUP BY AGENT_DATABASE_NAME, AGENT_SCHEMA_NAME, AGENT_NAME
    ORDER BY TOTAL_CREDITS DESC
    """
    return run_query(query)


@st.cache_data(ttl=timedelta(minutes=5))
def get_agent_threads(agent_db: str, agent_schema: str, agent_name: str, limit: int = 50):
    """Get recent thread IDs for a specific agent."""
    query = f"""
    SELECT DISTINCT
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.thread_id"::NUMBER AS THREAD_ID,
        MIN(START_TIMESTAMP) AS FIRST_TIMESTAMP,
        MAX(TIMESTAMP) AS LAST_TIMESTAMP,
        COUNT(*) AS SPAN_COUNT
    FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(
        '{agent_db}', '{agent_schema}', '{agent_name}', 'CORTEX AGENT'
    ))
    WHERE RECORD_ATTRIBUTES:"snow.ai.observability.agent.thread_id" IS NOT NULL
    GROUP BY THREAD_ID
    ORDER BY LAST_TIMESTAMP DESC
    LIMIT {limit}
    """
    return run_query(query)


@st.cache_data(ttl=timedelta(minutes=5))
def get_agent_trace_spans(agent_db: str, agent_schema: str, agent_name: str, thread_id: int):
    """Get detailed trace spans for an agent thread."""
    query = f"""
    SELECT 
        RECORD_ATTRIBUTES:"request_id"::STRING AS REQUEST_ID,
        RECORD:"name"::STRING AS SPAN_NAME,
        START_TIMESTAMP,
        TIMESTAMP AS END_TIMESTAMP,
        TIMESTAMPDIFF('millisecond', START_TIMESTAMP, TIMESTAMP) AS DURATION_MS,
        RESOURCE_ATTRIBUTES:"snow.user.name"::STRING AS USER_NAME,
        RECORD:"status":"code"::STRING AS STATUS_CODE,
        ROW_NUMBER() OVER (ORDER BY START_TIMESTAMP) AS STEP_ORDER,
        CASE 
            WHEN RECORD:"name"::STRING = 'Agent' THEN 'ORCHESTRATION'
            WHEN RECORD:"name"::STRING LIKE 'ReasoningAgentStep%' THEN 'LLM_REASONING'
            WHEN RECORD:"name"::STRING LIKE 'CortexAnalystTool%' THEN 'CORTEX_ANALYST'
            WHEN RECORD:"name"::STRING LIKE 'SqlExecution_CortexAnalyst%' THEN 'ANALYST_SQL'
            WHEN RECORD:"name"::STRING LIKE 'ToolCall-%' THEN 'TOOL_CALL'
            WHEN RECORD:"name"::STRING = 'SqlExecution' THEN 'SQL_EXECUTION'
            ELSE 'OTHER'
        END AS COST_CATEGORY,
        -- Cortex Analyst specific fields
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_analyst.sql_query"::STRING AS ANALYST_SQL_QUERY,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_analyst.status"::STRING AS ANALYST_STATUS,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_analyst.status.code"::STRING AS ANALYST_STATUS_CODE,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_analyst.semantic_model"::STRING AS SEMANTIC_MODEL,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_analyst.question_category"::STRING AS QUESTION_CATEGORY,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_analyst.messages"::STRING AS ANALYST_MESSAGES,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_analyst.text"::STRING AS ANALYST_RESPONSE_TEXT,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_analyst.think"::STRING AS ANALYST_THINKING,
        -- SQL Execution specific fields
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.sql_execution.query"::STRING AS SQL_QUERY,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.sql_execution.query_id"::STRING AS SQL_QUERY_ID,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.sql_execution.duration"::NUMBER AS SQL_DURATION_MS,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.sql_execution.result"::STRING AS SQL_RESULT,
        -- Custom Tool specific fields
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.custom_tool.name"::STRING AS CUSTOM_TOOL_NAME,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.custom_tool.argument.name"::STRING AS CUSTOM_TOOL_ARG_NAMES,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.custom_tool.argument.value"::STRING AS CUSTOM_TOOL_ARG_VALUES,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.custom_tool.results"::STRING AS CUSTOM_TOOL_RESULTS,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.custom_tool.status"::STRING AS CUSTOM_TOOL_STATUS,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.custom_tool.status.code"::STRING AS CUSTOM_TOOL_STATUS_CODE,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.custom_tool.duration"::NUMBER AS CUSTOM_TOOL_DURATION_MS
    FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(
        '{agent_db}', '{agent_schema}', '{agent_name}', 'CORTEX AGENT'
    ))
    WHERE RECORD_ATTRIBUTES:"snow.ai.observability.agent.thread_id"::NUMBER = {thread_id}
      AND RECORD:"name"::STRING NOT IN ('AgentV2RequestResponseInfo', 'CORTEX_AGENT_REQUEST')
    ORDER BY START_TIMESTAMP
    """
    return run_query(query)


@st.cache_data(ttl=timedelta(minutes=5))
def get_agent_cost_summary(agent_db: str, agent_schema: str, agent_name: str, thread_id: int):
    """Get cost summary by category for an agent thread.
    
    Calculates credits using a hybrid approach:
    1. LLM Reasoning: Real-time token data from observability API
    2. Cortex Analyst: 
       - For threads < 3 hours old: Estimated using Table 6(i) per-call pricing
       - For threads >= 3 hours old: Accurate tokens from CORTEX_AGENT_USAGE_HISTORY
    
    Official credit rates from Snowflake Credit Consumption Table (March 2026):
    
    Table 6(e) - Cortex Agents (Credits per million tokens)
    Table 6(f) - Cortex Analyst via Cortex Agents (Credits per million tokens)
    Table 6(i) - Cortex Analyst per-call: 67 credits per 1,000 messages (0.067/call)
    
    All model prices are included and dynamically applied based on the model used.
    
    Warehouse compute: Based on warehouse size multiplier and execution time
    """
    query = """
    WITH 
    -- Table 6(e): LLM Reasoning / Cortex Agents Orchestration pricing (Credits per million tokens)
    cortex_agents_pricing AS (
        SELECT * FROM (VALUES
            ('claude-3-5-sonnet', 1.88, 9.41),
            ('claude-3-7-sonnet', 1.88, 9.41),
            ('claude-4-opus', 9.41, 47.07),
            ('claude-4-sonnet', 1.88, 9.41),
            ('claude-haiku-4-5', 0.69, 3.45),
            ('claude-opus-4-5', 3.45, 17.26),
            ('claude-opus-4-6', 3.45, 17.26),
            ('claude-sonnet-4-5', 2.07, 10.36),
            ('claude-sonnet-4-6', 2.07, 10.36),
            ('gemini-2-5-flash', 0.19, 1.57),
            ('gemini-3-pro', 1.26, 7.53),
            ('llama-4-maverick', 0.17, 1.38),
            ('llama3.1-405b', 1.26, 5.02),
            ('llama3.1-70b', 0.60, 0.60),
            ('llama3.1-8b', 0.17, 0.17),
            ('llama3.2-1b', 0.02, 0.02),
            ('llama3.2-3b', 0.03, 0.03),
            ('llama3.3-70b', 0.60, 0.60),
            ('llama4-scout', 0.17, 1.38),
            ('mistral-large', 1.26, 3.77),
            ('mistral-large2', 0.60, 1.76),
            ('openai-gpt-4.1', 1.38, 5.52),
            ('openai-gpt-5', 0.86, 6.90),
            ('openai-gpt-5-mini', 0.17, 1.38),
            ('openai-gpt-5.1', 0.86, 6.90),
            ('openai-gpt-5.2', 1.21, 9.67),
            ('reka-core', 1.88, 9.41),
            ('reka-flash', 0.21, 0.62)
        ) AS t(model_name, input_rate, output_rate)
    ),
    -- NOTE: Table 6(f) rates for Cortex Analyst are different from Table 6(e)!
    -- For estimation, we assume claude-4-sonnet (most common model): Input 3.14, Output 15.69
    -- For accurate data, credits come directly from CORTEX_AGENT_USAGE_HISTORY with correct rates
    trace_spans AS (
        SELECT 
            RECORD:"name"::STRING AS span_name,
            START_TIMESTAMP,
            TIMESTAMP AS end_timestamp,
            TIMESTAMPDIFF('millisecond', START_TIMESTAMP, TIMESTAMP) AS duration_ms,
            CASE 
                WHEN RECORD:"name"::STRING = 'Agent' THEN 'ORCHESTRATION'
                WHEN RECORD:"name"::STRING LIKE 'ReasoningAgentStep%' THEN 'LLM_REASONING'
                WHEN RECORD:"name"::STRING LIKE 'CortexAnalystTool%' THEN 'CORTEX_ANALYST'
                WHEN RECORD:"name"::STRING LIKE 'SqlExecution_CortexAnalyst%' THEN 'ANALYST_SQL'
                WHEN RECORD:"name"::STRING LIKE 'ToolCall-%' THEN 'TOOL_CALL'
                WHEN RECORD:"name"::STRING = 'SqlExecution' THEN 'SQL_EXECUTION'
                ELSE 'OTHER'
            END AS cost_category,
            -- Extract model name from observability data
            COALESCE(
                RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.model"::STRING,
                RECORD_ATTRIBUTES:"snow.ai.observability.model"::STRING,
                RECORD_ATTRIBUTES:"model"::STRING
            ) AS raw_model_name,
            COALESCE(
                RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.model"::STRING,
                RECORD_ATTRIBUTES:"snow.ai.observability.model"::STRING,
                RECORD_ATTRIBUTES:"model"::STRING,
                '(not found - using default: claude-3-5-sonnet)'
            ) AS model_name,
            -- Token counts from observability data (for LLM_REASONING spans)
            RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.token_count.input"::NUMBER AS input_tokens,
            RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.token_count.output"::NUMBER AS output_tokens,
            RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.token_count.total"::NUMBER AS total_tokens,
            -- Cortex Analyst request_id to join with usage history
            RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_analyst.request_id"::STRING AS analyst_request_id,
            -- Cortex Analyst content for token estimation (when accurate data not yet available)
            RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_analyst.messages"::STRING AS analyst_messages,
            RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_analyst.text"::STRING AS analyst_response_text,
            RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_analyst.think"::STRING AS analyst_thinking,
            -- Query IDs for SQL executions
            RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.sql_execution.query_id"::STRING AS query_id,
            -- Record ID to correlate with CORTEX_AGENT_USAGE_HISTORY
            RECORD_ATTRIBUTES:"ai.observability.record_id"::STRING AS record_id
        FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(
            '{agent_db}', '{agent_schema}', '{agent_name}', 'CORTEX AGENT'
        ))
        WHERE RECORD_ATTRIBUTES:"snow.ai.observability.agent.thread_id"::NUMBER = {thread_id}
          AND RECORD:"name"::STRING NOT IN ('AgentV2RequestResponseInfo', 'CORTEX_AGENT_REQUEST')
    ),
    -- Check thread age to determine if usage history is available
    thread_timing AS (
        SELECT 
            MIN(START_TIMESTAMP) AS thread_start,
            TIMESTAMPDIFF('hour', MIN(START_TIMESTAMP), CURRENT_TIMESTAMP()) AS hours_old
        FROM trace_spans
    ),
    -- Get the record_id for this thread to join with usage history
    thread_record_id AS (
        SELECT DISTINCT record_id
        FROM trace_spans
        WHERE record_id IS NOT NULL
        LIMIT 1
    ),
    -- Get granular Cortex Analyst token/credit data from ACCOUNT_USAGE (if available)
    -- This has 2-3 hour latency but provides accurate token breakdown
    analyst_usage_data AS (
        SELECT 
            u.REQUEST_ID,
            u.TOKENS AS total_agent_tokens,
            u.TOKEN_CREDITS AS total_agent_credits,
            u.TOKENS_GRANULAR,
            u.CREDITS_GRANULAR
        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY u
        WHERE u.REQUEST_ID IN (SELECT record_id FROM thread_record_id)
    ),
    -- Parse Cortex Analyst tokens from TOKENS_GRANULAR array
    -- Structure: [{{request_id: {{cortex_analyst: {{model: {{input: N, output: N}}}}}}}}]
    analyst_token_details AS (
        SELECT 
            SUM(
                COALESCE(inner_f.value:cortex_analyst['claude-4-sonnet']['input']::NUMBER, 0) +
                COALESCE(inner_f.value:cortex_analyst['claude-sonnet-4-5']['input']::NUMBER, 0) +
                COALESCE(inner_f.value:cortex_analyst['claude-3-5-sonnet']['input']::NUMBER, 0)
            ) AS analyst_input_tokens,
            SUM(
                COALESCE(inner_f.value:cortex_analyst['claude-4-sonnet']['output']::NUMBER, 0) +
                COALESCE(inner_f.value:cortex_analyst['claude-sonnet-4-5']['output']::NUMBER, 0) +
                COALESCE(inner_f.value:cortex_analyst['claude-3-5-sonnet']['output']::NUMBER, 0)
            ) AS analyst_output_tokens,
            SUM(
                COALESCE(inner_f.value:cortex_analyst['claude-4-sonnet']['input']::NUMBER, 0) +
                COALESCE(inner_f.value:cortex_analyst['claude-4-sonnet']['output']::NUMBER, 0) +
                COALESCE(inner_f.value:cortex_analyst['claude-sonnet-4-5']['input']::NUMBER, 0) +
                COALESCE(inner_f.value:cortex_analyst['claude-sonnet-4-5']['output']::NUMBER, 0) +
                COALESCE(inner_f.value:cortex_analyst['claude-3-5-sonnet']['input']::NUMBER, 0) +
                COALESCE(inner_f.value:cortex_analyst['claude-3-5-sonnet']['output']::NUMBER, 0)
            ) AS analyst_tokens
        FROM analyst_usage_data u,
        LATERAL FLATTEN(input => u.TOKENS_GRANULAR, OUTER => TRUE) f,
        LATERAL FLATTEN(input => f.value, OUTER => TRUE) inner_f
        WHERE inner_f.value:cortex_analyst IS NOT NULL
    ),
    -- Parse Cortex Analyst credits from CREDITS_GRANULAR array
    analyst_credit_details AS (
        SELECT 
            SUM(
                COALESCE(inner_g.value:cortex_analyst['claude-4-sonnet']['input']::FLOAT, 0) +
                COALESCE(inner_g.value:cortex_analyst['claude-sonnet-4-5']['input']::FLOAT, 0) +
                COALESCE(inner_g.value:cortex_analyst['claude-3-5-sonnet']['input']::FLOAT, 0)
            ) AS analyst_input_credits,
            SUM(
                COALESCE(inner_g.value:cortex_analyst['claude-4-sonnet']['output']::FLOAT, 0) +
                COALESCE(inner_g.value:cortex_analyst['claude-sonnet-4-5']['output']::FLOAT, 0) +
                COALESCE(inner_g.value:cortex_analyst['claude-3-5-sonnet']['output']::FLOAT, 0)
            ) AS analyst_output_credits,
            SUM(
                COALESCE(inner_g.value:cortex_analyst['claude-4-sonnet']['input']::FLOAT, 0) +
                COALESCE(inner_g.value:cortex_analyst['claude-4-sonnet']['output']::FLOAT, 0) +
                COALESCE(inner_g.value:cortex_analyst['claude-sonnet-4-5']['input']::FLOAT, 0) +
                COALESCE(inner_g.value:cortex_analyst['claude-sonnet-4-5']['output']::FLOAT, 0) +
                COALESCE(inner_g.value:cortex_analyst['claude-3-5-sonnet']['input']::FLOAT, 0) +
                COALESCE(inner_g.value:cortex_analyst['claude-3-5-sonnet']['output']::FLOAT, 0)
            ) AS analyst_credits
        FROM analyst_usage_data u,
        LATERAL FLATTEN(input => u.CREDITS_GRANULAR, OUTER => TRUE) g,
        LATERAL FLATTEN(input => g.value, OUTER => TRUE) inner_g
        WHERE inner_g.value:cortex_analyst IS NOT NULL
    ),
    -- Calculate LLM token costs using Table 6(e) rates based on actual model
    llm_token_summary AS (
        SELECT 
            COALESCE(SUM(ts.input_tokens), 0) AS total_input_tokens,
            COALESCE(SUM(ts.output_tokens), 0) AS total_output_tokens,
            COALESCE(SUM(ts.total_tokens), 0) AS total_tokens,
            LISTAGG(DISTINCT ts.model_name, ', ') AS models_used,
            -- Get the primary model's rates for display (use first model if multiple)
            MAX(COALESCE(p.input_rate, 1.88)) AS input_rate_used,
            MAX(COALESCE(p.output_rate, 9.41)) AS output_rate_used,
            ROUND(SUM(
                (COALESCE(ts.input_tokens, 0) * COALESCE(p.input_rate, 1.88) / 1000000.0) + 
                (COALESCE(ts.output_tokens, 0) * COALESCE(p.output_rate, 9.41) / 1000000.0)
            ), 6) AS llm_credits,
            -- Store individual credit calculations for transparency
            ROUND(SUM(COALESCE(ts.input_tokens, 0) * COALESCE(p.input_rate, 1.88) / 1000000.0), 6) AS input_credits,
            ROUND(SUM(COALESCE(ts.output_tokens, 0) * COALESCE(p.output_rate, 9.41) / 1000000.0), 6) AS output_credits
        FROM trace_spans ts
        LEFT JOIN cortex_agents_pricing p ON LOWER(ts.raw_model_name) = LOWER(p.model_name)
        WHERE ts.cost_category = 'LLM_REASONING'
    ),
    -- Cortex Analyst: Token-based pricing (Table 6f)
    -- When accurate data not available, estimate tokens from message content length
    -- Estimation: ~1 token per 4 characters (standard approximation)
    analyst_content_estimate AS (
        SELECT 
            COUNT(*) AS analyst_calls,
            -- Sum of all input characters (messages/questions sent to analyst)
            SUM(COALESCE(LENGTH(analyst_messages), 0)) AS total_input_chars,
            -- Sum of all output characters (response text + thinking)
            SUM(COALESCE(LENGTH(analyst_response_text), 0) + COALESCE(LENGTH(analyst_thinking), 0)) AS total_output_chars,
            -- Estimate tokens: ~4 characters per token
            ROUND(SUM(COALESCE(LENGTH(analyst_messages), 0)) / 4.0) AS estimated_input_tokens,
            ROUND(SUM(COALESCE(LENGTH(analyst_response_text), 0) + COALESCE(LENGTH(analyst_thinking), 0)) / 4.0) AS estimated_output_tokens
        FROM trace_spans 
        WHERE cost_category = 'CORTEX_ANALYST'
    ),
    -- Calculate estimated credits using Table 6(f) rates for claude-4-sonnet (default Analyst model)
    -- Input: 3.14 credits/million tokens, Output: 15.69 credits/million tokens
    -- NOTE: These are Table 6(f) rates, NOT Table 6(e) LLM Reasoning rates!
    analyst_estimated_credits AS (
        SELECT 
            ace.analyst_calls,
            ace.total_input_chars,
            ace.total_output_chars,
            ace.estimated_input_tokens,
            ace.estimated_output_tokens,
            ace.estimated_input_tokens + ace.estimated_output_tokens AS estimated_total_tokens,
            ROUND((ace.estimated_input_tokens * 3.14 / 1000000.0) + (ace.estimated_output_tokens * 15.69 / 1000000.0), 6) AS estimated_credits,
            -- Store rates used for display
            3.14 AS input_rate_used,
            15.69 AS output_rate_used,
            'claude-4-sonnet' AS model_assumed
        FROM analyst_content_estimate ace
    ),
    analyst_summary AS (
        SELECT 
            aec.analyst_calls,
            -- Tokens: Use accurate data if available, otherwise use content-based estimate
            CASE 
                WHEN atd.analyst_tokens IS NOT NULL AND atd.analyst_tokens > 0 
                THEN atd.analyst_tokens
                ELSE aec.estimated_total_tokens
            END AS analyst_tokens,
            -- Credits: Use accurate data if available, otherwise use content-based estimate
            CASE 
                WHEN acd.analyst_credits IS NOT NULL AND acd.analyst_credits > 0 
                THEN acd.analyst_credits
                ELSE aec.estimated_credits
            END AS analyst_credits,
            -- Flag to indicate data source with clear explanation
            CASE 
                WHEN acd.analyst_credits IS NOT NULL AND acd.analyst_credits > 0 
                THEN 'accurate'
                ELSE 'estimated'
            END AS data_source,
            -- Time until accurate data available
            tt.hours_old,
            GREATEST(0, 3 - tt.hours_old) AS hours_until_accurate,
            -- Estimation details for transparency
            aec.total_input_chars AS est_input_chars,
            aec.total_output_chars AS est_output_chars,
            aec.estimated_input_tokens AS est_input_tokens,
            aec.estimated_output_tokens AS est_output_tokens,
            aec.estimated_credits AS est_credits,
            -- Estimation rates used (Table 6f)
            aec.input_rate_used AS est_input_rate,
            aec.output_rate_used AS est_output_rate,
            aec.model_assumed AS est_model,
            -- Accurate input/output breakdown (when available)
            atd.analyst_input_tokens AS accurate_input_tokens,
            atd.analyst_output_tokens AS accurate_output_tokens,
            acd.analyst_input_credits AS accurate_input_credits,
            acd.analyst_output_credits AS accurate_output_credits
        FROM analyst_estimated_credits aec
        CROSS JOIN thread_timing tt
        LEFT JOIN analyst_token_details atd ON 1=1
        LEFT JOIN analyst_credit_details acd ON 1=1
    ),
    -- Get warehouse compute credits from QUERY_HISTORY
    sql_query_ids AS (
        SELECT DISTINCT query_id
        FROM trace_spans
        WHERE query_id IS NOT NULL
    ),
    warehouse_compute AS (
        SELECT 
            COALESCE(SUM(
                CASE q.WAREHOUSE_SIZE
                    WHEN 'X-Small' THEN 1
                    WHEN 'Small' THEN 2
                    WHEN 'Medium' THEN 4
                    WHEN 'Large' THEN 8
                    WHEN 'X-Large' THEN 16
                    WHEN '2X-Large' THEN 32
                    WHEN '3X-Large' THEN 64
                    WHEN '4X-Large' THEN 128
                    ELSE 1
                END * (q.EXECUTION_TIME / 3600000.0)
            ), 0) AS compute_credits,
            COALESCE(SUM(q.CREDITS_USED_CLOUD_SERVICES), 0) AS cloud_services_credits,
            COUNT(DISTINCT q.QUERY_ID) AS query_count
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
        WHERE q.QUERY_ID IN (SELECT query_id FROM sql_query_ids)
    ),
    category_summary AS (
        SELECT 
            cost_category,
            COUNT(*) AS call_count,
            ROUND(SUM(duration_ms) / 1000.0, 2) AS total_duration_sec,
            SUM(COALESCE(total_tokens, 0)) AS category_tokens
        FROM trace_spans
        WHERE cost_category IN ('LLM_REASONING', 'CORTEX_ANALYST')  -- Only token-based categories
        GROUP BY cost_category
    ),
    -- Aggregate SQL duration for warehouse compute row
    sql_duration AS (
        SELECT 
            ROUND(SUM(duration_ms) / 1000.0, 2) AS total_sql_duration_sec,
            COUNT(*) AS sql_call_count
        FROM trace_spans
        WHERE cost_category IN ('SQL_EXECUTION', 'ANALYST_SQL')
    )
    -- Token-based categories (LLM_REASONING, CORTEX_ANALYST)
    SELECT 
        cs.cost_category AS CATEGORY,
        cs.call_count AS CALLS,
        cs.total_duration_sec AS DURATION_SEC,
        CASE cs.cost_category
            WHEN 'LLM_REASONING' THEN lts.total_tokens
            WHEN 'CORTEX_ANALYST' THEN ans.analyst_tokens
        END AS TOKENS,
        CASE cs.cost_category
            WHEN 'LLM_REASONING' THEN lts.llm_credits
            WHEN 'CORTEX_ANALYST' THEN ROUND(ans.analyst_credits, 6)
        END AS CREDITS,
        CASE cs.cost_category
            WHEN 'LLM_REASONING' THEN lts.models_used
            WHEN 'CORTEX_ANALYST' THEN ans.data_source
        END AS MODELS_USED,
        -- Calculation details for transparency
        lts.total_input_tokens AS LLM_INPUT_TOKENS,
        lts.total_output_tokens AS LLM_OUTPUT_TOKENS,
        lts.input_rate_used AS LLM_INPUT_RATE,
        lts.output_rate_used AS LLM_OUTPUT_RATE,
        lts.input_credits AS LLM_INPUT_CREDITS,
        lts.output_credits AS LLM_OUTPUT_CREDITS,
        -- Analyst estimation details
        ans.analyst_calls AS ANALYST_CALLS,
        ans.hours_until_accurate AS ANALYST_HOURS_UNTIL_ACCURATE,
        ans.est_input_chars AS ANALYST_EST_INPUT_CHARS,
        ans.est_output_chars AS ANALYST_EST_OUTPUT_CHARS,
        ans.est_input_tokens AS ANALYST_EST_INPUT_TOKENS,
        ans.est_output_tokens AS ANALYST_EST_OUTPUT_TOKENS,
        ans.est_credits AS ANALYST_EST_CREDITS,
        -- Analyst estimation rates (Table 6f)
        ans.est_input_rate AS ANALYST_EST_INPUT_RATE,
        ans.est_output_rate AS ANALYST_EST_OUTPUT_RATE,
        ans.est_model AS ANALYST_EST_MODEL,
        -- Analyst accurate details (when available from ACCOUNT_USAGE)
        ans.accurate_input_tokens AS ANALYST_ACTUAL_INPUT_TOKENS,
        ans.accurate_output_tokens AS ANALYST_ACTUAL_OUTPUT_TOKENS,
        ans.accurate_input_credits AS ANALYST_ACTUAL_INPUT_CREDITS,
        ans.accurate_output_credits AS ANALYST_ACTUAL_OUTPUT_CREDITS,
        -- Warehouse details
        wc.compute_credits AS WH_COMPUTE_CREDITS,
        wc.cloud_services_credits AS WH_CLOUD_CREDITS,
        wc.query_count AS WH_QUERY_COUNT
    FROM category_summary cs
    CROSS JOIN llm_token_summary lts
    CROSS JOIN analyst_summary ans
    CROSS JOIN warehouse_compute wc
    
    UNION ALL
    
    -- Warehouse compute row (consolidated from all SQL queries)
    SELECT 
        'WAREHOUSE_COMPUTE' AS CATEGORY,
        wc.query_count AS CALLS,
        sd.total_sql_duration_sec AS DURATION_SEC,
        NULL AS TOKENS,
        ROUND(wc.compute_credits + wc.cloud_services_credits, 6) AS CREDITS,
        'All SQL queries in thread' AS MODELS_USED,
        -- LLM fields (not applicable)
        NULL AS LLM_INPUT_TOKENS,
        NULL AS LLM_OUTPUT_TOKENS,
        NULL AS LLM_INPUT_RATE,
        NULL AS LLM_OUTPUT_RATE,
        NULL AS LLM_INPUT_CREDITS,
        NULL AS LLM_OUTPUT_CREDITS,
        -- Analyst fields (not applicable)
        NULL AS ANALYST_CALLS,
        NULL AS ANALYST_HOURS_UNTIL_ACCURATE,
        NULL AS ANALYST_EST_INPUT_CHARS,
        NULL AS ANALYST_EST_OUTPUT_CHARS,
        NULL AS ANALYST_EST_INPUT_TOKENS,
        NULL AS ANALYST_EST_OUTPUT_TOKENS,
        NULL AS ANALYST_EST_CREDITS,
        -- Analyst estimation rates (not applicable)
        NULL AS ANALYST_EST_INPUT_RATE,
        NULL AS ANALYST_EST_OUTPUT_RATE,
        NULL AS ANALYST_EST_MODEL,
        -- Analyst accurate fields (not applicable)
        NULL AS ANALYST_ACTUAL_INPUT_TOKENS,
        NULL AS ANALYST_ACTUAL_OUTPUT_TOKENS,
        NULL AS ANALYST_ACTUAL_INPUT_CREDITS,
        NULL AS ANALYST_ACTUAL_OUTPUT_CREDITS,
        -- Warehouse details
        wc.compute_credits AS WH_COMPUTE_CREDITS,
        wc.cloud_services_credits AS WH_CLOUD_CREDITS,
        wc.query_count AS WH_QUERY_COUNT
    FROM warehouse_compute wc
    CROSS JOIN sql_duration sd
    WHERE wc.compute_credits > 0 OR wc.cloud_services_credits > 0  -- Only show if there are SQL queries
    
    ORDER BY CREDITS DESC NULLS LAST
    """.format(agent_db=agent_db, agent_schema=agent_schema, agent_name=agent_name, thread_id=thread_id)
    return run_query(query)


@st.cache_data(ttl=timedelta(minutes=5))
def get_thread_time_range(agent_db: str, agent_schema: str, agent_name: str, thread_id: int):
    """Get time range info for a thread."""
    query = f"""
    SELECT 
        MIN(START_TIMESTAMP) AS THREAD_START,
        MAX(TIMESTAMP) AS THREAD_END,
        TIMESTAMPDIFF('second', MIN(START_TIMESTAMP), MAX(TIMESTAMP)) AS TOTAL_DURATION_SEC,
        MAX(RESOURCE_ATTRIBUTES:"snow.user.name"::STRING) AS USER_NAME
    FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(
        '{agent_db}', '{agent_schema}', '{agent_name}', 'CORTEX AGENT'
    ))
    WHERE RECORD_ATTRIBUTES:"snow.ai.observability.agent.thread_id"::NUMBER = {thread_id}
    """
    return run_query(query)


# =============================================================================
# RENDERING FUNCTIONS
# =============================================================================

def render_agent_timeline(spans_df):
    """Render a visual timeline of agent execution steps."""
    st.subheader("Execution Timeline")
    
    if spans_df.empty:
        st.info("No trace spans found.")
        return
    
    # Filter out orchestration for cleaner view
    display_df = spans_df[spans_df["COST_CATEGORY"] != "ORCHESTRATION"].copy()
    
    if display_df.empty:
        st.info("No execution spans to display.")
        return
    
    # Get the overall time range
    min_time = display_df["START_TIMESTAMP"].min()
    max_time = display_df["END_TIMESTAMP"].max()
    
    if pd.isna(min_time) or pd.isna(max_time):
        st.warning("Cannot determine time range.")
        return
    
    total_duration_ms = (max_time - min_time).total_seconds() * 1000
    if total_duration_ms <= 0:
        total_duration_ms = 1
    
    category_icons = {
        "ORCHESTRATION": "🎯",
        "LLM_REASONING": "🧠",
        "CORTEX_ANALYST": "📊",
        "ANALYST_SQL": "💾",
        "TOOL_CALL": "🔧",
        "SQL_EXECUTION": "⚡",
        "OTHER": "❓"
    }
    
    # Display each span as a row
    for idx, row in display_df.iterrows():
        span_name = str(row["SPAN_NAME"]) if row["SPAN_NAME"] else "Unknown"
        duration_ms = int(row["DURATION_MS"]) if row["DURATION_MS"] and not pd.isna(row["DURATION_MS"]) else 0
        category = str(row["COST_CATEGORY"]) if row["COST_CATEGORY"] else "OTHER"
        step_order = int(row["STEP_ORDER"]) if row["STEP_ORDER"] and not pd.isna(row["STEP_ORDER"]) else idx + 1
        status_code = str(row["STATUS_CODE"]) if row["STATUS_CODE"] else ""
        
        # Create visual representation
        icon = category_icons.get(category, "❓")
        duration_str = f"{duration_ms/1000:.2f}s" if duration_ms >= 1000 else f"{duration_ms}ms"
        
        # Status indicator
        status_icon = "✅" if "OK" in status_code else ("❌" if status_code else "")
        
        # Truncate span name for display
        display_name = span_name[:50] + "..." if len(span_name) > 50 else span_name
        
        with st.expander(f"{icon} {status_icon} Step {step_order}: {display_name} ({duration_str})", expanded=False):
            col1, col2 = st.columns([1, 2])
            with col1:
                st.markdown(f"**Category:** {category}")
                st.markdown(f"**Duration:** {duration_str}")
                st.markdown(f"**Status:** {status_code}")
            with col2:
                st.markdown(f"**Full Name:** `{span_name}`")
                st.markdown(f"**Start:** {row['START_TIMESTAMP']}")
            
            # Show Cortex Analyst details if available
            if category == "CORTEX_ANALYST" and row.get("ANALYST_SQL_QUERY"):
                render_cortex_analyst_details(row)
            
            # Show SQL execution details if available
            if category in ["ANALYST_SQL", "SQL_EXECUTION"] and row.get("SQL_QUERY"):
                render_sql_execution_details(row)


def render_cortex_analyst_details(row):
    """Render detailed Cortex Analyst information."""
    st.markdown("---")
    st.markdown("**Cortex Analyst Details:**")
    
    # Status
    analyst_status = row.get("ANALYST_STATUS") or "Unknown"
    status_code = row.get("ANALYST_STATUS_CODE") or ""
    status_color = "green" if analyst_status == "SUCCESS" else "red"
    st.markdown(f"**Status:** :{status_color}[{analyst_status}] (Code: {status_code})")
    
    # Semantic Model
    if row.get("SEMANTIC_MODEL"):
        st.markdown(f"**Semantic Model:** `{row['SEMANTIC_MODEL']}`")
    
    # Question Category
    if row.get("QUESTION_CATEGORY"):
        st.markdown(f"**Question Category:** {row['QUESTION_CATEGORY']}")
    
    # User Question (from messages)
    if row.get("ANALYST_MESSAGES"):
        try:
            messages = json.loads(row["ANALYST_MESSAGES"])
            for msg in messages:
                if isinstance(msg, str):
                    msg_obj = json.loads(msg)
                    if msg_obj.get("role") == "user":
                        for content in msg_obj.get("content", []):
                            if content.get("type") == "text":
                                st.markdown("**User Question:**")
                                st.info(content.get("text", ""))
        except:
            pass
    
    # Analyst Response
    if row.get("ANALYST_RESPONSE_TEXT"):
        st.markdown("**Analyst Response:**")
        st.success(row["ANALYST_RESPONSE_TEXT"])
    
    # Generated SQL
    if row.get("ANALYST_SQL_QUERY"):
        st.markdown("**Generated SQL:**")
        st.code(row["ANALYST_SQL_QUERY"], language="sql")


def render_sql_execution_details(row):
    """Render SQL execution details."""
    st.markdown("---")
    st.markdown("**SQL Execution Details:**")
    
    # Query ID for linking to Query History
    if row.get("SQL_QUERY_ID"):
        st.markdown(f"**Query ID:** `{row['SQL_QUERY_ID']}`")
    
    # Duration
    sql_duration = row.get("SQL_DURATION_MS")
    if sql_duration and not pd.isna(sql_duration):
        st.markdown(f"**SQL Duration:** {int(sql_duration)}ms")
    
    # The SQL Query
    if row.get("SQL_QUERY"):
        st.markdown("**Executed SQL:**")
        st.code(row["SQL_QUERY"], language="sql")
    
    # Result preview (truncated)
    if row.get("SQL_RESULT"):
        try:
            result = json.loads(row["SQL_RESULT"])
            data = result.get("data", [])
            if data:
                st.markdown(f"**Result:** {len(data)} row(s) returned")
                # Show first few rows
                if len(data) > 5:
                    st.caption("(Showing first 5 rows)")
                    data = data[:5]
                st.json(data)
        except:
            pass


def render_agent_waterfall(spans_df, total_duration_sec):
    """Render a waterfall diagram of agent execution."""
    st.subheader("Execution Waterfall")
    
    if spans_df.empty or total_duration_sec <= 0:
        st.info("No data for waterfall.")
        return
    
    filtered_df = spans_df.copy()
    
    max_bar_width = 50
    
    # Display waterfall
    st.markdown("**Time Distribution by Step:**")
    for _, row in filtered_df.iterrows():
        duration_ms = int(row["DURATION_MS"]) if row["DURATION_MS"] and not pd.isna(row["DURATION_MS"]) else 0
        duration_sec = duration_ms / 1000.0
        category = str(row["COST_CATEGORY"]) if row["COST_CATEGORY"] else "OTHER"
        span_name = str(row["SPAN_NAME"]) if row["SPAN_NAME"] else "Unknown"
        step = int(row["STEP_ORDER"]) if row["STEP_ORDER"] and not pd.isna(row["STEP_ORDER"]) else 0
        
        # Calculate bar width
        pct = (duration_sec / total_duration_sec) * 100 if total_duration_sec > 0 else 0
        bar_width = max(1, int((pct / 100) * max_bar_width))
        bar = "█" * bar_width
        
        # Truncate span name
        short_name = span_name[:30] + "..." if len(span_name) > 30 else span_name
        
        col1, col2, col3 = st.columns([1, 3, 2])
        with col1:
            st.markdown(f"**{step}. {category[:12]}**")
        with col2:
            st.code(f"{bar} {pct:.1f}%")
        with col3:
            st.caption(f"{duration_sec:.2f}s | {short_name}")


def render_agent_cost_breakdown(cost_df):
    """Render cost breakdown by category with calculation transparency."""
    st.subheader("Cost by Category")
    
    if cost_df.empty:
        st.info("No cost data available.")
        return
    
    # Pricing methodology info box
    with st.expander("ℹ️ **Pricing Methodology** - How costs are calculated", expanded=False):
        st.markdown("""
        **Data Sources:** [Snowflake Credit Consumption Table](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)
        
        | Category | Pricing Table | Method |
        |----------|--------------|--------|
        | **LLM Reasoning** | Table 6(e) | `(input_tokens × input_rate + output_tokens × output_rate) / 1,000,000` |
        | **Cortex Analyst** | Table 6(f) | Token-based: accurate from usage history, or estimated from content length |
        | **Warehouse Compute** | Standard compute | `warehouse_size_multiplier × (execution_time_ms / 3,600,000) + cloud_services` |
        
        **Token Estimation:** When accurate data isn't yet available (2-3 hour latency), we estimate tokens from 
        message content using `characters ÷ 4` (standard approximation for English text).
        
        **Warehouse Multipliers:** X-Small=1, Small=2, Medium=4, Large=8, X-Large=16, 2XL=32, 3XL=64, 4XL=128
        """)
    
    # Pricing reference tables
    with st.expander("📊 **Pricing Reference Tables** - Validate calculations", expanded=False):
        st.markdown("### Table 6(e): LLM Reasoning / Cortex Agents Orchestration")
        st.markdown("*Credits per million tokens*")
        
        llm_pricing_data = {
            "Model": ["claude-3-5-sonnet", "claude-3-7-sonnet", "claude-4-opus", "claude-4-sonnet",
                     "claude-haiku-4-5", "claude-opus-4-5", "claude-opus-4-6", "claude-sonnet-4-5",
                     "claude-sonnet-4-6", "gemini-2-5-flash", "gemini-3-pro", "llama-4-maverick",
                     "llama3.1-405b", "llama3.1-70b", "llama3.1-8b", "llama3.2-1b", "llama3.2-3b",
                     "llama3.3-70b", "llama4-scout", "mistral-large", "mistral-large2",
                     "openai-gpt-4.1", "openai-gpt-5", "openai-gpt-5-mini", "openai-gpt-5.1",
                     "openai-gpt-5.2", "reka-core", "reka-flash"],
            "Input Rate": [1.88, 1.88, 9.41, 1.88, 0.69, 3.45, 3.45, 2.07, 2.07, 0.19, 1.26,
                          0.17, 1.26, 0.60, 0.17, 0.02, 0.03, 0.60, 0.17, 1.26, 0.60,
                          1.38, 0.86, 0.17, 0.86, 1.21, 1.88, 0.21],
            "Output Rate": [9.41, 9.41, 47.07, 9.41, 3.45, 17.26, 17.26, 10.36, 10.36, 1.57, 7.53,
                           1.38, 5.02, 0.60, 0.17, 0.02, 0.03, 0.60, 1.38, 3.77, 1.76,
                           5.52, 6.90, 1.38, 6.90, 9.67, 9.41, 0.62]
        }
        st.dataframe(pd.DataFrame(llm_pricing_data), use_container_width=True)
        
        st.markdown("---")
        st.markdown("### Table 6(f): Cortex Analyst via Agents")
        st.markdown("*Credits per million tokens - **Different rates from Table 6(e)!***")
        
        analyst_pricing_data = {
            "Model": ["claude-3-5-sonnet", "claude-3-7-sonnet", "claude-4-opus", "claude-4-sonnet",
                     "claude-haiku-4-5", "claude-opus-4-5", "claude-opus-4-6", "claude-sonnet-4-5",
                     "claude-sonnet-4-6", "gemini-2-5-flash", "gemini-3-pro", "llama-4-maverick",
                     "llama3.1-405b", "llama3.1-70b", "llama3.3-70b", "mistral-large2", "reka-flash"],
            "Input Rate": [3.14, 3.14, 15.69, 3.14, 1.15, 5.75, 5.75, 3.46, 3.46, 0.31, 2.10,
                          0.29, 2.10, 1.00, 1.00, 1.00, 0.35],
            "Output Rate": [15.69, 15.69, 78.45, 15.69, 5.75, 28.77, 28.77, 17.28, 17.28, 2.62, 12.55,
                           2.30, 8.37, 1.00, 1.00, 2.94, 1.04]
        }
        st.dataframe(pd.DataFrame(analyst_pricing_data), use_container_width=True)
        
        st.info("""
        **Important:** Table 6(f) rates for Cortex Analyst are **~1.67x higher** than Table 6(e) LLM Reasoning rates.
        
        For example, `claude-4-sonnet`:
        - LLM Reasoning (6e): Input 1.88, Output 9.41
        - Cortex Analyst (6f): Input 3.14, Output 15.69
        
        *Rates validated from actual `SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY` billing data.*
        """)
    
    # Summary metrics
    total_duration = cost_df["DURATION_SEC"].sum() if "DURATION_SEC" in cost_df.columns else 0
    total_calls = cost_df["CALLS"].sum() if "CALLS" in cost_df.columns else 0
    
    # Get credits (may be NULL for some categories)
    total_credits = 0
    for _, row in cost_df.iterrows():
        if row["CREDITS"] and not pd.isna(row["CREDITS"]):
            total_credits += float(row["CREDITS"])
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Duration", f"{total_duration:.1f}s")
    with col2:
        st.metric("Total Calls", int(total_calls))
    with col3:
        st.metric("Est. Credits", format_credits(total_credits) if total_credits > 0 else "N/A")
    
    st.markdown("---")
    
    # Category breakdown with calculation details
    st.markdown("**Breakdown by Category:**")
    
    # Get calculation details from first row (they're the same across all rows)
    first_row = cost_df.iloc[0] if len(cost_df) > 0 else None
    
    for _, row in cost_df.iterrows():
        category = str(row["CATEGORY"]) if row["CATEGORY"] else "Unknown"
        calls = int(row["CALLS"]) if row["CALLS"] and not pd.isna(row["CALLS"]) else 0
        duration = float(row["DURATION_SEC"]) if row["DURATION_SEC"] and not pd.isna(row["DURATION_SEC"]) else 0
        tokens = row["TOKENS"] if "TOKENS" in row and row["TOKENS"] and not pd.isna(row["TOKENS"]) else None
        credits = row["CREDITS"] if "CREDITS" in row and row["CREDITS"] and not pd.isna(row["CREDITS"]) else None
        models_used = row["MODELS_USED"] if "MODELS_USED" in row and row["MODELS_USED"] else None
        
        with st.expander(f"**{category}** - {calls} calls, {duration:.2f}s", expanded=True):
            # Summary metrics row
            mcol1, mcol2, mcol3, mcol4 = st.columns(4)
            with mcol1:
                st.metric("Calls", calls)
            with mcol2:
                st.metric("Duration", f"{duration:.2f}s")
            with mcol3:
                st.metric("Tokens", format_tokens(tokens) if tokens else "N/A")
            with mcol4:
                st.metric("Credits", format_credits(credits) if credits else "N/A")
            
            # Calculation details based on category
            if category == "LLM_REASONING" and first_row is not None:
                st.markdown("---")
                st.markdown("**📊 Calculation Details** *(Table 6e - Cortex Agents)*")
                
                # Get LLM details
                input_tokens = first_row.get("LLM_INPUT_TOKENS", 0) or 0
                output_tokens = first_row.get("LLM_OUTPUT_TOKENS", 0) or 0
                input_rate = first_row.get("LLM_INPUT_RATE", 1.88) or 1.88
                output_rate = first_row.get("LLM_OUTPUT_RATE", 9.41) or 9.41
                input_credits = first_row.get("LLM_INPUT_CREDITS", 0) or 0
                output_credits = first_row.get("LLM_OUTPUT_CREDITS", 0) or 0
                
                # Display model info
                if models_used:
                    st.markdown(f"**Model:** `{models_used}`")
                
                # Calculation breakdown
                calc_col1, calc_col2 = st.columns(2)
                with calc_col1:
                    st.markdown("**Input Tokens:**")
                    st.code(f"{int(input_tokens):,} tokens × {input_rate:.2f} credits/1M\n= {float(input_credits):.6f} credits", language=None)
                with calc_col2:
                    st.markdown("**Output Tokens:**")
                    st.code(f"{int(output_tokens):,} tokens × {output_rate:.2f} credits/1M\n= {float(output_credits):.6f} credits", language=None)
                
                # Total
                total_llm = float(input_credits) + float(output_credits)
                st.markdown(f"**Total:** `{float(input_credits):.6f} + {float(output_credits):.6f} = {total_llm:.6f} credits`")
            
            elif category == "CORTEX_ANALYST" and first_row is not None:
                st.markdown("---")
                
                # Check if accurate or estimated
                data_source = models_used  # This field contains the data source info for analyst
                is_accurate = data_source and "accurate" in str(data_source).lower()
                
                analyst_calls = row.get("ANALYST_CALLS", 0) or 0
                hours_until_accurate = row.get("ANALYST_HOURS_UNTIL_ACCURATE", 0) or 0
                
                if is_accurate:
                    st.markdown("**📊 Calculation Details** *(Table 6f - Cortex Analyst via Agents)*")
                    st.success("✅ **Accurate Data** - Retrieved from `SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY`")
                    
                    # Get accurate input/output breakdown
                    actual_input_tokens = row.get("ANALYST_ACTUAL_INPUT_TOKENS", 0) or 0
                    actual_output_tokens = row.get("ANALYST_ACTUAL_OUTPUT_TOKENS", 0) or 0
                    actual_input_credits = row.get("ANALYST_ACTUAL_INPUT_CREDITS", 0) or 0
                    actual_output_credits = row.get("ANALYST_ACTUAL_OUTPUT_CREDITS", 0) or 0
                    
                    # Calculate actual rates from the data
                    actual_input_rate = (float(actual_input_credits) / float(actual_input_tokens) * 1000000) if actual_input_tokens > 0 else 3.14
                    actual_output_rate = (float(actual_output_credits) / float(actual_output_tokens) * 1000000) if actual_output_tokens > 0 else 15.69
                    
                    # Show breakdown in two columns like estimation view
                    st.markdown("**Token & Credit Breakdown:**")
                    
                    acc_col1, acc_col2 = st.columns(2)
                    with acc_col1:
                        st.markdown("**Input Tokens:**")
                        st.code(f"{int(actual_input_tokens):,} tokens\n× {actual_input_rate:.2f} credits/1M\n= {float(actual_input_credits):.6f} credits", language=None)
                    with acc_col2:
                        st.markdown("**Output Tokens:**")
                        st.code(f"{int(actual_output_tokens):,} tokens\n× {actual_output_rate:.2f} credits/1M\n= {float(actual_output_credits):.6f} credits", language=None)
                    
                    # Total
                    total_tokens = int(actual_input_tokens) + int(actual_output_tokens)
                    total_credits = float(actual_input_credits) + float(actual_output_credits)
                    st.markdown(f"""
                    **Total:** `{total_tokens:,} tokens` → `{total_credits:.6f} credits`
                    
                    *Rates: Table 6(f) claude-4-sonnet - Input: {actual_input_rate:.2f}/1M, Output: {actual_output_rate:.2f}/1M*
                    """)
                else:
                    # Show estimation with full transparency
                    st.markdown("**📊 Calculation Details** *(Table 6f - Estimated from Content)*")
                    
                    # Warning box explaining WHY we're estimating
                    st.warning(f"""
                    ⏳ **Why is this an estimate?**
                    
                    Accurate token data for Cortex Analyst is only available in `SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY` 
                    after a 2-3 hour latency period. This thread is too recent for accurate data.
                    
                    **⏱️ Accurate data expected in:** ~{int(hours_until_accurate)} hour(s)
                    """)
                    
                    # Get estimation details
                    est_input_chars = row.get("ANALYST_EST_INPUT_CHARS", 0) or 0
                    est_output_chars = row.get("ANALYST_EST_OUTPUT_CHARS", 0) or 0
                    est_input_tokens = row.get("ANALYST_EST_INPUT_TOKENS", 0) or 0
                    est_output_tokens = row.get("ANALYST_EST_OUTPUT_TOKENS", 0) or 0
                    est_credits = row.get("ANALYST_EST_CREDITS", 0) or 0
                    
                    st.markdown("**Estimation Method:**")
                    st.info("""
                    📏 **Content-based token estimation**
                    
                    We estimate tokens by analyzing the actual message content from the observability API:
                    - Input = questions/messages sent to Cortex Analyst
                    - Output = response text + thinking/reasoning
                    - Token estimate = characters ÷ 4 (standard approximation)
                    - **Rates assume `claude-4-sonnet`** (most common Cortex Analyst model)
                    
                    If a different model is used, actual costs may vary. See Pricing Reference Tables for all model rates.
                    """)
                    
                    # Get estimation rates from the data
                    est_input_rate = row.get("ANALYST_EST_INPUT_RATE", 3.14) or 3.14
                    est_output_rate = row.get("ANALYST_EST_OUTPUT_RATE", 15.69) or 15.69
                    est_model = row.get("ANALYST_EST_MODEL", "claude-4-sonnet") or "claude-4-sonnet"
                    
                    # Show the calculation breakdown
                    st.markdown("**Calculation Breakdown:**")
                    
                    calc_col1, calc_col2 = st.columns(2)
                    with calc_col1:
                        st.markdown("**Input (Questions):**")
                        st.code(f"{int(est_input_chars):,} chars ÷ 4\n≈ {int(est_input_tokens):,} tokens\n× {est_input_rate:.2f} credits/1M tokens\n= {float(est_input_tokens) * est_input_rate / 1000000:.6f} credits", language=None)
                    with calc_col2:
                        st.markdown("**Output (Responses):**")
                        st.code(f"{int(est_output_chars):,} chars ÷ 4\n≈ {int(est_output_tokens):,} tokens\n× {est_output_rate:.2f} credits/1M tokens\n= {float(est_output_tokens) * est_output_rate / 1000000:.6f} credits", language=None)
                    
                    total_est_tokens = int(est_input_tokens) + int(est_output_tokens)
                    st.markdown(f"""
                    **Estimated Total:** `~{total_est_tokens:,} tokens` → `~{float(est_credits):.6f} credits`
                    
                    *Rates used: Table 6(f) {est_model} - Input: {est_input_rate:.2f}/1M, Output: {est_output_rate:.2f}/1M*
                    """)
                    
                    # Important note about estimation accuracy
                    st.warning("""
                    ⏰ **Note: Estimate may be lower than actual**
                    
                    This estimate is based only on visible message content. Cortex Analyst internally uses additional 
                    context (semantic model definitions, schemas, examples) that isn't captured in the observability API.
                    
                    **Actual token counts are typically 5-15x higher** than this estimate. For accurate cost data, 
                    please check back after the usage history is updated (~2-3 hours after thread execution).
                    """)
            
            elif category == "WAREHOUSE_COMPUTE" and first_row is not None:
                st.markdown("---")
                st.markdown("**📊 Calculation Details** *(Warehouse Compute)*")
                
                wh_compute = row.get("WH_COMPUTE_CREDITS", 0) or 0
                wh_cloud = row.get("WH_CLOUD_CREDITS", 0) or 0
                query_count = row.get("WH_QUERY_COUNT", 0) or 0
                
                st.markdown(f"**Queries Executed:** `{int(query_count)}` SQL queries initiated by this agent thread")
                st.markdown("")
                
                calc_col1, calc_col2 = st.columns(2)
                with calc_col1:
                    st.markdown("**Compute Credits:**")
                    st.code(f"warehouse_size × (exec_time / 3,600,000)\n= {float(wh_compute):.6f} credits", language=None)
                with calc_col2:
                    st.markdown("**Cloud Services:**")
                    st.code(f"{float(wh_cloud):.6f} credits", language=None)
                
                total_wh = float(wh_compute) + float(wh_cloud)
                st.markdown(f"**Total:** `{float(wh_compute):.6f} + {float(wh_cloud):.6f} = {total_wh:.6f} credits`")
                st.caption("Warehouse multipliers: X-Small=1, Small=2, Medium=4, Large=8, X-Large=16, 2XL=32, 3XL=64, 4XL=128")


def render_cortex_analyst_summary(spans_df):
    """Render a summary of all Cortex Analyst calls with SQL details."""
    st.subheader("Cortex Analyst Requests")
    
    # Filter to Cortex Analyst spans
    analyst_df = spans_df[spans_df["COST_CATEGORY"] == "CORTEX_ANALYST"].copy()
    
    if analyst_df.empty:
        st.info("No Cortex Analyst requests in this thread.")
        return
    
    for idx, row in analyst_df.iterrows():
        step = int(row["STEP_ORDER"]) if row["STEP_ORDER"] and not pd.isna(row["STEP_ORDER"]) else idx + 1
        span_name = str(row["SPAN_NAME"]) if row["SPAN_NAME"] else "Unknown"
        duration_ms = int(row["DURATION_MS"]) if row["DURATION_MS"] and not pd.isna(row["DURATION_MS"]) else 0
        
        # Status
        analyst_status = row.get("ANALYST_STATUS") or "Unknown"
        status_icon = "✅" if analyst_status == "SUCCESS" else "❌"
        
        # Tool name from span
        tool_name = span_name.replace("CortexAnalystTool_", "") if "CortexAnalystTool_" in span_name else span_name
        
        with st.expander(f"{status_icon} **{tool_name}** (Step {step}, {duration_ms}ms)", expanded=True):
            # Metadata row
            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown(f"**Status:** {analyst_status}")
                st.markdown(f"**Status Code:** {row.get('ANALYST_STATUS_CODE', 'N/A')}")
            with col2:
                st.markdown(f"**Question Type:** {row.get('QUESTION_CATEGORY', 'N/A')}")
            with col3:
                if row.get("SEMANTIC_MODEL"):
                    st.markdown(f"**Semantic Model:**")
                    st.code(row["SEMANTIC_MODEL"], language=None)
            
            # User Question
            if row.get("ANALYST_MESSAGES"):
                try:
                    messages = json.loads(row["ANALYST_MESSAGES"])
                    for msg in messages:
                        if isinstance(msg, str):
                            msg_obj = json.loads(msg)
                            if msg_obj.get("role") == "user":
                                for content in msg_obj.get("content", []):
                                    if content.get("type") == "text":
                                        st.markdown("**User Question:**")
                                        st.info(content.get("text", ""))
                except:
                    pass
            
            # Analyst Response
            if row.get("ANALYST_RESPONSE_TEXT"):
                st.markdown("**Analyst Response:**")
                st.success(row["ANALYST_RESPONSE_TEXT"])
            
            # Generated SQL
            if row.get("ANALYST_SQL_QUERY"):
                st.markdown("**Generated SQL:**")
                st.code(row["ANALYST_SQL_QUERY"], language="sql")
            
            # Thinking (optional) - using checkbox instead of nested expander
            if row.get("ANALYST_THINKING"):
                st.markdown("**Analyst Thinking:**")
                st.caption(row["ANALYST_THINKING"][:500] + "..." if len(row["ANALYST_THINKING"]) > 500 else row["ANALYST_THINKING"])


def render_sql_executions_summary(spans_df):
    """Render a summary of all SQL executions."""
    st.subheader("SQL Executions")
    
    # Filter to SQL execution spans
    sql_df = spans_df[spans_df["COST_CATEGORY"].isin(["ANALYST_SQL", "SQL_EXECUTION"])].copy()
    
    if sql_df.empty:
        st.info("No SQL executions in this thread.")
        return
    
    for idx, row in sql_df.iterrows():
        step = int(row["STEP_ORDER"]) if row["STEP_ORDER"] and not pd.isna(row["STEP_ORDER"]) else idx + 1
        span_name = str(row["SPAN_NAME"]) if row["SPAN_NAME"] else "Unknown"
        category = str(row["COST_CATEGORY"]) if row["COST_CATEGORY"] else "OTHER"
        
        # Duration
        sql_duration = row.get("SQL_DURATION_MS")
        duration_str = f"{int(sql_duration)}ms" if sql_duration and not pd.isna(sql_duration) else "N/A"
        
        # Status
        status_code = str(row["STATUS_CODE"]) if row["STATUS_CODE"] else ""
        status_icon = "✅" if "OK" in status_code else ("❌" if status_code else "⚠️")
        
        with st.expander(f"{status_icon} **{span_name}** (Step {step}, {duration_str})", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**Category:** {category}")
                st.markdown(f"**Status:** {status_code}")
            with col2:
                if row.get("SQL_QUERY_ID"):
                    st.markdown(f"**Query ID:**")
                    st.code(row["SQL_QUERY_ID"], language=None)
            
            # The SQL Query
            if row.get("SQL_QUERY"):
                st.markdown("**Executed SQL:**")
                st.code(row["SQL_QUERY"], language="sql")
            
            # Result preview
            if row.get("SQL_RESULT"):
                try:
                    result = json.loads(row["SQL_RESULT"])
                    data = result.get("data", [])
                    if data:
                        st.markdown(f"**Result:** {len(data)} row(s)")
                        # Show first few rows directly (no nested expander)
                        st.json(data[:5] if len(data) > 5 else data)
                        if len(data) > 5:
                            st.caption(f"... and {len(data) - 5} more rows")
                except:
                    pass


def render_custom_tools_summary(spans_df):
    """Render a summary of all custom tool calls with inputs and outputs."""
    st.subheader("Custom Tool Calls")
    
    # Filter to tool call spans
    tool_df = spans_df[spans_df["COST_CATEGORY"] == "TOOL_CALL"].copy()
    
    if tool_df.empty:
        st.info("No custom tool calls in this thread.")
        return
    
    for idx, row in tool_df.iterrows():
        step = int(row["STEP_ORDER"]) if row["STEP_ORDER"] and not pd.isna(row["STEP_ORDER"]) else idx + 1
        span_name = str(row["SPAN_NAME"]) if row["SPAN_NAME"] else "Unknown"
        
        # Get tool name (from custom_tool_name or parse from span_name)
        tool_name = row.get("CUSTOM_TOOL_NAME") or span_name.replace("ToolCall-", "")
        
        # Duration
        tool_duration = row.get("CUSTOM_TOOL_DURATION_MS")
        duration_str = f"{int(tool_duration)}ms" if tool_duration and not pd.isna(tool_duration) else "N/A"
        
        # Status
        tool_status = row.get("CUSTOM_TOOL_STATUS") or "Unknown"
        status_code = row.get("CUSTOM_TOOL_STATUS_CODE") or ""
        status_icon = "✅" if tool_status == "SUCCESS" else "❌"
        
        with st.expander(f"{status_icon} **{tool_name}** (Step {step}, {duration_str})", expanded=True):
            # Status row
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**Status:** {tool_status}")
                st.markdown(f"**Status Code:** {status_code}")
            with col2:
                st.markdown(f"**Duration:** {duration_str}")
            
            # Input Parameters
            arg_names = row.get("CUSTOM_TOOL_ARG_NAMES")
            arg_values = row.get("CUSTOM_TOOL_ARG_VALUES")
            
            if arg_names and arg_values:
                st.markdown("**Input Parameters:**")
                
                # Parse the comma-separated names and values
                names_list = [n.strip() for n in str(arg_names).split(",")]
                values_list = parse_tool_arg_values(str(arg_values))
                
                # Create a nice display of parameters
                if len(names_list) == len(values_list):
                    params_data = []
                    for name, value in zip(names_list, values_list):
                        params_data.append({"Parameter": name, "Value": value})
                    st.table(params_data)
                else:
                    # Fallback: show raw
                    st.code(f"Names: {arg_names}\nValues: {arg_values}", language=None)
            
            # Output / Results
            tool_results = row.get("CUSTOM_TOOL_RESULTS")
            if tool_results:
                st.markdown("**Output:**")
                
                # Try to parse as JSON for nice display
                try:
                    # Handle escaped JSON strings
                    result_str = str(tool_results)
                    if result_str.startswith('"') and result_str.endswith('"'):
                        result_str = result_str[1:-1]
                    result_str = result_str.replace('\\"', '"').replace('\\\\', '\\')
                    
                    result_json = json.loads(result_str)
                    st.json(result_json)
                except:
                    # Show as text if not valid JSON
                    st.code(tool_results, language=None)


def parse_tool_arg_values(values_str: str) -> list:
    """Parse comma-separated tool argument values, handling quoted strings."""
    values = []
    current = ""
    in_quotes = False
    
    for char in values_str:
        if char == '"' and (not current or current[-1] != '\\'):
            in_quotes = not in_quotes
            current += char
        elif char == ',' and not in_quotes:
            values.append(current.strip().strip('"'))
            current = ""
        else:
            current += char
    
    if current:
        values.append(current.strip().strip('"'))
    
    return values


# =============================================================================
# MAIN APPLICATION
# =============================================================================

def main():
    st.title("Cortex Agent Cost Analyzer")
    st.markdown("""
    Analyze Cortex Agent execution traces with detailed observability data.
    See LLM reasoning steps, tool calls, Cortex Analyst queries, and SQL executions.
    """)
    
    # Get available agents
    with st.spinner("Loading available agents..."):
        try:
            agents_df = get_available_agents()
        except Exception as e:
            st.error(f"Error loading agents: {str(e)[:200]}")
            agents_df = pd.DataFrame()
    
    if agents_df.empty:
        st.warning("No Cortex Agents found in the last 30 days.")
        st.info("""
        **To use this analyzer:**
        1. Create and run a Cortex Agent
        2. Wait for usage data to sync (~45 min latency)
        3. Return here to analyze execution traces
        """)
        return
    
    # Agent selector
    st.subheader("Select Agent")
    
    agent_options = []
    for _, row in agents_df.iterrows():
        db = row["AGENT_DATABASE_NAME"] or "N/A"
        schema = row["AGENT_SCHEMA_NAME"] or "N/A"
        name = row["AGENT_NAME"] or "Unknown"
        credits = format_credits(row["TOTAL_CREDITS"])
        count = int(row["REQUEST_COUNT"]) if row["REQUEST_COUNT"] else 0
        agent_options.append(f"{db}.{schema}.{name} ({credits} cr, {count} requests)")
    
    selected_agent_idx = st.selectbox(
        "Choose an agent",
        range(len(agent_options)),
        format_func=lambda i: agent_options[i]
    )
    
    if selected_agent_idx is not None:
        selected_row = agents_df.iloc[selected_agent_idx]
        agent_db = selected_row["AGENT_DATABASE_NAME"]
        agent_schema = selected_row["AGENT_SCHEMA_NAME"]
        agent_name = selected_row["AGENT_NAME"]
        
        st.success(f"Selected: **{agent_db}.{agent_schema}.{agent_name}**")
        
        # Get threads for this agent
        st.subheader("Select Thread")
        
        with st.spinner("Loading agent threads..."):
            try:
                threads_df = get_agent_threads(agent_db, agent_schema, agent_name)
            except Exception as e:
                st.error(f"Error loading threads: {str(e)[:200]}")
                threads_df = pd.DataFrame()
        
        # Manual thread ID input
        manual_thread = st.text_input(
            "Enter Thread ID manually",
            placeholder="e.g., 14296154461"
        )
        
        # Thread selector dropdown
        thread_id = None
        
        if not threads_df.empty:
            thread_options = []
            for _, row in threads_df.iterrows():
                tid = int(row["THREAD_ID"]) if row["THREAD_ID"] and not pd.isna(row["THREAD_ID"]) else 0
                spans = int(row["SPAN_COUNT"]) if row["SPAN_COUNT"] and not pd.isna(row["SPAN_COUNT"]) else 0
                last_ts = row["LAST_TIMESTAMP"]
                thread_options.append(f"Thread {tid} ({spans} spans) - {last_ts}")
            
            selected_thread_idx = st.selectbox(
                "Or choose from recent threads",
                range(len(thread_options)),
                format_func=lambda i: thread_options[i]
            )
            
            if not manual_thread and selected_thread_idx is not None:
                thread_id = int(threads_df.iloc[selected_thread_idx]["THREAD_ID"])
        
        # Manual input takes precedence
        if manual_thread:
            try:
                thread_id = int(manual_thread)
            except ValueError:
                st.error("Thread ID must be a number.")
                thread_id = None
        
        if thread_id:
            render_agent_analysis(agent_db, agent_schema, agent_name, thread_id)


def render_agent_analysis(agent_db: str, agent_schema: str, agent_name: str, thread_id: int):
    """Render the detailed agent analysis for a specific thread."""
    st.markdown("---")
    st.header(f"Thread Analysis: {thread_id}")
    
    # Fetch all data
    with st.spinner("Fetching trace spans..."):
        try:
            spans_df = get_agent_trace_spans(agent_db, agent_schema, agent_name, thread_id)
            time_range_df = get_thread_time_range(agent_db, agent_schema, agent_name, thread_id)
            cost_df = get_agent_cost_summary(agent_db, agent_schema, agent_name, thread_id)
        except Exception as e:
            st.error(f"Error fetching data: {str(e)[:300]}")
            return
    
    if spans_df.empty:
        st.warning(f"No trace spans found for thread {thread_id}.")
        return
    
    # Thread overview
    st.subheader("Thread Overview")
    
    total_duration = 0
    if not time_range_df.empty:
        time_info = time_range_df.iloc[0]
        total_duration = float(time_info["TOTAL_DURATION_SEC"]) if time_info["TOTAL_DURATION_SEC"] and not pd.isna(time_info["TOTAL_DURATION_SEC"]) else 0
        user = time_info["USER_NAME"] or "Unknown"
        
        # Count categories
        analyst_count = len(spans_df[spans_df["COST_CATEGORY"] == "CORTEX_ANALYST"])
        sql_count = len(spans_df[spans_df["COST_CATEGORY"].isin(["ANALYST_SQL", "SQL_EXECUTION"])])
        tool_count = len(spans_df[spans_df["COST_CATEGORY"] == "TOOL_CALL"])
        
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        with col1:
            st.metric("Duration", f"{total_duration:.1f}s")
        with col2:
            st.metric("Total Steps", len(spans_df))
        with col3:
            st.metric("Analyst Calls", analyst_count)
        with col4:
            st.metric("Custom Tools", tool_count)
        with col5:
            st.metric("SQL Executions", sql_count)
        with col6:
            st.metric("User", user)
    
    st.markdown("---")
    
    # Tabs for different views
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "Timeline", 
        "Cortex Analyst", 
        "Custom Tools",
        "SQL Executions",
        "Cost Breakdown", 
        "Raw Data"
    ])
    
    with tab1:
        render_agent_timeline(spans_df)
        st.divider()
        render_agent_waterfall(spans_df, total_duration)
    
    with tab2:
        render_cortex_analyst_summary(spans_df)
    
    with tab3:
        render_custom_tools_summary(spans_df)
    
    with tab4:
        render_sql_executions_summary(spans_df)
    
    with tab5:
        render_agent_cost_breakdown(cost_df)
    
    with tab6:
        st.subheader("Raw Trace Spans")
        display_cols = ["STEP_ORDER", "SPAN_NAME", "COST_CATEGORY", "DURATION_MS", "STATUS_CODE", "START_TIMESTAMP"]
        available_cols = [c for c in display_cols if c in spans_df.columns]
        if available_cols:
            st.dataframe(spans_df[available_cols])
        else:
            st.dataframe(spans_df)


if __name__ == "__main__":
    main()
