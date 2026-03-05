# Cortex Agent Cost Analyzer

A Streamlit application for analyzing Snowflake Cortex Agent execution costs with detailed observability data. Track LLM reasoning steps, tool calls, Cortex Analyst queries, SQL executions, and their associated credit consumption.

## Features

- **Real-time Observability**: View detailed execution traces using `SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS`
- **Cost Breakdown by Category**: See credits consumed by LLM Reasoning, Cortex Analyst, and Warehouse Compute
- **Transparent Calculations**: Full breakdown of token counts, rates, and credit calculations
- **Accurate vs Estimated Data**: Automatically uses accurate billing data when available, falls back to content-based estimation for recent threads
- **Timeline Visualization**: Waterfall chart showing execution flow and duration
- **Cortex Analyst Details**: View semantic model queries, SQL generated, and results

## Prerequisites

- Snowflake account with Cortex Agents enabled
- `ACCOUNTADMIN` role or appropriate privileges to:
  - Query `SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS`
  - Query `SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY`
  - Query `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`
- Snowflake CLI (`snow`) installed and configured
- At least one Cortex Agent with execution history

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/llohmann/cortex-agent-cost-analyzer.git
cd cortex-agent-cost-analyzer
```

### 2. Create Snowflake Objects

Connect to your Snowflake account and run the following SQL:

```sql
-- Create database and schema
CREATE DATABASE IF NOT EXISTS AGENT_COST_ANALYSIS;
CREATE SCHEMA IF NOT EXISTS AGENT_COST_ANALYSIS.APP;

-- Create stage for Streamlit app
CREATE STAGE IF NOT EXISTS AGENT_COST_ANALYSIS.APP.STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- Create the Streamlit app
CREATE STREAMLIT IF NOT EXISTS AGENT_COST_ANALYSIS.APP.CORTEX_THREAD_COST_ANALYZER
    ROOT_LOCATION = '@AGENT_COST_ANALYSIS.APP.STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = 'COMPUTE_WH';
```

### 3. Deploy the Application

Upload the Streamlit app to the stage:

```sql
PUT file:///path/to/cortex_thread_cost_analyzer/streamlit_app.py 
    @AGENT_COST_ANALYSIS.APP.STREAMLIT_STAGE 
    OVERWRITE=TRUE 
    AUTO_COMPRESS=FALSE;
```

Or using Snowflake CLI:

```bash
snow streamlit deploy --replace
```

### 4. Access the Application

Navigate to your Streamlit app in Snowsight:
1. Go to **Projects** → **Streamlit**
2. Find **CORTEX_THREAD_COST_ANALYZER**
3. Click to open the application

## Usage

### Selecting an Agent and Thread

1. **Select Agent**: Choose from the dropdown list of Cortex Agents that have execution history
2. **Select Thread**: Either enter a Thread ID manually or select from recent threads
3. The app will load all trace spans and cost data for that thread

### Understanding the Tabs

| Tab | Description |
|-----|-------------|
| **Timeline** | Chronological list of execution steps with waterfall visualization |
| **Cortex Analyst** | Details of Cortex Analyst calls including semantic models, SQL, and results |
| **Custom Tools** | Information about custom tool invocations |
| **SQL Executions** | All SQL queries executed during the thread |
| **Cost Breakdown** | Detailed cost analysis by category with calculation transparency |
| **Raw Data** | Raw trace span data for debugging |

### Cost Breakdown Categories

| Category | Description | Pricing Table |
|----------|-------------|---------------|
| **LLM_REASONING** | Token costs for agent reasoning/planning steps | Table 6(e) |
| **CORTEX_ANALYST** | Token costs for Cortex Analyst via Agents | Table 6(f) |
| **WAREHOUSE_COMPUTE** | Compute credits for SQL query execution | Standard compute |

## Understanding Pricing

### Table 6(e): LLM Reasoning / Cortex Agents Orchestration

These rates apply to the agent's reasoning and planning steps (e.g., `ReasoningAgentStepPlanning`).

| Model | Input (credits/1M tokens) | Output (credits/1M tokens) |
|-------|---------------------------|----------------------------|
| claude-4-sonnet | 1.88 | 9.41 |
| claude-sonnet-4-5 | 2.07 | 10.36 |
| claude-4-opus | 9.41 | 47.07 |
| claude-opus-4-5 | 3.45 | 17.26 |
| claude-haiku-4-5 | 0.69 | 3.45 |
| llama3.3-70b | 0.60 | 0.60 |
| openai-gpt-5 | 0.86 | 6.90 |
| mistral-large2 | 0.60 | 1.76 |

### Table 6(f): Cortex Analyst via Agents

**Important**: These rates are **different** from Table 6(e) and are approximately **1.67x higher**.

| Model | Input (credits/1M tokens) | Output (credits/1M tokens) |
|-------|---------------------------|----------------------------|
| claude-4-sonnet | 3.14 | 15.69 |
| claude-sonnet-4-5 | 3.46 | 17.28 |
| claude-4-opus | 15.69 | 78.45 |
| claude-opus-4-5 | 5.75 | 28.77 |
| claude-haiku-4-5 | 1.15 | 5.75 |
| llama3.3-70b | 1.00 | 1.00 |
| gemini-2-5-flash | 0.31 | 2.62 |
| mistral-large2 | 1.00 | 2.94 |

**Source**: [Snowflake Credit Consumption Table](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)

### Warehouse Compute

Warehouse credits are calculated as:

```
credits = warehouse_size_multiplier × (execution_time_ms / 3,600,000) + cloud_services
```

| Warehouse Size | Multiplier |
|---------------|------------|
| X-Small | 1 |
| Small | 2 |
| Medium | 4 |
| Large | 8 |
| X-Large | 16 |
| 2X-Large | 32 |
| 3X-Large | 64 |
| 4X-Large | 128 |

## Data Sources and Latency

### Real-time Data: `SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS`

This function provides immediate access to:
- Trace spans (execution steps)
- Token counts for LLM reasoning
- Cortex Analyst message content
- SQL query IDs and results

**Latency**: Near real-time (seconds)

### Accurate Billing Data: `SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY`

This view provides:
- Accurate token counts for Cortex Analyst
- Accurate credit consumption
- Granular breakdown by model and request

**Latency**: 2-3 hours (can be longer during high-load periods)

### How the App Handles Data Latency

1. **For recent threads (<3 hours)**: The app estimates Cortex Analyst costs using content-based token estimation:
   - Tokens ≈ characters ÷ 4
   - Rates assume `claude-4-sonnet` (most common model)
   - Shows yellow warning indicating estimate

2. **For older threads (>3 hours)**: The app uses accurate data from `CORTEX_AGENT_USAGE_HISTORY`:
   - Shows green checkmark indicating accurate data
   - Displays actual token counts and rates

## Technical Details

### Record ID Mapping

Each agent thread has a unique `record_id` that correlates between:
- `RECORD_ATTRIBUTES:"ai.observability.record_id"` in observability events
- `REQUEST_ID` in `CORTEX_AGENT_USAGE_HISTORY`

The app extracts this ID to join real-time observability data with accurate billing data.

### Token Estimation Formula

When accurate data isn't available, Cortex Analyst costs are estimated:

```
estimated_input_tokens = input_characters ÷ 4
estimated_output_tokens = output_characters ÷ 4
estimated_credits = (input_tokens × 3.14 / 1,000,000) + (output_tokens × 15.69 / 1,000,000)
```

**Note**: Actual token counts are typically **5-15x higher** than content-based estimates because Cortex Analyst internally uses additional context (semantic model definitions, schemas, examples) not visible in the observability API.

### SQL Query Structure

The main cost query uses multiple CTEs:

1. `trace_spans` - Extracts and categorizes spans from observability
2. `thread_timing` - Calculates thread age for latency determination
3. `thread_record_id` - Gets the record_id for ACCOUNT_USAGE correlation
4. `analyst_usage_data` - Fetches accurate data from ACCOUNT_USAGE
5. `analyst_token_details` - Parses granular token breakdown
6. `analyst_credit_details` - Parses granular credit breakdown
7. `analyst_content_estimate` - Calculates content-based estimation
8. `analyst_estimated_credits` - Applies Table 6(f) rates to estimates
9. `analyst_summary` - Combines accurate/estimated data
10. `llm_token_summary` - Aggregates LLM reasoning tokens with Table 6(e) rates
11. `warehouse_compute` - Calculates warehouse credits from QUERY_HISTORY
12. `category_summary` - Final aggregation by cost category

## Troubleshooting

### "No Cortex Agents found"

- Ensure you have the `ACCOUNTADMIN` role or equivalent privileges
- Verify that Cortex Agents have been executed in the last 30 days
- Check that the warehouse has sufficient privileges

### Cortex Analyst showing "estimated" for old threads

- ACCOUNT_USAGE has variable latency (typically 2-3 hours, sometimes longer)
- Verify the thread's `record_id` exists in `CORTEX_AGENT_USAGE_HISTORY`:
  ```sql
  SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
  WHERE REQUEST_ID = '<record_id>';
  ```

### Costs showing as N/A

- The thread may not have any spans for that category
- For LLM_REASONING: Check if there are `ReasoningAgentStep*` spans
- For CORTEX_ANALYST: Check if there are `CortexAnalystTool*` spans

### Warehouse compute showing 0

- SQL queries may have executed too quickly to register credits
- Cloud services credits are only charged when they exceed 10% of compute

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

MIT License - See [LICENSE](LICENSE) for details.

## Acknowledgments

- Built for analyzing [Snowflake Cortex Agents](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents)
- Pricing data from [Snowflake Credit Consumption Table](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)
- Uses [Streamlit](https://streamlit.io/) for the web interface
