import streamlit as st
import pandas as pd
from utils import format_credits, format_tokens, safe_float, safe_int
from config import CORTEX_CODE_CLI_PRICING, TOKEN_TYPE_LABELS


def render_overview(overview_df, days):
    st.subheader("Usage Overview")

    if overview_df.empty:
        st.warning("No Cortex Code CLI usage found.")
        return

    row = overview_df.iloc[0]
    total_credits = safe_float(row.get("TOTAL_CREDITS"))
    total_requests = safe_int(row.get("TOTAL_REQUESTS"))
    total_tokens = safe_int(row.get("TOTAL_TOKENS"))
    distinct_users = safe_int(row.get("DISTINCT_USERS"))
    avg_credits = safe_float(row.get("AVG_CREDITS_PER_REQUEST"))
    median_credits = safe_float(row.get("MEDIAN_CREDITS_PER_REQUEST"))

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        st.metric("Total Credits", format_credits(total_credits))
    with col2:
        st.metric("Total Requests", f"{total_requests:,}")
    with col3:
        st.metric("Total Tokens", format_tokens(total_tokens))
    with col4:
        st.metric("Active Users", distinct_users)
    with col5:
        st.metric("Avg Credits/Request", format_credits(avg_credits))
    with col6:
        st.metric("Median Credits/Request", format_credits(median_credits))

    earliest = row.get("EARLIEST_USAGE")
    latest = row.get("LATEST_USAGE")
    if earliest and latest:
        st.caption(f"Data range: {earliest} to {latest} (last {days} days)")


def render_daily_trends(daily_df):
    st.subheader("Daily Credit Consumption")

    if daily_df.empty:
        st.info("No daily usage data available.")
        return

    chart_df = daily_df[["USAGE_DAY", "DAILY_CREDITS"]].copy()
    chart_df = chart_df.set_index("USAGE_DAY")
    st.bar_chart(chart_df["DAILY_CREDITS"], use_container_width=True)

    st.subheader("Daily Request Volume")
    req_df = daily_df[["USAGE_DAY", "REQUESTS"]].copy()
    req_df = req_df.set_index("USAGE_DAY")
    st.bar_chart(req_df["REQUESTS"], use_container_width=True)

    with st.expander("Daily usage table", expanded=False):
        display_df = daily_df.copy()
        display_df["DAILY_CREDITS"] = display_df["DAILY_CREDITS"].apply(
            lambda x: format_credits(x)
        )
        display_df["DAILY_TOKENS"] = display_df["DAILY_TOKENS"].apply(
            lambda x: format_tokens(x)
        )
        st.dataframe(display_df, use_container_width=True)


def render_model_breakdown(model_df):
    st.subheader("Usage by Model")

    if model_df.empty:
        st.info("No model usage data available.")
        return

    for _, row in model_df.iterrows():
        model = row.get("MODEL_NAME", "Unknown")
        total_credits = safe_float(row.get("TOTAL_CREDITS"))
        requests = safe_int(row.get("REQUESTS"))

        with st.expander(
            f"**{model}** - {format_credits(total_credits)} credits, {requests:,} requests",
            expanded=True,
        ):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Credits", format_credits(total_credits))
            with col2:
                st.metric("Requests", f"{requests:,}")
            with col3:
                st.metric("Total Tokens", format_tokens(row.get("TOTAL_TOKENS")))
            with col4:
                input_creds = safe_float(row.get("INPUT_CREDITS"))
                cache_read_creds = safe_float(row.get("CACHE_READ_CREDITS"))
                cache_write_creds = safe_float(row.get("CACHE_WRITE_CREDITS"))
                output_creds = safe_float(row.get("OUTPUT_CREDITS"))
                computed_total = input_creds + cache_read_creds + cache_write_creds + output_creds
                st.metric("Sum (granular)", format_credits(computed_total))

            st.markdown("---")
            st.markdown("**Token Breakdown:**")
            tcol1, tcol2, tcol3, tcol4 = st.columns(4)
            with tcol1:
                st.markdown("**Input**")
                st.code(
                    f"{safe_int(row.get('INPUT_TOKENS')):,} tokens\n{format_credits(row.get('INPUT_CREDITS'))} credits",
                    language=None,
                )
            with tcol2:
                st.markdown("**Cache Read**")
                st.code(
                    f"{safe_int(row.get('CACHE_READ_TOKENS')):,} tokens\n{format_credits(row.get('CACHE_READ_CREDITS'))} credits",
                    language=None,
                )
            with tcol3:
                st.markdown("**Cache Write**")
                st.code(
                    f"{safe_int(row.get('CACHE_WRITE_TOKENS')):,} tokens\n{format_credits(row.get('CACHE_WRITE_CREDITS'))} credits",
                    language=None,
                )
            with tcol4:
                st.markdown("**Output**")
                st.code(
                    f"{safe_int(row.get('OUTPUT_TOKENS')):,} tokens\n{format_credits(row.get('OUTPUT_CREDITS'))} credits",
                    language=None,
                )

            st.markdown("**Derived Rates** (credits per million tokens):")
            rates_data = {
                "Token Type": ["Input", "Cache Read", "Cache Write", "Output"],
                "Rate (credits/1M tokens)": [
                    row.get("DERIVED_INPUT_RATE"),
                    row.get("DERIVED_CACHE_READ_RATE"),
                    row.get("DERIVED_CACHE_WRITE_RATE"),
                    row.get("DERIVED_OUTPUT_RATE"),
                ],
            }
            st.dataframe(pd.DataFrame(rates_data), use_container_width=True)


def render_user_usage(user_df):
    st.subheader("Usage by User")

    if user_df.empty:
        st.info("No user usage data available.")
        return

    display_cols = [
        "USER_NAME",
        "REQUESTS",
        "TOTAL_CREDITS",
        "TOTAL_TOKENS",
        "AVG_CREDITS_PER_REQUEST",
        "FIRST_USAGE",
        "LAST_USAGE",
    ]
    available_cols = [c for c in display_cols if c in user_df.columns]
    st.dataframe(user_df[available_cols], use_container_width=True)


def render_cost_analysis(dist_df, model_df):
    st.subheader("Cost Distribution (per request)")

    if not dist_df.empty:
        row = dist_df.iloc[0]
        col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
        with col1:
            st.metric("Min", format_credits(row.get("MIN_CREDITS")))
        with col2:
            st.metric("Avg", format_credits(row.get("AVG_CREDITS")))
        with col3:
            st.metric("Median", format_credits(row.get("MEDIAN_CREDITS")))
        with col4:
            st.metric("P90", format_credits(row.get("P90_CREDITS")))
        with col5:
            st.metric("P95", format_credits(row.get("P95_CREDITS")))
        with col6:
            st.metric("P99", format_credits(row.get("P99_CREDITS")))
        with col7:
            st.metric("Max", format_credits(row.get("MAX_CREDITS")))

    st.markdown("---")

    with st.expander("Pricing Methodology", expanded=False):
        st.markdown("""
        **Source:** [Snowflake Credit Consumption Table](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf)

        Cortex Code CLI is billed based on token consumption. Credits come pre-calculated
        from `SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY`.

        **Token types:**
        | Type | Description |
        |------|-------------|
        | **Input** | New tokens sent as input to the model |
        | **Cache Read** | Previously cached input tokens (discounted) |
        | **Cache Write** | New tokens written to cache for future reuse |
        | **Output** | Tokens generated by the model |

        Cache read tokens are significantly cheaper than other token types.
        """)

    with st.expander("Pricing Reference Table (derived from actual billing)", expanded=False):
        st.markdown("*Credits per million tokens - derived from `CREDITS_GRANULAR / TOKENS_GRANULAR`*")

        if not model_df.empty:
            pricing_data = {
                "Model": [],
                "Input Rate": [],
                "Cache Read Rate": [],
                "Cache Write Rate": [],
                "Output Rate": [],
            }
            for _, row in model_df.iterrows():
                pricing_data["Model"].append(row.get("MODEL_NAME", "Unknown"))
                pricing_data["Input Rate"].append(row.get("DERIVED_INPUT_RATE"))
                pricing_data["Cache Read Rate"].append(row.get("DERIVED_CACHE_READ_RATE"))
                pricing_data["Cache Write Rate"].append(row.get("DERIVED_CACHE_WRITE_RATE"))
                pricing_data["Output Rate"].append(row.get("DERIVED_OUTPUT_RATE"))
            st.dataframe(pd.DataFrame(pricing_data), use_container_width=True)

        st.markdown("---")
        st.markdown("**Reference rates from config:**")
        ref_data = {
            "Model": [],
            "Input": [],
            "Cache Read": [],
            "Cache Write": [],
            "Output": [],
        }
        for model, rates in CORTEX_CODE_CLI_PRICING.items():
            ref_data["Model"].append(model)
            ref_data["Input"].append(rates["input"])
            ref_data["Cache Read"].append(rates["cache_read_input"])
            ref_data["Cache Write"].append(rates["cache_write_input"])
            ref_data["Output"].append(rates["output"])
        st.dataframe(pd.DataFrame(ref_data), use_container_width=True)


def render_raw_data(request_df):
    st.subheader("Request Details")

    if request_df.empty:
        st.info("No request data available.")
        return

    st.caption(f"Showing top {len(request_df)} requests by credit consumption")

    display_cols = [
        "REQUEST_ID",
        "USER_NAME",
        "USAGE_TIME",
        "TOKEN_CREDITS",
        "TOKENS",
    ]
    available_cols = [c for c in display_cols if c in request_df.columns]
    st.dataframe(request_df[available_cols], use_container_width=True)

    with st.expander("Request details with granular breakdown", expanded=False):
        st.dataframe(request_df, use_container_width=True)
