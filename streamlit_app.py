"""
Cortex Code Cost Analyzer
=========================
A Streamlit app to analyze Cortex Code CLI credit consumption.
Uses SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY for billing data.

Requirements:
- ACCOUNTADMIN role OR granted access to SNOWFLAKE.ACCOUNT_USAGE schema
"""

import streamlit as st
from config import PAGE_CONFIG, DEFAULT_LOOKBACK_DAYS
from data import (
    get_cli_usage_overview,
    get_daily_usage,
    get_user_usage,
    get_model_usage,
    get_cost_distribution,
    get_request_details,
)
from render import (
    render_overview,
    render_daily_trends,
    render_model_breakdown,
    render_user_usage,
    render_cost_analysis,
    render_raw_data,
)

st.set_page_config(**PAGE_CONFIG)


def main():
    st.title("Cortex Code Cost Analyzer")
    st.markdown("Analyze Cortex Code CLI credit consumption by user, model, and time period.")

    days = st.sidebar.slider("Lookback (days)", min_value=1, max_value=365, value=DEFAULT_LOOKBACK_DAYS)

    with st.spinner("Loading usage data..."):
        try:
            overview_df = get_cli_usage_overview(days)
        except Exception as e:
            st.error(f"Error loading data: {str(e)[:300]}")
            st.info(
                "Ensure you have ACCOUNTADMIN or access to "
                "SNOWFLAKE.ACCOUNT_USAGE.CORTEX_CODE_CLI_USAGE_HISTORY."
            )
            return

    if overview_df.empty or overview_df.iloc[0].get("TOTAL_REQUESTS", 0) == 0:
        st.warning("No Cortex Code CLI usage found in the selected time period.")
        return

    render_overview(overview_df, days)
    st.markdown("---")

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Daily Trends",
        "Model Breakdown",
        "User Usage",
        "Cost Analysis",
        "Raw Data",
    ])

    with tab1:
        with st.spinner("Loading daily trends..."):
            try:
                daily_df = get_daily_usage(days)
                render_daily_trends(daily_df)
            except Exception as e:
                st.error(f"Error loading daily trends: {str(e)[:200]}")

    with tab2:
        with st.spinner("Loading model breakdown..."):
            try:
                model_df = get_model_usage(days)
                render_model_breakdown(model_df)
            except Exception as e:
                st.error(f"Error loading model breakdown: {str(e)[:200]}")

    with tab3:
        with st.spinner("Loading user usage..."):
            try:
                user_df = get_user_usage(days)
                render_user_usage(user_df)
            except Exception as e:
                st.error(f"Error loading user usage: {str(e)[:200]}")

    with tab4:
        with st.spinner("Loading cost analysis..."):
            try:
                dist_df = get_cost_distribution(days)
                model_df = get_model_usage(days)
                render_cost_analysis(dist_df, model_df)
            except Exception as e:
                st.error(f"Error loading cost analysis: {str(e)[:200]}")

    with tab5:
        with st.spinner("Loading request details..."):
            try:
                request_df = get_request_details(days)
                render_raw_data(request_df)
            except Exception as e:
                st.error(f"Error loading request details: {str(e)[:200]}")


if __name__ == "__main__":
    main()
