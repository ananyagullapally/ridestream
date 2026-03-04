import streamlit as st
import psycopg2
import pandas as pd
import time

# ---------------------------------------------------
# Page Setup
# ---------------------------------------------------

st.set_page_config(page_title="Ride Surge Dashboard", layout="wide")

st.title("Real-Time Ride Monitoring Dashboard")

# ---------------------------------------------------
# Connect to Postgres
# ---------------------------------------------------

def load_data():

    conn = psycopg2.connect(
        host="localhost",
        database="rides",
        user="admin",
        password="admin"
    )

    query = """
    SELECT *
    FROM city_minute_metrics
    ORDER BY window_start DESC
    LIMIT 100
    """

    df = pd.read_sql(query, conn)

    conn.close()

    return df


# ---------------------------------------------------
# Auto Refresh
# ---------------------------------------------------

placeholder = st.empty()

while True:

    df = load_data()

    with placeholder.container():

        st.subheader("Latest Ride Metrics")

        st.dataframe(df)

        # ------------------------------------------
        # City Metrics
        # ------------------------------------------

        st.subheader("Rides Per City")

        city_summary = df.groupby("city").agg(
            rides=("rides_per_window", "sum"),
            revenue=("revenue_per_window", "sum")
        ).reset_index()

        st.bar_chart(city_summary.set_index("city")["rides"])

        st.subheader("Revenue Per City")

        st.bar_chart(city_summary.set_index("city")["revenue"])

        # ------------------------------------------
        # Surge Detection
        # ------------------------------------------

        st.subheader("Surge Alerts")

        surge_df = df[df["surge_active"] == True]

        if len(surge_df) == 0:
            st.success("No surge right now")
        else:
            st.error("Surge detected!")
            st.dataframe(surge_df)

    time.sleep(5)
