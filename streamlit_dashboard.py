#!/usr/bin/env python3
"""
Streamlit Dashboard for E-commerce Data Pipeline

This module implements a visualization dashboard for the e-commerce data pipeline
using Streamlit. It displays KPIs, data quality metrics, and pipeline health.

Features:
- KPI visualizations (total revenue, top products, user retention)
- Data quality metrics
- Pipeline health monitoring
- Anomaly detection results
"""

import os
import sys
import json
import datetime
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Set page configuration
st.set_page_config(
    page_title="E-commerce Data Pipeline Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Define paths - make them work in both local and Streamlit Cloud environments
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
MONITORING_DIR = os.path.join(BASE_DIR, "monitoring")

# Create directories if they don't exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(MONITORING_DIR, exist_ok=True)

# Create sample data if real data doesn't exist yet
def create_sample_data():
    """Create sample data for demonstration purposes."""
    # Set seed for reproducibility
    np.random.seed(42)
    
    # Sample revenue data
    revenue_data = pd.DataFrame({
        "date": pd.date_range(start="2023-01-01", periods=90, freq="D"),
        "total_revenue": np.random.normal(10000, 2000, 90),
        "order_count": np.random.normal(500, 100, 90).astype(int),
        "items_sold": np.random.normal(1500, 300, 90).astype(int)
    })
    
    # Add some weekly patterns
    revenue_data["total_revenue"] = revenue_data["total_revenue"] * (1 + 0.3 * (revenue_data["date"].dt.dayofweek == 5))
    revenue_data["total_revenue"] = revenue_data["total_revenue"] * (1 + 0.2 * (revenue_data["date"].dt.dayofweek == 6))
    
    # Ensure no negative values
    revenue_data["total_revenue"] = revenue_data["total_revenue"].clip(lower=0)
    revenue_data["order_count"] = revenue_data["order_count"].clip(lower=0)
    revenue_data["items_sold"] = revenue_data["items_sold"].clip(lower=0)
    
    # Rest of the function remains the same
    # Sample product data
    categories = ["Electronics", "Clothing", "Home", "Books", "Sports"]
    products = []
    
    for i in range(50):
        category = categories[i % len(categories)]
        product_id = i + 1
        product_name = f"Product {product_id}"
        revenue = np.random.normal(5000, 1000) * (1 + 0.5 * (i < 10))
        quantity = np.random.normal(250, 50) * (1 + 0.5 * (i < 15))
        
        products.append({
            "product_id": product_id,
            "product_name": product_name,
            "category": category,
            "revenue": max(0, revenue),  # Ensure no negative values
            "quantity_sold": max(0, int(quantity)),  # Ensure no negative values
            "avg_price": max(0, revenue / quantity if quantity > 0 else 0)  # Ensure no negative values
        })
    
    product_data = pd.DataFrame(products)
    
    # Sample user retention data
    months = ["2023-01", "2023-02", "2023-03"]
    cohorts = []
    
    for cohort_month in months:
        initial_users = np.random.randint(800, 1200)
        
        for i, month in enumerate(months):
            if month >= cohort_month:
                # Retention rate decreases over time
                retention_rate = 1.0 if i == 0 else (0.7 if i == 1 else 0.5)
                active_users = int(initial_users * retention_rate)
                
                cohorts.append({
                    "cohort_month": cohort_month,
                    "activity_month": month,
                    "active_users": active_users,
                    "retention_rate": retention_rate
                })
    
    retention_data = pd.DataFrame(cohorts)
    
    # Sample data quality metrics
    quality_metrics = []
    
    for dataset in ["user_activity", "orders", "inventory"]:
        for i in range(30):
            date = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
            
            quality_metrics.append({
                "date": date,
                "dataset": dataset,
                "completeness": np.random.uniform(0.95, 1.0),
                "uniqueness": np.random.uniform(0.98, 1.0),
                "validity": np.random.uniform(0.97, 1.0),
                "consistency": np.random.uniform(0.96, 1.0)
            })
    
    quality_data = pd.DataFrame(quality_metrics)
    
    # Sample pipeline health metrics
    health_metrics = []
    
    for i in range(30):
        date = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        
        health_metrics.append({
            "date": date,
            "processing_time_minutes": np.random.normal(15, 3),
            "error_count": np.random.poisson(0.2),
            "bronze_size_gb": np.random.normal(2, 0.1) + i * 0.05,
            "silver_size_gb": np.random.normal(1.5, 0.08) + i * 0.03,
            "gold_size_gb": np.random.normal(1, 0.05) + i * 0.02
        })
    
    health_data = pd.DataFrame(health_metrics)
    
    return {
        "revenue_data": revenue_data,
        "product_data": product_data,
        "retention_data": retention_data,
        "quality_data": quality_data,
        "health_data": health_data
    }

# Load data
@st.cache_data(ttl=3600)
def load_data():
    """Load data for the dashboard."""
    # In a real implementation, we would load data from the data lake
    # For this demo, we'll use sample data
    return create_sample_data()

# Dashboard components
def render_header():
    """Render the dashboard header."""
    st.title("E-commerce Data Pipeline Dashboard")
    st.markdown("""
    This dashboard provides insights into the e-commerce data pipeline, including KPIs,
    data quality metrics, and pipeline health monitoring.
    """)
    
    # Add date filter
    col1, col2 = st.columns([1, 3])
    with col1:
        date_range = st.selectbox(
            "Date Range",
            ["Last 7 days", "Last 30 days", "Last 90 days", "Year to date", "All time"],
            index=1
        )

def render_kpi_metrics(data):
    """Render KPI metrics cards."""
    st.subheader("Key Performance Indicators")
    
    revenue_data = data["revenue_data"]
    
    # Calculate metrics
    total_revenue = revenue_data["total_revenue"].sum()
    avg_daily_revenue = revenue_data["total_revenue"].mean()
    total_orders = revenue_data["order_count"].sum()
    total_items = revenue_data["items_sold"].sum()
    
    # Create metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Revenue",
            f"${total_revenue:,.2f}",
            f"{np.random.uniform(2, 8):.1f}%"
        )
    
    with col2:
        st.metric(
            "Average Daily Revenue",
            f"${avg_daily_revenue:,.2f}",
            f"{np.random.uniform(1, 5):.1f}%"
        )
    
    with col3:
        st.metric(
            "Total Orders",
            f"{total_orders:,}",
            f"{np.random.uniform(3, 7):.1f}%"
        )
    
    with col4:
        st.metric(
            "Total Items Sold",
            f"{total_items:,}",
            f"{np.random.uniform(2, 6):.1f}%"
        )

def render_revenue_chart(data):
    """Render revenue trend chart."""
    revenue_data = data["revenue_data"]
    
    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Add revenue line
    fig.add_trace(
        go.Scatter(
            x=revenue_data["date"],
            y=revenue_data["total_revenue"],
            name="Revenue",
            line=dict(color="#1f77b4", width=3)
        ),
        secondary_y=False
    )
    
    # Add order count line
    fig.add_trace(
        go.Scatter(
            x=revenue_data["date"],
            y=revenue_data["order_count"],
            name="Orders",
            line=dict(color="#ff7f0e", width=2, dash="dot")
        ),
        secondary_y=True
    )
    
    # Set titles
    fig.update_layout(
        title="Daily Revenue and Order Count",
        xaxis_title="Date",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    # Set y-axes titles
    fig.update_yaxes(title_text="Revenue ($)", secondary_y=False)
    fig.update_yaxes(title_text="Order Count", secondary_y=True)
    
    # Display the chart
    st.plotly_chart(fig, use_container_width=True)

def render_top_products(data):
    """Render top products chart."""
    product_data = data["product_data"]
    
    # Sort by revenue
    top_products = product_data.sort_values("revenue", ascending=False).head(10)
    
    # Create bar chart
    fig = px.bar(
        top_products,
        x="product_name",
        y="revenue",
        color="category",
        title="Top 10 Products by Revenue",
        labels={"product_name": "Product", "revenue": "Revenue ($)", "category": "Category"}
    )
    
    # Update layout
    fig.update_layout(xaxis_tickangle=-45)
    
    # Display the chart
    st.plotly_chart(fig, use_container_width=True)

def render_category_breakdown(data):
    """Render category breakdown chart."""
    product_data = data["product_data"]
    
    # Group by category
    category_data = product_data.groupby("category").agg({
        "revenue": "sum",
        "quantity_sold": "sum"
    }).reset_index()
    
    # Create pie chart
    fig = px.pie(
        category_data,
        values="revenue",
        names="category",
        title="Revenue by Category",
        hole=0.4
    )
    
    # Update layout
    fig.update_traces(textposition="inside", textinfo="percent+label")
    
    # Display the chart
    st.plotly_chart(fig, use_container_width=True)

def render_user_retention(data):
    """Render user retention chart."""
    retention_data = data["retention_data"]
    
    # Pivot data for heatmap
    retention_pivot = retention_data.pivot(
        index="cohort_month",
        columns="activity_month",
        values="retention_rate"
    )
    
    # Create heatmap
    fig = px.imshow(
        retention_pivot,
        labels=dict(x="Activity Month", y="Cohort Month", color="Retention Rate"),
        x=retention_pivot.columns,
        y=retention_pivot.index,
        color_continuous_scale="Blues",
        title="User Retention by Cohort"
    )
    
    # Update layout
    fig.update_layout(
        xaxis_title="Activity Month",
        yaxis_title="Cohort Month"
    )
    
    # Add text annotations
    for i in range(len(retention_pivot.index)):
        for j in range(len(retention_pivot.columns)):
            if not np.isnan(retention_pivot.iloc[i, j]):
                fig.add_annotation(
                    x=j,
                    y=i,
                    text=f"{retention_pivot.iloc[i, j]:.0%}",
                    showarrow=False,
                    font=dict(color="white" if retention_pivot.iloc[i, j] > 0.5 else "black")
                )
    
    # Display the chart
    st.plotly_chart(fig, use_container_width=True)

def render_data_quality(data):
    """Render data quality metrics."""
    st.subheader("Data Quality Metrics")
    
    quality_data = data["quality_data"]
    
    # Filter for latest date
    latest_date = quality_data["date"].max()
    latest_quality = quality_data[quality_data["date"] == latest_date]
    
    # Create metrics for each dataset
    datasets = latest_quality["dataset"].unique()
    cols = st.columns(len(datasets))
    
    for i, dataset in enumerate(datasets):
        dataset_quality = latest_quality[latest_quality["dataset"] == dataset]
        
        with cols[i]:
            st.subheader(dataset.capitalize())
            
            # Create gauge charts for each metric
            for metric in ["completeness", "uniqueness", "validity", "consistency"]:
                value = dataset_quality[metric].values[0]
                
                fig = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=value * 100,
                    domain={"x": [0, 1], "y": [0, 1]},
                    title={"text": metric.capitalize()},
                    gauge={
                        "axis": {"range": [0, 100]},
                        "bar": {"color": "darkblue"},
                        "steps": [
                            {"range": [0, 80], "color": "red"},
                            {"range": [80, 95], "color": "orange"},
                            {"range": [95, 100], "color": "green"}
                        ],
                        "threshold": {
                            "line": {"color": "black", "width": 4},
                            "thickness": 0.75,
                            "value": 95
                        }
                    }
                ))
                
                fig.update_layout(height=200, margin=dict(l=20, r=20, t=50, b=20))
                st.plotly_chart(fig, use_container_width=True)
    
    # Show quality trend
    st.subheader("Data Quality Trend")
    
    # Group by date and dataset
    quality_trend = quality_data.groupby(["date", "dataset"]).mean().reset_index()
    
    # Create line chart
    fig = px.line(
        quality_trend,
        x="date",
        y="completeness",
        color="dataset",
        title="Data Completeness Trend",
        labels={"date": "Date", "completeness": "Completeness", "dataset": "Dataset"}
    )
    
    # Update layout
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Completeness",
        yaxis=dict(range=[0.9, 1.0])
    )
    
    # Display the chart
    st.plotly_chart(fig, use_container_width=True)

def render_pipeline_health(data):
    """Render pipeline health metrics."""
    st.subheader("Pipeline Health Monitoring")
    
    health_data = data["health_data"]
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["Processing Time", "Error Count", "Storage Size"])
    
    with tab1:
        # Create processing time chart
        fig = px.line(
            health_data,
            x="date",
            y="processing_time_minutes",
            title="Daily Processing Time",
            labels={"date": "Date", "processing_time_minutes": "Processing Time (minutes)"}
        )
        
        # Add threshold line
        fig.add_shape(
            type="line",
            x0=health_data["date"].min(),
            y0=20,
            x1=health_data["date"].max(),
            y1=20,
            line=dict(color="red", width=2, dash="dash")
        )
        
        # Update layout
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Processing Time (minutes)"
        )
        
        # Display the chart
        st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        # Create error count chart
        fig = px.bar(
            health_data,
            x="date",
            y="error_count",
            title="Daily Error Count",
            labels={"date": "Date", "error_count": "Error Count"}
        )
        
        # Update layout
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Error Count"
        )
        
        # Display the chart
        st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        # Create storage size chart
        fig = go.Figure()
        
        # Add lines for each layer
        fig.add_trace(go.Scatter(
            x=health_data["date"],
            y=health_data["bronze_size_gb"],
            name="Bronze",
            stackgroup="one",
            groupnorm="percent"
        ))
        
        fig.add_trace(go.Scatter(
            x=health_data["date"],
            y=health_data["silver_size_gb"],
            name="Silver",
            stackgroup="one"
        ))
        
        fig.add_trace(go.Scatter(
            x=health_data["date"],
            y=health_data["gold_size_gb"],
            name="Gold",
            stackgroup="one"
        ))
        
        # Update layout
        fig.update_layout(
            title="Storage Size by Layer",
            xaxis_title="Date",
            yaxis_title="Percentage",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        # Display the chart
        st.plotly_chart(fig, use_container_width=True)
        
        # Show total storage size
        total_size = health_data["bronze_size_gb"].iloc[0] + health_data["silver_size_gb"].iloc[0] + health_data["gold_size_gb"].iloc[0]
        st.metric("Total Storage Size", f"{total_size:.2f} GB")

def main():
    """Main function to run the dashboard."""
    try:
        # Load data
        data = load_data()
        
        # Render dashboard components
        render_header()
        render_kpi_metrics(data)
        render_revenue_chart(data)
        
        # Create two columns for charts
        col1, col2 = st.columns(2)
        with col1:
            render_top_products(data)
        with col2:
            render_category_breakdown(data)
        
        render_user_retention(data)
        render_data_quality(data)
        render_pipeline_health(data)
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        st.exception(e)

# Add these requirements to a requirements.txt file in your project root:
# streamlit
# plotly
# pandas
# numpy

if __name__ == "__main__":
    # This is the standard Streamlit entry point
    main()

