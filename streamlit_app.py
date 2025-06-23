"""
Streamlit App for Annuity Policy Data Visualization
This app extracts data from Redshift and provides interactive visualizations.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime
import sys
import os

# Add src directory to path to import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Import our Redshift extraction module
from extract_from_redshift import main as extract_from_redshift

# Page configuration
st.set_page_config(
    page_title="Annuity Policy Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .chart-container {
        background-color: white;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_data(limit=None):
    """Load data from Redshift with caching."""
    try:
        st.info("🔄 Connecting to Redshift and extracting data...")
        df = extract_from_redshift()
        if limit:
            df = df.head(limit)
        st.success(f"✅ Successfully loaded {len(df)} records!")
        return df
    except Exception as e:
        st.error(f"❌ Error loading data: {str(e)}")
        return None

def main():
    """Main Streamlit app function."""
    
    # Header
    st.markdown('<h1 class="main-header">📊 Annuity Policy Analytics Dashboard</h1>', unsafe_allow_html=True)
    
    # Sidebar for controls
    st.sidebar.header("🎛️ Dashboard Controls")
    
    # Data loading options
    st.sidebar.subheader("📥 Data Loading")
    use_sample = st.sidebar.checkbox("Use Sample Data (1000 records)", value=True)
    sample_size = st.sidebar.slider("Sample Size", min_value=100, max_value=5000, value=1000, step=100) if use_sample else None
    
    # Load data
    if use_sample:
        df = load_data(limit=sample_size)
    else:
        df = load_data()
    
    if df is None:
        st.error("Failed to load data. Please check your Redshift connection.")
        return
    
    # Data preprocessing
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['year'] = df['start_date'].dt.year
    df['month'] = df['start_date'].dt.month
    
    # Main content
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Policies", f"{len(df):,}")
    
    with col2:
        avg_age = df['age'].mean()
        st.metric("Average Age", f"{avg_age:.1f} years")
    
    with col3:
        avg_value = df['initial_value'].mean()
        st.metric("Average Initial Value", f"${avg_value:,.0f}")
    
    with col4:
        deferred_pct = (df['status'] == 'deferred').mean() * 100
        st.metric("Deferred Policies", f"{deferred_pct:.1f}%")
    
    # Tabs for different visualizations
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 Overview", "👥 Demographics", "💰 Financial", "📅 Temporal", "🔍 Details"])
    
    with tab1:
        st.header("📈 Policy Overview")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Policy Status Distribution
            status_counts = df['status'].value_counts()
            fig_status = px.pie(
                values=status_counts.values,
                names=status_counts.index,
                title="Policy Status Distribution",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig_status.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_status, use_container_width=True)
        
        with col2:
            # Rider Type Distribution
            rider_counts = df['rider_type'].value_counts()
            fig_rider = px.bar(
                x=rider_counts.index,
                y=rider_counts.values,
                title="Rider Type Distribution",
                color_discrete_sequence=['#1f77b4']
            )
            fig_rider.update_layout(xaxis_title="Rider Type", yaxis_title="Count")
            st.plotly_chart(fig_rider, use_container_width=True)
    
    with tab2:
        st.header("👥 Demographics Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Age Distribution by Gender
            fig_age_gender = px.histogram(
                df, x='age', color='gender', 
                title="Age Distribution by Gender",
                nbins=20,
                color_discrete_map={'M': '#1f77b4', 'F': '#ff7f0e'}
            )
            fig_age_gender.update_layout(xaxis_title="Age", yaxis_title="Count")
            st.plotly_chart(fig_age_gender, use_container_width=True)
        
        with col2:
            # Gender Distribution
            gender_counts = df['gender'].value_counts()
            fig_gender = px.pie(
                values=gender_counts.values,
                names=gender_counts.index,
                title="Gender Distribution",
                color_discrete_sequence=['#1f77b4', '#ff7f0e']
            )
            fig_gender.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_gender, use_container_width=True)
    
    with tab3:
        st.header("💰 Financial Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Initial Value Distribution
            fig_value = px.histogram(
                df, x='initial_value', 
                title="Initial Value Distribution",
                nbins=30,
                color_discrete_sequence=['#1f77b4']
            )
            fig_value.update_layout(xaxis_title="Initial Value ($)", yaxis_title="Count")
            st.plotly_chart(fig_value, use_container_width=True)
        
        with col2:
            # Total Fees vs Initial Value
            fig_fees = px.scatter(
                df, x='initial_value', y='total_fees',
                title="Total Fees vs Initial Value",
                color='rider_type',
                size='age',
                hover_data=['policy_id', 'age', 'gender']
            )
            fig_fees.update_layout(xaxis_title="Initial Value ($)", yaxis_title="Total Fees")
            st.plotly_chart(fig_fees, use_container_width=True)
    
    with tab4:
        st.header("📅 Temporal Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Policies by Year
            year_counts = df['year'].value_counts().sort_index()
            fig_year = px.line(
                x=year_counts.index,
                y=year_counts.values,
                title="Policies Created by Year",
                markers=True
            )
            fig_year.update_layout(xaxis_title="Year", yaxis_title="Number of Policies")
            st.plotly_chart(fig_year, use_container_width=True)
        
        with col2:
            # Deferral Years Distribution
            deferral_counts = df['deferral_years'].value_counts().sort_index()
            fig_deferral = px.bar(
                x=deferral_counts.index,
                y=deferral_counts.values,
                title="Deferral Years Distribution",
                color_discrete_sequence=['#1f77b4']
            )
            fig_deferral.update_layout(xaxis_title="Deferral Years", yaxis_title="Count")
            st.plotly_chart(fig_deferral, use_container_width=True)
    
    with tab5:
        st.header("🔍 Detailed Data View")
        
        # Filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            selected_status = st.multiselect(
                "Filter by Status",
                options=df['status'].unique(),
                default=df['status'].unique()
            )
        
        with col2:
            selected_rider = st.multiselect(
                "Filter by Rider Type",
                options=df['rider_type'].unique(),
                default=df['rider_type'].unique()
            )
        
        with col3:
            age_range = st.slider(
                "Age Range",
                min_value=int(df['age'].min()),
                max_value=int(df['age'].max()),
                value=(int(df['age'].min()), int(df['age'].max()))
            )
        
        # Apply filters
        filtered_df = df[
            (df['status'].isin(selected_status)) &
            (df['rider_type'].isin(selected_rider)) &
            (df['age'] >= age_range[0]) &
            (df['age'] <= age_range[1])
        ]
        
        st.write(f"Showing {len(filtered_df)} of {len(df)} records")
        
        # Display filtered data
        st.dataframe(
            filtered_df,
            use_container_width=True,
            hide_index=True
        )
        
        # Download button
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="📥 Download Filtered Data as CSV",
            data=csv,
            file_name=f"annuity_policies_filtered_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    main() 