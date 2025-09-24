import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
import datetime as dt
from io import StringIO
import random

# Page configuration
st.set_page_config(
    page_title="Data Engineering ETL Demo",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 5px;
        border-left: 5px solid #667eea;
    }
    .stAlert > div {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
    }
</style>
""", unsafe_allow_html=True)

class ETLPipeline:
    """Main ETL Pipeline Class"""
    
    def __init__(self):
        self.raw_data = None
        self.processed_data = None
        self.data_quality_report = {}
        self.processing_logs = []
        
    def log(self, message):
        """Add log entry with timestamp"""
        timestamp = dt.datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        self.processing_logs.append(log_entry)
        return log_entry
    
    def generate_sample_data(self, num_records=1000):
        """Generate realistic e-commerce sample data"""
        self.log(f"🎲 Generating {num_records} sample records...")
        
        # Sample data categories
        categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Automotive']
        regions = ['North America', 'Europe', 'Asia', 'South America', 'Africa', 'Oceania']
        statuses = ['Completed', 'Pending', 'Cancelled', 'Returned']
        
        # Date range for the last 6 months
        start_date = dt.datetime.now() - dt.timedelta(days=180)
        
        data = []
        for i in range(num_records):
            # Random date within the last 6 months
            random_days = random.randint(0, 180)
            sale_date = start_date + dt.timedelta(days=random_days)
            
            # Generate record
            record = {
                'order_id': f'ORD-{1000 + i}',
                'customer_id': f'CUST-{random.randint(1, 500)}',
                'product_name': f'Product {chr(65 + i % 26)}{i % 100:02d}',
                'category': random.choice(categories),
                'price': round(random.uniform(5.0, 999.99), 2),
                'quantity': random.randint(1, 10),
                'region': random.choice(regions),
                'order_status': random.choice(statuses),
                'order_date': sale_date.strftime('%Y-%m-%d'),
                'customer_email': self._generate_email(i),
                'discount_percent': round(random.uniform(0, 30), 1) if random.random() < 0.3 else 0
            }
            
            # Calculate total amount
            subtotal = record['price'] * record['quantity']
            discount_amount = subtotal * (record['discount_percent'] / 100)
            record['total_amount'] = round(subtotal - discount_amount, 2)
            
            data.append(record)
        
        self.raw_data = pd.DataFrame(data)
        self.log(f"✅ Successfully generated {len(self.raw_data)} records")
        return self.raw_data
    
    def _generate_email(self, index):
        """Generate email addresses with some invalid ones for demo"""
        if random.random() < 0.05:  # 5% invalid emails
            return f"invalid-email-{index}"
        
        domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'company.com']
        return f"customer{index}@{random.choice(domains)}"
    
    def extract_data(self, uploaded_file=None):
        """Extract data from uploaded file or use sample data"""
        if uploaded_file is not None:
            try:
                self.raw_data = pd.read_csv(uploaded_file)
                self.log(f"📁 Loaded {len(self.raw_data)} records from uploaded file")
                return self.raw_data
            except Exception as e:
                self.log(f"❌ Error loading file: {str(e)}")
                return None
        else:
            return self.generate_sample_data()
    
    def transform_data(self):
        """Transform and clean the data"""
        if self.raw_data is None:
            self.log("❌ No data to transform")
            return None
        
        self.log("🔧 Starting data transformation...")
        df = self.raw_data.copy()
        
        # Initialize quality tracking
        total_records = len(df)
        quality_issues = []
        
        # 1. Data Type Conversions
        self.log("📊 Converting data types...")
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
        df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')
        
        # 2. Data Validation
        self.log("✅ Validating data quality...")
        
        # Email validation
        email_mask = df['customer_email'].str.contains('@', na=False)
        invalid_emails = (~email_mask).sum()
        if invalid_emails > 0:
            quality_issues.append(f"Invalid emails: {invalid_emails}")
            df.loc[~email_mask, 'data_quality_flag'] = 'Invalid Email'
        
        # Price validation
        invalid_prices = (df['price'] <= 0).sum()
        if invalid_prices > 0:
            quality_issues.append(f"Invalid prices: {invalid_prices}")
            df.loc[df['price'] <= 0, 'data_quality_flag'] = 'Invalid Price'
        
        # Quantity validation
        invalid_quantities = (df['quantity'] <= 0).sum()
        if invalid_quantities > 0:
            quality_issues.append(f"Invalid quantities: {invalid_quantities}")
            df.loc[df['quantity'] <= 0, 'data_quality_flag'] = 'Invalid Quantity'
        
        # 3. Business Logic Application
        self.log("💼 Applying business rules...")
        
        # Create price tiers
        df['price_tier'] = pd.cut(df['price'], 
                                 bins=[0, 50, 200, 500, float('inf')],
                                 labels=['Budget', 'Mid-range', 'Premium', 'Luxury'])
        
        # Calculate revenue metrics
        df['revenue'] = df['total_amount']
        df['month'] = df['order_date'].dt.to_period('M')
        df['quarter'] = df['order_date'].dt.to_period('Q')
        
        # Customer segmentation
        customer_totals = df.groupby('customer_id')['total_amount'].sum()
        df['customer_segment'] = df['customer_id'].map(
            lambda x: 'VIP' if customer_totals.get(x, 0) > 1000 
            else 'Regular' if customer_totals.get(x, 0) > 300 
            else 'New'
        )
        
        # 4. Data Enrichment
        self.log("🔍 Enriching data with derived metrics...")
        df['days_since_order'] = (pd.Timestamp.now() - df['order_date']).dt.days
        df['is_weekend_order'] = df['order_date'].dt.weekday >= 5
        
        # 5. Data Quality Summary
        clean_records = len(df[~df.get('data_quality_flag', pd.Series()).notna()])
        error_records = total_records - clean_records
        
        self.data_quality_report = {
            'total_records': total_records,
            'clean_records': clean_records,
            'error_records': error_records,
            'data_quality_score': round((clean_records / total_records) * 100, 2),
            'quality_issues': quality_issues
        }
        
        self.processed_data = df
        self.log(f"✅ Transformation complete: {clean_records}/{total_records} clean records")
        
        return self.processed_data
    
    def load_and_analyze(self):
        """Load data into analytical structures and generate insights"""
        if self.processed_data is None:
            self.log("❌ No processed data to analyze")
            return None
        
        self.log("📊 Generating analytical insights...")
        
        df = self.processed_data
        
        # Generate key metrics
        insights = {
            'total_revenue': df['revenue'].sum(),
            'avg_order_value': df['revenue'].mean(),
            'total_orders': len(df),
            'unique_customers': df['customer_id'].nunique(),
            'top_category': df.groupby('category')['revenue'].sum().idxmax(),
            'best_region': df.groupby('region')['revenue'].sum().idxmax(),
            'monthly_growth': self._calculate_monthly_growth(df)
        }
        
        self.log("✅ Analysis complete - insights generated")
        return insights
    
    def _calculate_monthly_growth(self, df):
        """Calculate month-over-month growth rate"""
        monthly_revenue = df.groupby('month')['revenue'].sum().sort_index()
        if len(monthly_revenue) >= 2:
            current_month = monthly_revenue.iloc[-1]
            previous_month = monthly_revenue.iloc[-2]
            growth_rate = ((current_month - previous_month) / previous_month) * 100
            return round(growth_rate, 2)
        return 0

def main():
    """Main Streamlit application"""
    
    # Header
    st.markdown("""
    <div class="main-header">
        <h1 style="color: white; margin: 0;">🚀 Data Engineering ETL Pipeline Demo</h1>
        <p style="color: white; margin: 10px 0 0 0;">Interactive demonstration of Extract, Transform, Load processes</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Initialize session state
    if 'etl_pipeline' not in st.session_state:
        st.session_state.etl_pipeline = ETLPipeline()
    
    pipeline = st.session_state.etl_pipeline
    
    # Sidebar controls
    st.sidebar.header("🎛️ Pipeline Controls")
    
    # Data Source Selection
    st.sidebar.subheader("1️⃣ Extract - Data Source")
    data_source = st.sidebar.radio(
        "Choose data source:",
        ["Generate Sample Data", "Upload CSV File"]
    )
    
    uploaded_file = None
    if data_source == "Upload CSV File":
        uploaded_file = st.sidebar.file_uploader(
            "Choose a CSV file", type="csv"
        )
    
    # Pipeline execution buttons
    st.sidebar.subheader("🚀 Execute Pipeline")
    
    if st.sidebar.button("🎲 Extract Data", type="primary"):
        with st.spinner("Extracting data..."):
            pipeline.extract_data(uploaded_file)
            st.sidebar.success("Data extraction complete!")
    
    if st.sidebar.button("🔧 Transform Data", disabled=pipeline.raw_data is None):
        with st.spinner("Transforming data..."):
            time.sleep(1)  # Simulate processing time
            pipeline.transform_data()
            st.sidebar.success("Data transformation complete!")
    
    if st.sidebar.button("📊 Load & Analyze", disabled=pipeline.processed_data is None):
        with st.spinner("Loading and analyzing data..."):
            time.sleep(1)  # Simulate processing time
            pipeline.load_and_analyze()
            st.sidebar.success("Analysis complete!")
    
    # Reset button
    if st.sidebar.button("🔄 Reset Pipeline"):
        st.session_state.etl_pipeline = ETLPipeline()
        st.sidebar.success("Pipeline reset!")
        st.experimental_rerun()
    
    # Main content area
    col1, col2, col3 = st.columns(3)
    
    # Extract Phase
    with col1:
        st.subheader("1️⃣ Extract")
        if pipeline.raw_data is not None:
            st.success(f"✅ {len(pipeline.raw_data)} records loaded")
            with st.expander("View Raw Data Sample"):
                st.dataframe(pipeline.raw_data.head())
        else:
            st.info("📥 No data loaded yet")
    
    # Transform Phase
    with col2:
        st.subheader("2️⃣ Transform")
        if pipeline.processed_data is not None:
            st.success(f"✅ Data transformed successfully")
            
            # Data quality metrics
            report = pipeline.data_quality_report
            st.metric("Data Quality Score", f"{report['data_quality_score']}%")
            st.metric("Clean Records", f"{report['clean_records']:,}")
            st.metric("Error Records", f"{report['error_records']:,}")
            
        elif pipeline.raw_data is not None:
            st.warning("⏳ Ready for transformation")
        else:
            st.info("⏸️ Waiting for data extraction")
    
    # Load Phase
    with col3:
        st.subheader("3️⃣ Load")
        if pipeline.processed_data is not None:
            st.success("✅ Ready for analysis")
            
            if len(pipeline.processing_logs) > 0:
                with st.expander("View Processing Logs"):
                    for log in pipeline.processing_logs[-10:]:  # Show last 10 logs
                        st.text(log)
        else:
            st.info("⏸️ Waiting for data transformation")
    
    # Analytics Dashboard
    if pipeline.processed_data is not None:
        st.markdown("---")
        st.header("📊 Analytics Dashboard")
        
        df = pipeline.processed_data
        
        # Key Metrics Row
        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
        
        with metric_col1:
            st.metric("Total Revenue", f"${df['revenue'].sum():,.2f}")
        with metric_col2:
            st.metric("Total Orders", f"{len(df):,}")
        with metric_col3:
            st.metric("Avg Order Value", f"${df['revenue'].mean():.2f}")
        with metric_col4:
            st.metric("Unique Customers", f"{df['customer_id'].nunique():,}")
        
        # Charts Row
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            # Revenue by Category
            category_revenue = df.groupby('category')['revenue'].sum().reset_index()
            fig_category = px.pie(
                category_revenue, 
                values='revenue', 
                names='category',
                title="Revenue by Category"
            )
            st.plotly_chart(fig_category, use_container_width=True)
        
        with chart_col2:
            # Monthly Revenue Trend
            monthly_data = df.groupby(df['order_date'].dt.to_period('M'))['revenue'].sum().reset_index()
            monthly_data['order_date'] = monthly_data['order_date'].astype(str)
            
            fig_trend = px.line(
                monthly_data, 
                x='order_date', 
                y='revenue',
                title="Monthly Revenue Trend"
            )
            st.plotly_chart(fig_trend, use_container_width=True)
        
        # Regional Analysis
        st.subheader("🗺️ Regional Performance")
        region_metrics = df.groupby('region').agg({
            'revenue': 'sum',
            'order_id': 'count',
            'customer_id': 'nunique'
        }).reset_index()
        region_metrics.columns = ['Region', 'Total Revenue', 'Total Orders', 'Unique Customers']
        region_metrics['Avg Order Value'] = region_metrics['Total Revenue'] / region_metrics['Total Orders']
        
        st.dataframe(region_metrics.style.format({
            'Total Revenue': '${:,.2f}',
            'Avg Order Value': '${:,.2f}'
        }))
        
        # Data Quality Report
        st.subheader("🔍 Data Quality Report")
        quality_col1, quality_col2 = st.columns(2)
        
        with quality_col1:
            # Quality score gauge
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=pipeline.data_quality_report['data_quality_score'],
                domain={'x': [0, 1], 'y': [0, 1]},
                title={'text': "Data Quality Score"},
                gauge={
                    'axis': {'range': [None, 100]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, 50], 'color': "lightgray"},
                        {'range': [50, 80], 'color': "yellow"},
                        {'range': [80, 100], 'color': "green"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 90
                    }
                }
            ))
            st.plotly_chart(fig_gauge, use_container_width=True)
        
        with quality_col2:
            st.write("**Quality Issues Found:**")
            if pipeline.data_quality_report['quality_issues']:
                for issue in pipeline.data_quality_report['quality_issues']:
                    st.write(f"• {issue}")
            else:
                st.write("✅ No data quality issues detected")
            
            st.write("**Summary:**")
            st.write(f"• Total Records: {pipeline.data_quality_report['total_records']:,}")
            st.write(f"• Clean Records: {pipeline.data_quality_report['clean_records']:,}")
            st.write(f"• Error Records: {pipeline.data_quality_report['error_records']:,}")

if __name__ == "__main__":
    main()