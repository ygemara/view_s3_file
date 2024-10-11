import streamlit as st
import boto3
import pandas as pd
from datetime import datetime, timedelta
import re
from botocore.exceptions import ClientError
import io

if 'analysis' not in st.session_state:
    st.session_state.analysis = None

def get_s3_client(access_key, secret_key):
    return boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

def parse_s3_path(s3_path):
    parts = s3_path.replace("s3://", "").split("/")
    bucket = parts[0]
    prefix = "/".join(parts[1:])
    return bucket, prefix

@st.cache_data
def list_partitions(_s3_client, bucket, prefix):
    #st.write("Listing partitions...")
    paginator = _s3_client.get_paginator('list_objects_v2')
    day_pattern = re.compile(r'date=(\d{4}-\d{2}-\d{2})')
    month_pattern = re.compile(r'yearmonth=(\d{4}-\d{2})')
    dates = set()
    
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    day_match = day_pattern.search(key)
                    month_match = month_pattern.search(key)
                    if day_match:
                        dates.add(('day', day_match.group(1)))
                    elif month_match:
                        dates.add(('month', month_match.group(1)))
    except ClientError as e:
        st.error(f"Error accessing S3: {str(e)}")
        return None
    
    #st.write(f"Found {len(dates)} unique partitions.")
    return sorted(list(dates))

def get_file_extension(file_key):
    lower_key = file_key.lower()
    if lower_key.endswith('.csv'):
        return 'csv'
    elif lower_key.endswith('.parquet'):
        return 'parquet'
    elif lower_key.endswith('.json'):
        return 'json'
    return None

@st.cache_data
def get_sample_data(_s3_client, bucket, prefix):
    st.write("Searching for sample data...")
    try:
        response = _s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=100)
        
        if 'Contents' not in response:
            st.write("No objects found in the specified path.")
            return None, None, None
        
        for obj in response['Contents']:
            file_key = obj['Key']
            file_type = get_file_extension(file_key)
            
            if file_type:
                #st.write(f"Found compatible file: {file_key}")
                try:
                    obj_data = _s3_client.get_object(Bucket=bucket, Key=file_key)
                    if file_type == 'csv':
                        df = pd.read_csv(obj_data['Body'], nrows=10)
                    elif file_type == 'parquet':
                        df = pd.read_parquet(io.BytesIO(obj_data['Body'].read())).head(10)
                    elif file_type == 'json':
                        df = pd.read_json(io.BytesIO(obj_data['Body'].read()))
                    
                    st.write(f"Successfully read {file_type.upper()} file.")
                    return df.head(), file_type, file_key
                except Exception as e:
                    st.write(f"Error reading file {file_key}: {str(e)}")
                    continue
        
        st.write("No compatible files found or all files failed to read.")
        return None, None, None
    except ClientError as e:
        st.error(f"Error accessing S3: {str(e)}")
        return None, None, None

def analyze_partitions(partitions):
    day_partitions = [date for type, date in partitions if type == 'day']
    month_partitions = [date for type, date in partitions if type == 'month']
    
    result = {
        "partition_type": "none",
        "total_partitions": len(partitions)
    }
    
    if day_partitions:
        result["partition_type"] = "day"
        day_objects = [datetime.strptime(d, '%Y-%m-%d') for d in day_partitions]
        result["min_date"] = min(day_objects).strftime('%Y-%m-%d')
        result["max_date"] = max(day_objects).strftime('%Y-%m-%d')
        
        start_date, end_date = min(day_objects), max(day_objects)
        all_days = set(start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1))
        existing_days = set(day_objects)
        missing_days = sorted(list(all_days - existing_days))
        result["missing_dates"] = [d.strftime('%Y-%m-%d') for d in missing_days]
    
    elif month_partitions:
        result["partition_type"] = "month"
        month_objects = [datetime.strptime(d, '%Y-%m') for d in month_partitions]
        result["min_date"] = min(month_objects).strftime('%Y-%m')
        result["max_date"] = max(month_objects).strftime('%Y-%m')
        
        start_month, end_month = min(month_objects), max(month_objects)
        all_months = set((start_month + timedelta(days=32 * x)).replace(day=1) for x in range((end_month.year - start_month.year) * 12 + end_month.month - start_month.month + 1))
        existing_months = set(month_objects)
        missing_months = sorted(list(all_months - existing_months))
        result["missing_dates"] = [d.strftime('%Y-%m') for d in missing_months]
    
    return result

def main():
    st.title("S3 Data Analyzer")
    
    with st.sidebar:
        st.header("Configuration")
        access_key = st.text_input("AWS Access Key", type="password")
        secret_key = st.text_input("AWS Secret Key", type="password")
        s3_path = st.text_input("S3 Path (s3://bucket-name/path/to/data)")
        
        analyze_button = st.button("Analyze", type="primary")
    
    if analyze_button and access_key and secret_key and s3_path:
        s3_client = get_s3_client(access_key, secret_key)
        bucket, prefix = parse_s3_path(s3_path)
        
        with st.spinner("Analyzing S3 data..."):
            partitions = list_partitions(s3_client, bucket, prefix)
            
            if partitions:
                st.session_state.analysis = analyze_partitions(partitions)
                st.session_state.s3_client = s3_client
                st.session_state.bucket = bucket
                st.session_state.prefix = prefix
    
    if hasattr(st.session_state, 'analysis') and st.session_state.analysis:
        analysis = st.session_state.analysis
        
        if analysis['partition_type'] == 'none':
            st.warning("⚠️ No date partitions found in the specified path")
            return
        
        # Partition type-specific display
        partition_type = "Daily" if analysis['partition_type'] == 'day' else "Monthly"
        date_format = "%Y-%m-%d" if analysis['partition_type'] == 'day' else "%Y-%m"
        
        st.header(f"{partition_type} Partition Analysis")
        
        # Overview metric
        st.metric("Total Partitions", analysis['total_partitions'])
        
        # Date range
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"First {analysis['partition_type']}: {analysis['min_date']}")
        with col2:
            st.info(f"Last {analysis['partition_type']}: {analysis['max_date']}")
        
        # Missing dates analysis
        if analysis['missing_dates']:
            missing_count = len(analysis['missing_dates'])
            st.warning(f"Missing {analysis['partition_type']}s: {missing_count}")
            if st.expander(f"Show missing {analysis['partition_type']}s"):
                missing_df = pd.DataFrame(analysis['missing_dates'], columns=['Date'])
                st.dataframe(missing_df, hide_index=True)
        else:
            st.success(f"✅ No missing {analysis['partition_type']}s!")
        
        # Sample data
        st.divider()
        sample_data, file_type, file_key = get_sample_data(
            st.session_state.s3_client,
            st.session_state.bucket,
            st.session_state.prefix
        )
        
        if sample_data is not None:
            st.header("Sample Data Preview")
            st.caption(f"Source: `{file_key}`")
            
            file_type_col, dimensions_col = st.columns(2)
            with file_type_col:
                st.info(f"File type: {file_type.upper()}")
            # with dimensions_col:
                # st.info(f"Dimensions: {sample_data.shape[0]} rows × {sample_data.shape[1]} columns")
            
            st.dataframe(sample_data, use_container_width=True)
        else:
            st.warning("⚠️ No CSV, Parquet, or JSON files found in the specified path")

if __name__ == "__main__":
    main()
