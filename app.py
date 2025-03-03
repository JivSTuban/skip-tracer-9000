import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv
from apify_client import ApifyClient
from io import BytesIO
import time

def load_api_token():
    """Load and validate the API token from environment variables"""
    # Check if .env file exists
    if not os.path.exists('.env'):
        st.error("""
        No .env file found. Please create one with your APIFY_API_TOKEN:
        1. Copy .env.example to .env
        2. Add your Apify API token from https://console.apify.com/account/integrations
        """)
        st.code("APIFY_API_TOKEN=your_token_here")
        st.stop()
    
    # Load environment variables
    load_dotenv(override=True)
    
    # Check for API token
    token = os.getenv('APIFY_API_TOKEN')
    if not token or token == 'your_token_here':
        st.error("""
        APIFY_API_TOKEN not properly set in .env file.
        Please add your token from https://console.apify.com/account/integrations
        """)
        st.stop()
    
    return token

# Initialize Apify client
client = ApifyClient(load_api_token())

def process_row(row):
    """Process a single row of data to create search queries"""
    address = f"{row['Property Address']}"
    if pd.notna(row['Property City']) and pd.notna(row['Property State']) and pd.notna(row['Property Zip']):
        address += f"; {row['Property City']}, {row['Property State']} {row['Property Zip']}"
    
    return {
        "street_citystatezip": address
    }

def search_records(queries, progress_bar=None):
    """Search for records using Apify API"""
    run_input = {
        "street_citystatezip": [],
    }
    
    # Prepare input data
    for query in queries:
        run_input["street_citystatezip"].append(query["street_citystatezip"])
    
    try:
        # Run the Actor and get run details
        run = client.actor("vmf6h5lxPAkB1W2gT").call(run_input=run_input)
        run_id = run.get("id")
        
        if not run_id:
            st.error("Failed to get run ID from Apify")
            return []

        # Wait for run to complete (with timeout)
        max_wait_time = 120  # 2 minutes timeout
        start_time = time.time()
        
        while True:
            run_info = client.run(run_id).get()
            status = run_info.get("status")
            
            if status == "SUCCEEDED":
                break
            elif status in ["FAILED", "ABORTED", "TIMED-OUT"]:
                st.error(f"Apify run failed with status: {status}")
                return []
            
            # Check timeout
            if time.time() - start_time > max_wait_time:
                st.error("Search timeout reached")
                return []
            
            time.sleep(2)  # Wait before checking again
        
        # Get dataset items
        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            st.error("No dataset ID found in response")
            return []
        
        # Collect results with retry
        max_retries = 3
        for retry in range(max_retries):
            try:
                results = []
                for item in client.dataset(dataset_id).iterate_items():
                    if isinstance(item, dict):  # Validate item structure
                        results.append(item)
                    if progress_bar and len(queries) > 0:
                        progress_bar.progress(min(len(results) / len(queries), 1.0))
                
                if results:
                    return results
                elif retry < max_retries - 1:
                    time.sleep(2)  # Wait before retry
                    st.warning(f"Retry {retry + 1} of {max_retries}...")
                else:
                    st.warning("No results found in dataset")
                    return []
                    
            except Exception as e:
                if retry < max_retries - 1:
                    st.warning(f"Retry {retry + 1} of {max_retries} due to: {str(e)}")
                    time.sleep(2)
                else:
                    st.error(f"Failed to fetch results after {max_retries} attempts: {str(e)}")
                    return []
        
        return []
        
    except Exception as e:
        st.error(f"Error occurred while searching records: {str(e)}")
        return []

def fetch_dataset_records(dataset_id):
    """Fetch records from an existing Apify dataset"""
    try:
        results = []
        for item in client.dataset(dataset_id).iterate_items():
            if isinstance(item, dict):
                results.append(item)
        return results
    except Exception as e:
        st.error(f"Error fetching dataset records: {str(e)}")
        return []

def update_dataframe_with_results(df, results):
    """Update dataframe with search results"""
    # Ensure proper data types for new columns
    for j in range(1, 6):
        # Phone columns
        phone_cols = [
            f'Phone {j}',
            f'Phone {j} Type',
            f'Phone {j} Last Reported',
            f'Phone {j} Provider'
        ]
        for col in phone_cols:
            if col not in df.columns:
                df[col] = pd.Series(dtype='str')
        
        # Email columns
        email_col = f'Email {j}'
        if email_col not in df.columns:
            df[email_col] = pd.Series(dtype='str')
    
    # Additional columns
    additional_fields = ['Age', 'Lives in', 'Person Link']
    for field in additional_fields:
        if field not in df.columns:
            df[field] = pd.Series(dtype='str')

    # Update data
    for i, result in enumerate(results):
        # Extract phones with additional information
        for j in range(1, 6):
            phone_num = result.get(f'Phone {j}')
            if phone_num:
                df.at[i, f'Phone {j}'] = str(phone_num)
                df.at[i, f'Phone {j} Type'] = str(result.get(f'Phone {j} Type', ''))
                df.at[i, f'Phone {j} Last Reported'] = str(result.get(f'Phone {j} Last Reported', ''))
                df.at[i, f'Phone {j} Provider'] = str(result.get(f'Phone {j} Provider', ''))
        
        # Extract emails
        for j in range(1, 6):
            email = result.get(f'Email {j}')
            if email:
                df.at[i, f'Email {j}'] = str(email)
        
        # Add additional information
        for field in additional_fields:
            if field in result:
                df.at[i, field] = str(result[field])
    
    return df

def main():
    st.title("Property Skip Trace Tool")
    st.write("Upload your property list to find missing contact information.")
    
    # Add tabs for different modes
    tab1, tab2, tab3 = st.tabs(["Process New Records", "Fetch Existing Dataset", "Merge Dataset with File"])
    
    with tab1:
        uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=["csv", "xlsx"])
    
        if uploaded_file:
            try:
                # Determine file type and read accordingly
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                st.write("Preview of uploaded data:")
                st.dataframe(df.head())
                
                if st.button("Process Records", key="process_new"):
                    # Process each row
                    queries = []
                    for _, row in df.iterrows():
                        queries.append(process_row(row))
                    
                    # Show progress
                    progress_bar = st.progress(0)
                    st.write("Searching for records...")
                    
                    # Search records
                    results = search_records(queries, progress_bar)
                    
                    if results:
                        # Process results and update dataframe
                        df = update_dataframe_with_results(df, results)
                        
                        # Create Excel file in memory
                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            df.to_excel(writer, index=False)
                        
                        # Offer download
                        st.download_button(
                            label="Download Updated Records",
                            data=output.getvalue(),
                            file_name="updated_records.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                        
                        st.success("Processing complete! Click the button above to download your results.")
                    else:
                        st.warning("No results found or an error occurred during the search.")
                    
            except Exception as e:
                st.error(f"Error processing file: {str(e)}")
    
    with tab2:
        dataset_id = st.text_input(
            "Enter Dataset ID",
            help="Enter the dataset ID from your Apify run (e.g., jUZazpQovcCSuqmug)"
        )
        
        if dataset_id and st.button("Fetch Dataset Records", key="fetch_existing"):
            progress_text = st.empty()
            progress_text.write("Fetching records from dataset...")
            
            results = fetch_dataset_records(dataset_id)
            
            if results:
                # Create new dataframe with all fields from results
                df = pd.DataFrame(results)
                
                # Create Excel file in memory
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False)
                
                # Offer download
                st.download_button(
                    label="Download Dataset Records",
                    data=output.getvalue(),
                    file_name="dataset_records.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                
                st.success(f"Successfully retrieved {len(results)} records! Click the button above to download.")
            else:
                st.error("No records found in the dataset or an error occurred.")

def merge_dataset_with_file(df, dataset_results):
    """Merge dataset results with uploaded file based on name matching"""
    # Ensure proper data types for new columns
    for j in range(1, 6):
        # Phone columns
        phone_cols = [
            f'Phone {j}',
            f'Phone {j} Type',
            f'Phone {j} Last Reported',
            f'Phone {j} Provider'
        ]
        for col in phone_cols:
            if col not in df.columns:
                df[col] = pd.Series(dtype='str')
        
        # Email columns
        email_col = f'Email {j}'
        if email_col not in df.columns:
            df[email_col] = pd.Series(dtype='str')
    
    # Additional columns
    additional_cols = ['Age', 'Lives in', 'Person Link']
    for col in additional_cols:
        if col not in df.columns:
            df[col] = pd.Series(dtype='str')
    
    # Process matches
    for idx, row in df.iterrows():
        # Create search address from the row
        search_address = f"{row['Property Address']}".lower()
        if pd.notna(row['Property City']) and pd.notna(row['Property State']) and pd.notna(row['Property Zip']):
            search_address_full = f"{search_address}; {row['Property City']}, {row['Property State']} {row['Property Zip']}".lower()
        else:
            search_address_full = search_address
        
        # Search for matching record in dataset results
        for result in dataset_results:
            result_address = result.get('Input Given', '').lower()
            if search_address in result_address or search_address_full in result_address:
                # Extract phones with additional information
                for j in range(1, 6):
                    phone_num = result.get(f'Phone {j}')
                    if phone_num and (pd.isna(df.at[idx, f'Phone {j}']) or df.at[idx, f'Phone {j}'] == ''):
                        df.at[idx, f'Phone {j}'] = str(phone_num)
                        df.at[idx, f'Phone {j} Type'] = str(result.get(f'Phone {j} Type', ''))
                        df.at[idx, f'Phone {j} Last Reported'] = str(result.get(f'Phone {j} Last Reported', ''))
                        df.at[idx, f'Phone {j} Provider'] = str(result.get(f'Phone {j} Provider', ''))
                
                # Extract emails
                for j in range(1, 6):
                    email = result.get(f'Email {j}')
                    if email and (pd.isna(df.at[idx, f'Email {j}']) or df.at[idx, f'Email {j}'] == ''):
                        df.at[idx, f'Email {j}'] = str(email)
                
                # Add additional information if not present
                for field in additional_cols:
                    value = result.get(field)
                    if value and (pd.isna(df.at[idx, field]) or df.at[idx, field] == ''):
                        df.at[idx, field] = str(value)
                
                break  # Stop searching once we find a match
    
    return df

def main():
    st.title("Property Skip Trace Tool")
    st.write("Upload your property list to find missing contact information.")
    
    # Add tabs for different modes
    tab1, tab2, tab3 = st.tabs(["Process New Records", "Fetch Existing Dataset", "Merge Dataset with File"])
    
    with tab1:
        uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=["csv", "xlsx"], key="upload1")
    
        if uploaded_file:
            try:
                # Determine file type and read accordingly
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                st.write("Preview of uploaded data:")
                st.dataframe(df.head())
                
                if st.button("Process Records", key="process_new"):
                    # Process each row
                    queries = []
                    for _, row in df.iterrows():
                        queries.append(process_row(row))
                    
                    # Show progress
                    progress_bar = st.progress(0)
                    st.write("Searching for records...")
                    
                    # Search records
                    results = search_records(queries, progress_bar)
                    
                    if results:
                        # Process results and update dataframe
                        df = update_dataframe_with_results(df, results)
                        
                        # Create Excel file in memory
                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            df.to_excel(writer, index=False)
                        
                        # Offer download
                        st.download_button(
                            label="Download Updated Records",
                            data=output.getvalue(),
                            file_name="updated_records.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                        
                        st.success("Processing complete! Click the button above to download your results.")
                    else:
                        st.warning("No results found or an error occurred during the search.")
                    
            except Exception as e:
                st.error(f"Error processing file: {str(e)}")
    
    with tab2:
        dataset_id = st.text_input(
            "Enter Dataset ID",
            help="Enter the dataset ID from your Apify run (e.g., jUZazpQovcCSuqmug)",
            key="dataset1"
        )
        
        if dataset_id and st.button("Fetch Dataset Records", key="fetch_existing"):
            progress_text = st.empty()
            progress_text.write("Fetching records from dataset...")
            
            results = fetch_dataset_records(dataset_id)
            
            if results:
                # Create new dataframe with all fields from results
                df = pd.DataFrame(results)
                
                # Create Excel file in memory
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False)
                
                # Offer download
                st.download_button(
                    label="Download Dataset Records",
                    data=output.getvalue(),
                    file_name="dataset_records.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                
                st.success(f"Successfully retrieved {len(results)} records! Click the button above to download.")
            else:
                st.error("No records found in the dataset or an error occurred.")
    
    with tab3:
        st.write("Upload a file and merge it with existing dataset records")
        
        # File upload
        uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=["csv", "xlsx"], key="upload2")
        
        # Dataset ID input
        dataset_id = st.text_input(
            "Enter Dataset ID",
            help="Enter the dataset ID from your Apify run (e.g., jUZazpQovcCSuqmug)",
            key="dataset2"
        )
        
        if uploaded_file and dataset_id:
            try:
                # Read the uploaded file
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                st.write("Preview of uploaded data:")
                st.dataframe(df.head())
                
                if st.button("Merge with Dataset", key="merge"):
                    progress_text = st.empty()
                    progress_text.write("Fetching dataset records...")
                    
                    # Fetch dataset records
                    dataset_results = fetch_dataset_records(dataset_id)
                    
                    if dataset_results:
                        progress_text.write("Merging records...")
                        
                        # Merge dataset results with the uploaded file
                        df = merge_dataset_with_file(df, dataset_results)
                        
                        # Create Excel file in memory
                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            df.to_excel(writer, index=False)
                        
                        # Offer download
                        st.download_button(
                            label="Download Merged Records",
                            data=output.getvalue(),
                            file_name="merged_records.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                        
                        st.success("Merge complete! Click the button above to download your results.")
                    else:
                        st.error("No records found in the dataset or an error occurred.")
                
            except Exception as e:
                st.error(f"Error processing file: {str(e)}")

if __name__ == "__main__":
    main()
