# Property Skip Trace Tool

This Streamlit application helps find missing contact information (phone numbers and emails) for property owners using the Apify API.

## Features

- Upload CSV or Excel files containing property owner information
- Process multiple records in batch
- Search for phone numbers and email addresses
- Export results to Excel format
- Progress tracking for batch processing
- Support for multiple owners per property
- Three operation modes:
  1. Process New Records: Upload and search for new contact information
  2. Fetch Existing Dataset: Retrieve and download results from a previous Apify run using dataset ID
  3. Merge Dataset with File: Combine an uploaded file with existing dataset records

### Search Results Include

- Up to 5 phone numbers with detailed information:
  - Phone number
  - Phone type (Landline/Wireless)
  - Last reported date
  - Service provider
- Up to 5 email addresses
- Additional information:
  - Age
  - Current location
  - Person verification link

## Setup

1. Clone this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `.env` file in the project root and add your Apify API token:
   ```
   APIFY_API_TOKEN=your_token_here
   ```

## Running the Application

```bash
streamlit run app.py
```

## Operation Modes

### 1. Process New Records
- Upload a new file to search for contact information
- Automatically searches for each property owner
- Results are appended to your original file

### 2. Fetch Existing Dataset
- Retrieve complete results from a previous search using dataset ID
- Downloads all available fields from the dataset
- Useful for accessing historical search results

### 3. Merge Dataset with File
- Upload a file and provide a dataset ID
- Matches records based on owner names
- Adds missing contact information from the dataset to your file
- Preserves existing data and only fills in empty fields

## Input File Format

For "Process New Records" and "Merge Dataset with File" modes, the application expects CSV or Excel files with the following columns:

- Owner #1 First Name
- Owner #1 Last Name
- Owner #2 First Name
- Owner #2 Last Name
- Property Address
- Property City
- Property State
- Property Zip
- Additional fields (if any) will be preserved in the output

## Output

The application generates an Excel file containing all original data plus:

- Up to 10 phone numbers (Phone 1 - Phone 10)
- Up to 5 email addresses (Email 1 - Email 5)

## Notes

- Ensure your Apify API token is valid and has sufficient credits
- Large files may take longer to process
- Internet connection is required for API calls
