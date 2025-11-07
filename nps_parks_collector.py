"""
National Parks Data Collector for Google Colab
================================================
This script collects data from the National Parks Service API and writes it to Google Sheets.

SETUP INSTRUCTIONS:
1. Open this file in Google Colab
2. Paste your NPS API key in the API_KEY variable below
3. Paste your Google Sheet URL in the SHEET_URL variable below
4. Run all cells

Get your NPS API key at: https://www.nps.gov/subjects/developer/get-started.htm
"""

# ============================================================================
# IMPORTS
# ============================================================================
import requests
import pandas as pd
import gspread
from google.colab import auth
from google.auth import default

# ============================================================================
# CONFIGURATION - PASTE YOUR VALUES HERE
# ============================================================================

# PASTE YOUR NPS API KEY HERE (between the quotes)
API_KEY = "YOUR_API_KEY_HERE"

# PASTE YOUR GOOGLE SHEET URL HERE (between the quotes)
# Example: "https://docs.google.com/spreadsheets/d/1abc123def456/edit"
SHEET_URL = "YOUR_SHEET_URL_HERE"

# ============================================================================
# CONSTANTS
# ============================================================================
NPS_API_BASE_URL = "https://developer.nps.gov/api/v1/parks"
MAX_PARKS = 50

# ============================================================================
# GOOGLE AUTHENTICATION
# ============================================================================
def authenticate_google():
    """Authenticate with Google using Colab authentication."""
    print("Authenticating with Google...")
    auth.authenticate_user()
    creds, _ = default()
    gc = gspread.authorize(creds)
    print("✓ Successfully authenticated with Google")
    return gc

# ============================================================================
# NPS API DATA COLLECTION
# ============================================================================
def fetch_parks_data(api_key, limit=50):
    """
    Fetch parks data from the NPS API.

    Args:
        api_key (str): Your NPS API key
        limit (int): Maximum number of parks to retrieve (default: 50)

    Returns:
        list: List of park dictionaries, or None if request fails
    """
    print(f"\nFetching data from NPS API (limit: {limit} parks)...")

    # Build API request parameters
    params = {
        'api_key': api_key,
        'limit': limit
    }

    try:
        # Make API request
        response = requests.get(NPS_API_BASE_URL, params=params)

        # Check if request was successful
        if response.status_code == 200:
            data = response.json()
            parks = data.get('data', [])
            print(f"✓ Successfully retrieved {len(parks)} parks")
            return parks
        else:
            print(f"✗ API request failed with status code: {response.status_code}")
            print(f"  Response: {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"✗ Error making API request: {e}")
        return None

# ============================================================================
# DATA PROCESSING
# ============================================================================
def process_parks_data(parks):
    """
    Process raw parks data into a structured format.

    Args:
        parks (list): List of park dictionaries from API

    Returns:
        pandas.DataFrame: Processed parks data
    """
    print("\nProcessing parks data...")

    processed_data = []

    for park in parks:
        try:
            # Extract required fields, handling missing data gracefully
            park_info = {
                'Full Name': park.get('fullName', 'N/A'),
                'States': park.get('states', 'N/A'),
                'Description': park.get('description', 'N/A'),
                'Acres': park.get('acres', 'N/A'),
                'Designation': park.get('designation', 'N/A')
            }
            processed_data.append(park_info)

        except Exception as e:
            print(f"  Warning: Error processing park {park.get('fullName', 'Unknown')}: {e}")
            continue

    # Create DataFrame
    df = pd.DataFrame(processed_data)
    print(f"✓ Successfully processed {len(df)} parks")

    return df

# ============================================================================
# GOOGLE SHEETS EXPORT
# ============================================================================
def write_to_google_sheet(gc, sheet_url, dataframe):
    """
    Write DataFrame to Google Sheets.

    Args:
        gc: Authenticated gspread client
        sheet_url (str): Google Sheet URL
        dataframe (pandas.DataFrame): Data to write

    Returns:
        bool: True if successful, False otherwise
    """
    print("\nWriting data to Google Sheets...")

    try:
        # Open the Google Sheet
        spreadsheet = gc.open_by_url(sheet_url)

        # Get the first worksheet (or create one if needed)
        try:
            worksheet = spreadsheet.sheet1
        except:
            worksheet = spreadsheet.add_worksheet(title="NPS Parks Data", rows="100", cols="20")

        # Clear existing data
        worksheet.clear()

        # Write DataFrame to sheet
        # Convert DataFrame to list of lists (including header)
        data_to_write = [dataframe.columns.tolist()] + dataframe.values.tolist()

        # Update the worksheet
        worksheet.update('A1', data_to_write)

        print(f"✓ Successfully wrote {len(dataframe)} rows to Google Sheet")
        print(f"  Sheet URL: {sheet_url}")
        return True

    except Exception as e:
        print(f"✗ Error writing to Google Sheet: {e}")
        return False

# ============================================================================
# MAIN EXECUTION
# ============================================================================
def main():
    """Main execution function."""
    print("=" * 70)
    print("NATIONAL PARKS DATA COLLECTOR")
    print("=" * 70)

    # Validate configuration
    if API_KEY == "YOUR_API_KEY_HERE":
        print("\n✗ ERROR: Please paste your NPS API key in the API_KEY variable")
        return

    if SHEET_URL == "YOUR_SHEET_URL_HERE":
        print("\n✗ ERROR: Please paste your Google Sheet URL in the SHEET_URL variable")
        return

    # Step 1: Authenticate with Google
    try:
        gc = authenticate_google()
    except Exception as e:
        print(f"\n✗ Authentication failed: {e}")
        return

    # Step 2: Fetch parks data from NPS API
    parks_data = fetch_parks_data(API_KEY, limit=MAX_PARKS)

    if parks_data is None or len(parks_data) == 0:
        print("\n✗ No parks data retrieved. Please check your API key and try again.")
        return

    # Step 3: Process the data
    df = process_parks_data(parks_data)

    if df.empty:
        print("\n✗ No data to write. Processing failed.")
        return

    # Step 4: Display preview
    print("\n" + "=" * 70)
    print("DATA PREVIEW (first 5 rows):")
    print("=" * 70)
    print(df.head())

    # Step 5: Write to Google Sheets
    success = write_to_google_sheet(gc, SHEET_URL, df)

    # Final status
    print("\n" + "=" * 70)
    if success:
        print("✓ SUCCESS! Data collection complete.")
    else:
        print("✗ FAILED. Please check the errors above.")
    print("=" * 70)

# ============================================================================
# RUN THE SCRIPT
# ============================================================================
if __name__ == "__main__":
    main()
