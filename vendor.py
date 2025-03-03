import streamlit as st
import snowflake.connector
import pandas as pd

# Set page configuration
st.set_page_config(
    page_title="Vendor Tickets Dashboard",
    page_icon="📋",
    layout="wide",
)

# Get the current URL path and vendor name
current_path = st.query_params.get("path", [""])[0]
vendor_name = None

# Extract vendor name from path if it exists
if current_path and current_path != "/":
    # Remove leading/trailing slashes and get the last segment
    path_parts = current_path.strip("/").split("/")
    if path_parts:
        vendor_name = path_parts[-1]

# If no vendor in path, check query params
if not vendor_name:
    vendor_name = st.query_params.get("vendor", None)

# Show current filter if applied
if vendor_name:
    pass

# Custom CSS for right-aligned image with specific size
st.markdown("""
<style>
    .right-aligned-image {
        display: flex;
        justify-content: flex-end;
        margin-bottom: 20px;
    }
    .right-aligned-image img {
        width: 100px;
        height: auto;
    }
    .dataframe-container {
        padding: 0;
        border-radius: 5px;
        background-color: #f8f9fa;
        margin-top: 20px;
        margin-bottom: 20px;
        overflow-x: auto;
        overflow-y: auto;
        max-width: 100%;
        max-height: 600px;
    }
    .header-container {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 20px;
    }
    .vendor-header {
        display: flex;
        align-items: center;
        gap: 10px;
    }
    .vendor-logo {
        width: 30px;
        height: 30px;
        object-fit: contain;
    }
    table {
        width: 100%;
        border-collapse: collapse;
        table-layout: auto;
        margin: 0;
        padding: 0;
        border-radius: 5px;
        overflow: hidden;
    }
    th {
        background-color: #f1f1f1;
        font-weight: bold;
        text-align: left !important;
        padding: 4px 8px;
        border: 1px solid #ddd;
        white-space: nowrap;
        min-width: 120px;
    }
    .dataframe th {
        text-align: left !important;
    }
    td {
        padding: 4px 8px;
        border: 1px solid #ddd;
        min-width: 120px;
    }
    /* Column-specific widths */
    th:nth-child(1), td:nth-child(1) { min-width: 100px; } /* Type */
    th:nth-child(2), td:nth-child(2) { min-width: 150px; } /* Property */
    th:nth-child(3), td:nth-child(3) { min-width: 200px; } /* Name */
    th:nth-child(4), td:nth-child(4) { min-width: 250px; } /* Scope */
    th:nth-child(5), td:nth-child(5) { min-width: 100px; } /* Due Date */
    th:nth-child(6), td:nth-child(6) { min-width: 150px; } /* File URL */
    th:nth-child(7), td:nth-child(7) { min-width: 150px; } /* Directions */
    th:nth-child(8), td:nth-child(8) { min-width: 150px; } /* Pricing */
    
    tr:nth-child(even) {
        background-color: #ffffff;
    }
    tr:hover {
        background-color: #f1f1f1;
    }
    /* Remove extra space from pandas dataframe */
    .dataframe {
        margin-bottom: 0 !important;
    }
    /* Style for the first row to match container border radius */
    tr:first-child th:first-child {
        border-top-left-radius: 5px;
    }
    tr:first-child th:last-child {
        border-top-right-radius: 5px;
    }
    /* Style for the last row to match container border radius */
    tr:last-child td:first-child {
        border-bottom-left-radius: 5px;
    }
    tr:last-child td:last-child {
        border-bottom-right-radius: 5px;
    }
    /* Style hyperlinks */
    .dataframe a {
        color: #0099D6;
        text-decoration: none;
    }
    .dataframe a:hover {
        text-decoration: underline;
    }
</style>
""", unsafe_allow_html=True)

# Get query parameters
query_params = st.query_params.to_dict()
vendor_name = query_params.get("vendor", None)

# Show current filter if applied
if vendor_name:
    pass

# Connect to Snowflake
def create_session():
    try:
        # Connection parameters from secrets.toml
        conn = snowflake.connector.connect(
            account=st.secrets["snowflake"]["account"],
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            role=st.secrets["snowflake"]["role"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database=st.secrets["snowflake"]["database"],
            schema=st.secrets["snowflake"]["schema"]
        )
        
        if conn:
            pass
        
        return conn
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return None

# Create Snowflake connection
conn = create_session()

if conn:
    # Get vendor logo if vendor is specified
    vendor_logo_url = None
    if vendor_name:
        try:
            logo_query = f"""
            SELECT DISTINCT VENDOR_LOGO_URL 
            FROM PRODUCTION.COMPANY.TICKETS 
            WHERE VENDOR_NAME = '{vendor_name}' 
            AND VENDOR_LOGO_URL IS NOT NULL 
            LIMIT 1
            """
            cursor = conn.cursor()
            cursor.execute(logo_query)
            result = cursor.fetchone()
            if result and result[0]:
                vendor_logo_url = result[0]
            cursor.close()
        except Exception as e:
            st.warning(f"Could not fetch vendor logo: {e}")
    
    # Base query
    base_query = """
    SELECT 
        TYPE, 
        PROPERTY_NAME, 
        SUBJECT AS NAME, 
        TICKET_SCOPE, 
        TICKET_SCOPE_FILE_URL, 
        TICKET_SCOPE_MASTER_FILE_NAME,
        ASSET_TICKET_DUE_DATE::DATE AS ASSET_TICKET_DUE_DATE, 
        GOOGLE_MAPS_URL, 
        CONFIRMED_TICKET_AMOUNT, 
        VENDOR_LOGO_URL,
        VENDOR_NAME,
        PRICING_AND_PAYMENT_MODEL, 
        CONCAT('https://welcome.bcstonehomes.com/vendor-bids?ticket_id=', TICKET_ID) AS URL,
        LOWER(REGEXP_REPLACE(VENDOR_NAME, '[^a-zA-Z0-9]', '-')) AS VENDOR_URL_NAME,
        CASE
            WHEN PRICING_AND_PAYMENT_MODEL = 'Fixed Cost - Vendor Confirmed' THEN CONFIRMED_TICKET_AMOUNT::STRING
            WHEN PRICING_AND_PAYMENT_MODEL = 'Fixed Cost - Vendor NOT Confirmed' THEN 'Need Pricing - Click Here'
            WHEN PRICING_AND_PAYMENT_MODEL = 'Open Ended' OR PRICING_AND_PAYMENT_MODEL = 'Schedule Pay' THEN 'Submit Pricing after Work Completed'
            ELSE ''
        END AS URL_TEXT
    FROM PRODUCTION.COMPANY.TICKETS
    WHERE STATUS NOT IN ('184338463', '230160731', '230030964', '184367022') AND ASSET_TICKET_DUE_DATE IS NOT NULL
    """
    
    # Add vendor filter if provided in query params
    if vendor_name:
        query = base_query + f" AND LOWER(REGEXP_REPLACE(VENDOR_NAME, '[^a-zA-Z0-9]', '-')) = '{vendor_name.lower()}'"
    else:
        query = base_query
    
    try:
        # Execute the query and get the results as a DataFrame
        cursor = conn.cursor()
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
        cursor.close()
        
        # Create a container for the dataframe and logo
        with st.container(border=True, height=None):
            # Create a header with title and logo
            col1, col2 = st.columns([3, 1])
            with col1:
                # Display vendor name in header if filtered, otherwise show "All Vendor Tickets"
                if vendor_name:
                    # Get the first row's vendor name and logo URL
                    vendor_info = df.iloc[0] if not df.empty else None
                    if vendor_info is not None:
                        vendor_display_name = vendor_info['VENDOR_NAME']
                        vendor_logo_url = vendor_info['VENDOR_LOGO_URL']
                        
                        # If vendor logo is available, display it next to the vendor name
                        if pd.notna(vendor_logo_url) and vendor_logo_url:
                            st.markdown(f"""
                            <div class="vendor-header">
                                <img src="{vendor_logo_url}" class="vendor-logo" alt="{vendor_display_name} logo">
                                <h3>{vendor_display_name} Tickets</h3>
                            </div>
                            """, unsafe_allow_html=True)
                            st.markdown(f'<p style="font-size: 1.2em; margin-top: -10px;"><strong>{len(df)}</strong> <span style="font-size: 0.7em; color:#757575; vertical-align: middle;">Open Tickets</span></p>', unsafe_allow_html=True)
                        else:
                            st.subheader(f"{vendor_display_name} Tickets")
                            st.markdown(f'<p style="font-size: 1.2em; margin-top: -10px;"><strong>{len(df)}</strong> <span style="font-size: 0.7em; color:#757575; vertical-align: middle;">Open Tickets</span></p>', unsafe_allow_html=True)
                else:
                    st.subheader("All Vendor Tickets")
                    st.markdown(f'<p style="font-size: 1.2em; margin-top: -10px;"><strong>{len(df)}</strong> <span style="font-size: 0.7em; color:#757575; vertical-align: middle;">Open Tickets</span></p>', unsafe_allow_html=True)
            with col2:
                st.markdown("""
                <div class="right-aligned-image">
                    <img src="https://i.ibb.co/Y4ZXb53Y/BC-Stone-Homes-Your-Land-or-Ours-Logo-1.png" alt="BC Stone Homes Logo">
                </div>
                """, unsafe_allow_html=True)
            
            # Add filters in 3 columns
            filter_col1, filter_col2, filter_col3 = st.columns(3)
            
            with filter_col1:
                # Filter for Type
                type_options = sorted(df['TYPE'].dropna().unique().tolist())
                selected_types = st.multiselect('Type', options=type_options, key='type_filter')
            
            with filter_col2:
                # Filter for Property Name
                property_options = sorted(df['PROPERTY_NAME'].dropna().unique().tolist())
                selected_properties = st.multiselect('Property Name', options=property_options, key='property_filter')
            
            with filter_col3:
                # Filter for Due Date
                # Convert to datetime for proper sorting if it's not already
                if 'ASSET_TICKET_DUE_DATE' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['ASSET_TICKET_DUE_DATE']):
                    df['ASSET_TICKET_DUE_DATE'] = pd.to_datetime(df['ASSET_TICKET_DUE_DATE'], errors='coerce')
                
                # Format dates to remove timestamp - convert to string with just the date part
                df['DUE_DATE_DISPLAY'] = df['ASSET_TICKET_DUE_DATE'].dt.date
                due_date_options = sorted(df['DUE_DATE_DISPLAY'].dropna().unique().tolist())
                selected_due_dates = st.multiselect('Due Date', options=due_date_options, key='due_date_filter')
            
            # Apply filters to dataframe
            filtered_df = df.copy()
            
            if selected_types:
                filtered_df = filtered_df[filtered_df['TYPE'].isin(selected_types)]
            
            if selected_properties:
                filtered_df = filtered_df[filtered_df['PROPERTY_NAME'].isin(selected_properties)]
            
            if selected_due_dates:
                filtered_df = filtered_df[filtered_df['DUE_DATE_DISPLAY'].isin(selected_due_dates)]
            
            # Format the display dataframe to show dates without timestamps
            display_df = filtered_df.copy()
            # Replace the original date column with the formatted one for display
            if 'ASSET_TICKET_DUE_DATE' in display_df.columns:
                display_df['ASSET_TICKET_DUE_DATE'] = display_df['DUE_DATE_DISPLAY']
            
            # Create a new column with HTML links for pricing
            display_df['PRICING'] = display_df.apply(
                lambda row: f'<a href="{row["URL"]}" target="_blank">{row["URL_TEXT"]}</a>' 
                if pd.notna(row['URL']) else "", 
                axis=1
            )
            
            # Create a new column with HTML links for Google Maps
            display_df['DIRECTIONS'] = display_df.apply(
                lambda row: f'<a href="{row["GOOGLE_MAPS_URL"]}" target="_blank">Get Directions</a>' 
                if pd.notna(row['GOOGLE_MAPS_URL']) else "", 
                axis=1
            )
            
            # Create a new column with HTML links for File URL
            display_df['FILE_URL'] = display_df.apply(
                lambda row: f'<a href="{row["TICKET_SCOPE_FILE_URL"]}" target="_blank">{row["TICKET_SCOPE_MASTER_FILE_NAME"]}</a>' 
                if pd.notna(row['TICKET_SCOPE_FILE_URL']) else "", 
                axis=1
            )
            
            # Select and rename columns for display
            display_columns = {
                'TYPE': 'Type',
                'PROPERTY_NAME': 'Property',
                'NAME': 'Name',
                'TICKET_SCOPE': 'Scope',
                'ASSET_TICKET_DUE_DATE': 'Due Date',
                'FILE_URL': 'File URL',
                'DIRECTIONS': 'Directions',
                'PRICING': 'Pricing'
            }
            
            html_df = display_df[display_columns.keys()].rename(columns=display_columns)
            
            # Generate HTML table with links
            html = html_df.to_html(escape=False, index=False)
            
            # Display the HTML table
            st.markdown(f'<div class="dataframe-container">{html}</div>', unsafe_allow_html=True)
            
            # Show record count
            st.toast(f"Found {len(filtered_df)} records", icon="📊")
        
    except Exception as e:
        st.error(f"Error executing query: {e}")
    finally:
        # Close the connection
        if conn:
            conn.close()
else:
    st.error("Failed to establish connection to database.")
