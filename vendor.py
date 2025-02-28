import streamlit as st
import snowflake.connector
import pandas as pd

# Set page title
st.title("Vendors Dashboard")

# Get query parameters
query_params = st.query_params.to_dict()
vendor_name = query_params.get("vendor", None)

# Show current filter if applied
if vendor_name:
    st.toast(f"Filtering for vendor: {vendor_name}", icon="🔍")

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
            st.toast("Successfully connected to database!", icon="✅")
        
        return conn
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return None

# Create Snowflake connection
conn = create_session()

if conn:
    # Base query
    base_query = """
    SELECT 
        TYPE, 
        PROPERTY_NAME, 
        SUBJECT AS NAME, 
        TICKET_SCOPE, 
        TICKET_SCOPE_FILE_URL, 
        ASSET_TICKET_DUE_DATE, 
        GOOGLE_MAPS_URL, 
        CONFIRMED_TICKET_AMOUNT, 
        PRICING_AND_PAYMENT_MODEL, 
        CONCAT('https://welcome.bcstonehomes.com/vendor-bids?ticket_id=', TICKET_ID) AS URL, 
        CASE
            WHEN PRICING_AND_PAYMENT_MODEL = 'Fixed Cost - Vendor Confirmed' THEN CONFIRMED_TICKET_AMOUNT::STRING
            WHEN PRICING_AND_PAYMENT_MODEL = 'Fixed Cost - Vendor NOT Confirmed' THEN 'Need Pricing - Click Here'
            WHEN PRICING_AND_PAYMENT_MODEL = 'Open Ended' OR PRICING_AND_PAYMENT_MODEL = 'Schedule Pay' THEN 'Submit Pricing after Work Completed'
            ELSE ''
        END AS URL_TEXT
    FROM PRODUCTION.COMPANY.TICKETS
    WHERE STATUS NOT IN ('184338463', '230160731', '230030964', '184367022')
    """
    
    # Add vendor filter if provided in query params
    if vendor_name:
        query = base_query + f" AND VENDOR_NAME = '{vendor_name}'"
    else:
        query = base_query
    
    try:
        # Execute the query and get the results as a DataFrame
        cursor = conn.cursor()
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
        cursor.close()
        
        # Display the DataFrame
        st.dataframe(df)
        
        # Show record count
        st.toast(f"Found {len(df)} records", icon="📊")
        
    except Exception as e:
        st.error(f"Error executing query: {e}")
    finally:
        # Close the connection
        if conn:
            conn.close()
else:
    st.error("Failed to establish connection to Snowflake.")
