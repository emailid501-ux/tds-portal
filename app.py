import streamlit as st
import pandas as pd
import time
import gspread
import requests
import base64
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# --- Page Configuration ---
st.set_page_config(
    page_title="TDS Management System",
    page_icon="📊",
    layout="wide"
)

# --- Constants ---
ALL_BLOCKS = [
    "DPCU Saran", "Sadar", "Manjhi", "Jalalpur", "Revelganj", 
    "Baniapur", "Ishuapur", "Parsa", "Nagra", "Amnour", 
    "Mashrakh", "Taraiya", "Panapur", "Marhoura"
]

SHEET_ID = "12WotSKSwCgRWPVgVb7zVcfO158Qv6spIFfjyLFOSN7Q"
GAS_UPLOAD_URL = "https://script.google.com/macros/s/AKfycbxpMTPfGF4RwvXbBECfBZsoa0WsHWDUHw9MJbkQxHqZLrvsKgFIixsmoTmb3KeEEXaW/exec"
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# --- Google Connectors ---
@st.cache_resource
def get_google_sheet():
    try:
        import os
        # 1. Prioritize Local JSON (Stable for Localhost)
        if os.path.exists("credentials.json"):
             creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", SCOPE)
        else:
            # 2. Fallback to Streamlit Secrets (Cloud)
            try:
                # DEBUG: Show what keys exist
                # st.write("Available Secrets Keys:", list(st.secrets.keys())) 
                
                if "gcp_service_account" in st.secrets:
                    creds_dict = st.secrets["gcp_service_account"]
                    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
                # Backup: Check if user pasted keys at ROOT level (common mistake)
                elif "type" in st.secrets and "project_id" in st.secrets:
                    st.warning("Found secrets at root level. Converting...")
                    creds_dict = dict(st.secrets)
                    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
                else:
                    st.error(f"Missing 'gcp_service_account' in Secrets. Found keys: {list(st.secrets.keys())}")
                    return None
            except FileNotFoundError:
                st.error("Secrets file not found.")
                return None
            
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID)
        return sheet
    except Exception as e:
        st.error(f"Error connecting to Google Sheets: {e}")
        return None

def upload_file_to_gas(file_obj, filename, block_name, bill_date):
    try:
        # Encode file to Base64
        file_content = file_obj.read()
        file_b64 = base64.b64encode(file_content).decode('utf-8')
        
        payload = {
            "file": file_b64,
            "filename": filename,
            "block": block_name,
            "year": str(bill_date.year),
            "month": bill_date.strftime("%B")
        }
        
        response = requests.post(GAS_UPLOAD_URL, json=payload)
        
        if response.status_code == 200:
            result = response.json()
            if result.get("status") == "success":
                return result.get("link")
            else:
                st.error(f"Upload Error from Script: {result.get('message')}")
                return "Upload Failed"
        else:
            st.error(f"HTTP Error: {response.status_code}")
            return "Upload Failed"
            
    except Exception as e:
        st.error(f"App Upload Error: {e}")
        return "Upload Failed"

def init_sheet_headers(sheet):
    try:
        worksheet = sheet.get_worksheet(0)
        # Check if empty, if so add headers
        if not worksheet.get_all_values():
            headers = [
                "Timestamp", "Project Name", "Block", "Vendor Name", "PAN", "Bill No", "Bill Date", 
                "Payment Head", "Payment Date", "Gross Amount", "Taxable Amount", "GST No",
                "CGST", "SGST", "IGST", "TDS 194C 1%", "TDS 194C 2%", "TDS 194J", "TDS 194I",
                "Total Deduction", "File Link", "Entered By"
            ]
            worksheet.append_row(headers)
        return worksheet
    except Exception as e:
        st.error(f"Error initializing sheet headers: {e}")
        return None

# --- Session State Initialization ---
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'user_role' not in st.session_state:
    st.session_state.user_role = None
if 'username' not in st.session_state:
    st.session_state.username = None
if 'assigned_block' not in st.session_state:
    st.session_state.assigned_block = None

# --- Mock Credentials ---

# --- User Management (Google Sheets) ---

# --- Helper: Retry Logic ---
def with_retry(func, retries=3, delay=1):
    """Wraps a function with automatic retry logic."""
    last_exception = None
    for i in range(retries):
        try:
            return func()
        except Exception as e:
            last_exception = e
            time.sleep(delay)
            delay *= 2
    raise last_exception

# --- User Management (Google Sheets) ---
def init_users_sheet(sheet):
    """Checks if 'Users' tab exists, else creates it with default admin."""
    try:
        def _get_or_create():
            try:
                return sheet.worksheet("Users")
            except gspread.WorksheetNotFound:
                # Create Worksheet
                ws = sheet.add_worksheet(title="Users", rows=100, cols=4)
                # Add Headers & Default Admin
                ws.append_row(["Username", "Password", "Role", "Block"])
                ws.append_row(["admin", "admin123", "Admin", "All"])
                return ws
        
        return with_retry(_get_or_create)
    except Exception as e:
        st.error(f"Error initializing Users sheet: {e}")
        return None

@st.cache_data(ttl=600) # Cache for 10 mins
def fetch_users_dynamic():
    """Fetches users from Google Sheets 'Users' tab."""
    try:
        sheet = get_google_sheet()
        if not sheet:
            return {}
        
        # Ensure sheet exists
        ws = init_users_sheet(sheet)
        if not ws:
            return {}
            
        def _fetch():
            return ws.get_all_records()

        data = with_retry(_fetch)
        
        users_dict = {}
        for row in data:
            u = str(row.get("Username", "")).strip()
            p = str(row.get("Password", "")).strip()
            r = str(row.get("Role", "Block User")).strip()
            b = str(row.get("Block", "")).strip()
            
            if u:
                users_dict[u] = {"password": p, "role": r, "block": b}
        
        return users_dict
    except Exception as e:
        st.error(f"Error fetching users: {e}")
        return {}

# --- Load Users (Dynamic + Fallback) ---
try:
    USERS = fetch_users_dynamic()
    if not USERS:
        USERS = {"admin": {"password": "admin123", "role": "Admin", "block": "All"}}

# --- Dynamic Block Synchronization ---
# Merge hardcoded blocks with any new blocks found in the Users sheet
try:
    if USERS:
        dynamic_blocks = set()
        for u_data in USERS.values():
            blk = u_data.get("block")
            if blk and blk != "All" and blk != "Block":
                dynamic_blocks.add(blk)
        
        # Add to global list if not present
        for d_blk in dynamic_blocks:
            if d_blk not in ALL_BLOCKS:
                ALL_BLOCKS.append(d_blk)
        
        ALL_BLOCKS.sort() # Keep dropdown sorted
except Exception as e:
    print(f"Error syncing blocks: {e}") # Fallback
except Exception:
     USERS = {"admin": {"password": "admin123", "role": "Admin", "block": "All"}}

# --- Light Sky Glass Theme ---
def load_css():
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
        
        /* Global Reset & Font */
        html, body, [class*="css"], [data-testid="stAppViewContainer"] {
            font-family: 'Inter', sans-serif;
            color: #000000 !important; /* Pitch Black Text */
        }
        
        /* Force Labels to be Black */
        .stTextInput label, .stNumberInput label, .stSelectbox label, p, .stMarkdown {
            color: #000000 !important;
            font-weight: 600;
        }

        /* Input Placeholders - Critical Fix */
        ::placeholder {
            color: #4A5568 !important; /* Dark Gray */
            opacity: 1;
        }
        
        /* Main Background - Light Sky Gradient */
        .stApp {
            background: linear-gradient(135deg, #F5F7FA 0%, #C3CFE2 100%);
            background-attachment: fixed;
        }

        /* Glassmorphism Cards (Light) */
        .css-1r6slb0, .css-12oz5g7, .stForm, [data-testid="stForm"] {
            background: rgba(255, 255, 255, 0.70) !important;
            backdrop-filter: blur(16px);
            -webkit-backdrop-filter: blur(16px);
            border: 1px solid rgba(255, 255, 255, 0.4);
            border-radius: 16px;
            box-shadow: 0 4px 30px rgba(0, 0, 0, 0.05); /* Soft Shadow */
            padding: 2rem;
        }
        
        /* Input Fields - White Glass */
        .stTextInput > div > div > input, 
        .stNumberInput > div > div > input,
        .stSelectbox > div > div > div {
            background: rgba(255, 255, 255, 0.95) !important;
            border: 1px solid #718096; /* Darker Border */
            color: #000000 !important;
            border-radius: 8px;
            font-weight: 600;
            caret-color: black;
        }
        .stTextInput > div > div > input:focus,
        .stNumberInput > div > div > input:focus {
            border-color: #3182CE;
            box-shadow: 0 0 0 1px #3182CE;
        }
        
        /* Buttons - Blue Gradient */
        .stButton > button {
            background: linear-gradient(90deg, #3182CE 0%, #63B3ED 100%);
            color: white !important;
            border: none;
            border-radius: 8px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 1px;
            transition: all 0.3s ease;
            box-shadow: 0 4px 15px rgba(49, 130, 206, 0.2);
        }
        .stButton > button:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(49, 130, 206, 0.3);
            color: white !important;
        }
        
        /* Headings & Metrics */
        h1 {
            color: #1A365D !important; /* Dark Navy for Titles */
            font-weight: 800;
        }
        h2, h3 { color: #000000 !important; }
        
        [data-testid="stMetricValue"] {
            font-size: 2.5rem;
            color: #2B6CB0 !important;
            text-shadow: none;
        }
        
        /* Tables (Light) */
        .dataframe {
            background: white;
            color: #000000;
            border-radius: 10px;
        }
        th { background-color: #EBF8FF !important; color: #000000 !important; font-weight: bold; }
        td { color: #000000 !important; }
        
    </style>
    """, unsafe_allow_html=True)

# --- Login Function (Glass Style) ---
def login():
    load_css()
    
    if "username" in st.query_params:
         st.query_params.clear()

    # Floating Centered Card Layout
    _, col2, _ = st.columns([1, 1.5, 1])
    
    with col2:
        st.markdown("<br><br><br>", unsafe_allow_html=True) # Spacer
        
        # Transparent Container directly
        with st.container():
            st.markdown("<div style='text-align: center; margin-bottom: 30px;'><h1>🚀 TDS Portal</h1><p style='color: #A0AEC0; font-size: 14px;'>SECURE WORKSPACE ACCESS</p></div>", unsafe_allow_html=True)
            
            username = st.text_input("Username", placeholder="Enter your ID")
            password = st.text_input("Password", type="password", placeholder="Enter Password")
            
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("ENTER DASHBOARD", use_container_width=True):
                if username in USERS and USERS[username]["password"] == password:
                    st.session_state.logged_in = True
                    st.session_state.username = username
                    st.session_state.user_role = USERS[username]["role"]
                    st.session_state.assigned_block = USERS[username]["block"]
                    st.toast("Access Granted", icon="🔓")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("Access Denied")

# --- Logout Function ---
def logout():
    st.session_state.logged_in = False
    st.session_state.user_role = None
    st.session_state.username = None
    st.session_state.assigned_block = None
    st.rerun()

# --- Main App Logic ---
if not st.session_state.logged_in:
    login()
else:
    load_css() # Ensure Premium CSS is loaded
    # --- Reset Logic (Must be before widgets) ---
    if st.session_state.get("reset_form"):
        # Text Inputs
        for k in ["v_name", "v_pan", "v_bill_no", "v_proj_name", "v_gst"]:
            if k in st.session_state:
                st.session_state[k] = ""
        
        # Number Inputs
        for k in ["v_gross", "v_taxable", "v_cgst", "v_sgst", "v_igst", 
                    "v_194c1", "v_194c2", "v_194j", "v_194i"]:
            if k in st.session_state:
                st.session_state[k] = 0.0
        
        # Date Inputs (Reset to Today)
        for k in ["v_bill_date", "v_pay_date"]:
            if k in st.session_state:
                st.session_state[k] = datetime.now()
        
        # Specific Selectboxes
        if "v_head" in st.session_state:
             st.session_state["v_head"] = "Vehicle Hiring"
        if "v_proj" in st.session_state:
             st.session_state["v_proj"] = "NRLM"

        # File Uploader
        if "v_file" in st.session_state:
            del st.session_state["v_file"]
            
        st.session_state.reset_form = False
        # No rerun needed here as we are already at the top, widgets below will pick up new values

    st.sidebar.title(f"Welcome, {st.session_state.username}")
    st.sidebar.markdown(f"**Role:** {st.session_state.user_role}")
    st.sidebar.markdown(f"**Block:** {st.session_state.assigned_block}")
    
    if st.sidebar.button("Logout"):
        logout()
    
    st.sidebar.markdown("---")
    menu = st.sidebar.radio("Navigation", ["📝 Data Entry", "📊 Reports", "⚙️ Settings"])

    if menu == "📝 Data Entry":
        st.title("📝 Data Entry")
        tab1, tab2 = st.tabs(["Vendor (26Q)", "Salary (24Q)"])
        
        with tab1:
            st.subheader("Vendor Payment Details (26Q)")
            
            # --- Project Selection ---
            st.markdown("### 0. Project Details")
            project_options = ["NRLM", "MMRY", "Mahila Samvad", "SJY", "Other"]
            project = st.selectbox("Select Project", project_options, key="v_proj")
            
            if project == "Other":
                project_name = st.text_input("Please specify Project Name", key="v_proj_name")
            else:
                project_name = project

            # --- Block Selection ---
            user_block = st.session_state.assigned_block
            default_index = 0
            is_disabled = False
            if user_block != "All":
                if user_block in ALL_BLOCKS:
                    default_index = ALL_BLOCKS.index(user_block)
                is_disabled = True

            # --- Form Section (Live Updates) ---
            st.markdown("### 1. Basic Information")
            col1, col2, col3 = st.columns(3)
            with col1:
                block_name = st.selectbox("BPIU/DPCU (Block)", ALL_BLOCKS, index=default_index, disabled=is_disabled, key="v_block")
            with col2:
                vendor_name = st.text_input("Name of Vendor", key="v_name")
            with col3:
                pan_no = st.text_input("PAN No.", key="v_pan")

            st.markdown("### 2. Bill & Payment Details")
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                bill_no = st.text_input("Bill No.", key="v_bill_no")
            with c2:
                bill_date = st.date_input("Bill Date", key="v_bill_date")
            with c3:
                payment_head = st.selectbox("Payment Head", ["Vehicle Hiring", "Meeting Cost", "Office Contingency", "Printing & Stationery", "Other"], key="v_head")
            with c4:
                date_of_payment = st.date_input("Date of Payment", key="v_pay_date")
            
            # Row 2 for Bill Details
            c5, c6 = st.columns([1, 3])
            with c5:
                bill_value = st.number_input("Bill Gross Amount", min_value=0.0, step=100.0, key="v_gross")

            st.markdown("### 3. Amount Details")
            ac1, ac2 = st.columns(2)
            with ac1:
                taxable_amount = st.number_input("Amount on which TDS Deducted", min_value=0.0, step=100.0, key="v_taxable")
            with ac2:
                gst_no = st.text_input("GST No. (Optional)", key="v_gst")

            st.markdown("### 4. Tax Calculations")
            st.caption("GST Details")
            gc1, gc2, gc3 = st.columns(3)
            with gc1:
                cgst = st.number_input("CGST @ 1%", min_value=0.0, step=10.0, key="v_cgst")
            with gc2:
                sgst = st.number_input("SGST @ 1%", min_value=0.0, step=10.0, key="v_sgst")
            with gc3:
                igst = st.number_input("IGST @ 2%", min_value=0.0, step=10.0, key="v_igst")

            st.caption("TDS Details")
            tc1, tc2, tc3, tc4 = st.columns(4)
            with tc1:
                tds_194c_1 = st.number_input("TDS u/s 194C (1%)", min_value=0.0, step=10.0, key="v_194c1")
            with tc2:
                tds_194c_2 = st.number_input("TDS u/s 194C (2%)", min_value=0.0, step=10.0, key="v_194c2")
            with tc3:
                tds_194j = st.number_input("TDS u/s 194J (10%)", min_value=0.0, step=10.0, key="v_194j")
            with tc4:
                tds_194i = st.number_input("TDS u/s 194I (10%)", min_value=0.0, step=10.0, key="v_194i")
            
            # Live Calculations
            total_deduction = cgst + sgst + igst + tds_194c_1 + tds_194c_2 + tds_194j + tds_194i
            st.divider()
            st.info(f"**Total Deductions:** ₹ {total_deduction:,.2f}")
            st.divider()

            uploaded_file = st.file_uploader("Upload Bill/Invoice (PDF)", type="pdf", key="v_file")
            
            # --- Preview & Submit Logic ---
            if "preview_data" not in st.session_state:
                st.session_state.preview_data = None
            
            if st.button("Preview Entry"):
                if not vendor_name:
                    st.error("Please enter Vendor Name")
                elif project == "Other" and not project_name:
                    st.error("Please specify the Project Name")
                else:
                    # Store data in session state for preview
                    st.session_state.preview_data = {
                        "Project": project_name,
                        "Block": block_name,
                        "Vendor": vendor_name,
                        "PAN": pan_no,
                        "Bill No": bill_no,
                        "Bill Date": str(bill_date),
                        "Head": payment_head,
                        "Pay Date": str(date_of_payment), 
                        "Gross": bill_value,
                        "Taxable": taxable_amount,
                        "GST No": gst_no,
                        "CGST": cgst, "SGST": sgst, "IGST": igst,
                        "TDS 194C 1%": tds_194c_1, "TDS 194C 2%": tds_194c_2,
                        "TDS 194J": tds_194j, "TDS 194I": tds_194i,
                        "Total Ded": total_deduction
                    }
            
            # Show Preview if data exists
            if st.session_state.preview_data:
                st.markdown("### 📝 Entry Preview")
                st.table(st.session_state.preview_data)
                
                col_edit, col_sub = st.columns(2)
                
                with col_edit:
                    if st.button("Edit Entry"):
                        st.session_state.preview_data = None
                        st.rerun()
                
                with col_sub:
                    if st.button("Confirm & Submit", type="primary"):
                        with st.spinner("Uploading File & Saving Data..."):
                            # 1. Upload File (Proxy)
                            file_link = "No File Uploaded"
                            if uploaded_file is not None:
                                file_link = upload_file_to_gas(
                                    uploaded_file, 
                                    uploaded_file.name, 
                                    block_name, 
                                    bill_date
                                )

                            # 2. Save to Sheet
                            if file_link != "Upload Failed":
                                sheet = get_google_sheet()
                                if sheet:
                                    ws = init_sheet_headers(sheet)
                                    if ws:
                                        row_data = [
                                            str(datetime.now()), project_name, block_name, vendor_name, pan_no, bill_no, str(bill_date),
                                            payment_head, str(date_of_payment), bill_value, taxable_amount, gst_no,
                                            cgst, sgst, igst, tds_194c_1, tds_194c_2, tds_194j, tds_194i,
                                            total_deduction, file_link, st.session_state.username
                                        ]
                                        ws.append_row(row_data)
                                        st.toast(f"Success! {vendor_name} entry saved.", icon="✅")
                                        st.balloons()
                                        
                                        # Reset Form and Preview
                                        st.session_state.preview_data = None
                                        st.session_state.reset_form = True
                                        time.sleep(1)
                                        st.rerun()
                                    else:
                                        st.error("Sheet Error")
                            else:
                                st.error("Aborting Save due to Upload Failure")

        with tab2:
            st.subheader("Salary Payment Details (24Q)")
            
            # --- Salary: Project Selection ---
            st.markdown("### 0. Project Details")
            sal_project_options = ["NRLM", "MMRY", "Mahila Samvad", "SJY", "Other"]
            sal_project = st.selectbox("Select Project ", sal_project_options, key="sal_proj")
            
            if sal_project == "Other":
                sal_project_name = st.text_input("Please specify Project Name ", key="sal_proj_name")
            else:
                sal_project_name = sal_project

            # --- Salary: Basic Info ---
            st.markdown("### 1. Employee Information")
            sc1, sc2, sc3 = st.columns(3)
            with sc1:
                # Reuse block logic variables but need unique key
                sal_block_name = st.selectbox("BPIU/DPCU (Block)", ALL_BLOCKS, index=default_index, disabled=is_disabled, key="sal_block")
            with sc2:
                emp_name = st.text_input("Name of Staff/Employee", key="emp_name")
            with sc3:
                emp_id = st.text_input("Employee ID", key="emp_id")
            
            sc4, sc5 = st.columns(2)
            with sc4:
                emp_pan = st.text_input("PAN No.", key="emp_pan")
            with sc5:
                designation = st.text_input("Designation (Optional)", key="desig")

            # --- Salary: Payment & Tax ---
            st.markdown("### 2. Salary & Tax Details")
            sp1, sp2, sp3 = st.columns(3)
            with sp1:
                sal_month = st.selectbox("Salary Month", ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"], key="sal_mon")
            with sp2:
                sal_year = st.number_input("Year", min_value=2024, max_value=2030, value=datetime.now().year, key="sal_yr")
            with sp3:
                date_credit = st.date_input("Date of Credit/Payment", key="sal_date")

            st.markdown("### 3. TDS Calculations (Section 192)")
            st1, st2, st3 = st.columns(3)
            with st1:
                 gross_salary = st.number_input("Gross Salary Credited", min_value=0.0, step=100.0, key="gross_sal")
            with st2:
                 taxable_salary = st.number_input("Taxable Amount (After Exemptions)", min_value=0.0, step=100.0, key="tax_sal")
            with st3:
                 tds_deducted = st.number_input("TDS Deducted", min_value=0.0, step=100.0, key="tds_ded")

            st.divider()
            st.info(f"**Net Disbursal:** ₹ {(gross_salary - tds_deducted):,.2f}")
            st.divider()

            sal_file = st.file_uploader("Upload Salary Slip/Declaration", type="pdf", key="sal_file")
            
            if st.button("Submit Salary Entry", type="primary", key="sal_btn"):
                if not emp_name:
                    st.error("Please enter Employee Name")
                else:
                    with st.spinner("Processing Salary Entry..."):
                        # 1. Upload File
                        sal_link = "No File Uploaded"
                        if sal_file is not None:
                            # Create a date object for the function
                            sal_date_obj = datetime(sal_year, 1, 1) # Dummy date just for year extraction if needed, or use actual payment date
                            sal_link = upload_file_to_gas(
                                sal_file, 
                                f"Salary_{emp_name}_{sal_month}", 
                                sal_block_name, 
                                datetime.now() # Use current date for folder structure
                            )
                        
                        # 2. Save to 'Salary Data' Worksheet
                        if sal_link != "Upload Failed":
                            sheet = get_google_sheet()
                            if sheet:
                                try:
                                    # Try to open 'Salary Data', create if not exists
                                    try:
                                        ws_sal = sheet.worksheet("Salary Data")
                                    except:
                                        ws_sal = sheet.add_worksheet(title="Salary Data", rows="1000", cols="20")
                                        headers = ["Timestamp", "Project", "Block", "Employee Name", "ID", "PAN", "Designation", 
                                                   "Month", "Year", "Payment Date", "Gross Salary", "Taxable Amount", "TDS Deducted", 
                                                   "File Link", "Entered By"]
                                        ws_sal.append_row(headers)
                                    
                                    row_data = [
                                        str(datetime.now()), sal_project_name, sal_block_name, emp_name, emp_id, emp_pan, designation,
                                        sal_month, sal_year, str(date_credit), gross_salary, taxable_salary, tds_deducted,
                                        sal_link, st.session_state.username
                                    ]
                                    ws_sal.append_row(row_data)
                                    st.toast(f"Salary Entry for {emp_name} saved!", icon="✅")
                                    st.balloons()
                                    
                                except Exception as e:
                                    st.error(f"Sheet Error: {e}")
                            else:
                                st.error("Connection Error")
                        else:
                            st.error("Upload Failed")

    elif menu == "📊 Reports":
        st.title("📊 Reports & Returns")
        st.write("Generate detailed TDS Reports with Project-wise subtotals.")
        
        report_month = st.selectbox("Select Month", ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"])
        report_year = st.number_input("Select Year", min_value=2024, max_value=2030, value=datetime.now().year)
        
        if st.button("Generate Vendor Report (26Q)"):
            with st.spinner("Fetching data from Google Sheets..."):
                sheet = get_google_sheet()
                if sheet:
                    try:
                        # Fetch Data using get_all_values with Retry Logic
                        ws = sheet.get_worksheet(0)
                        
                        def fetch_data_with_retry(worksheet, retries=3, delay=2):
                            for i in range(retries):
                                try:
                                    return worksheet.get_all_values()
                                except Exception as e:
                                    if i == retries - 1: # Last attempt
                                        raise e
                                    time.sleep(delay)
                                    delay *= 2 # Exponential backoff
                            return []

                        raw_data = fetch_data_with_retry(ws)
                        
                        if raw_data: # Check if data exists
                            # Define Standard Headers
                            expected_headers = [
                                "Timestamp", "Project Name", "Block", "Vendor Name", "PAN", "Bill No", "Bill Date", 
                                "Payment Head", "Date of Payment", "Gross Amount", "Taxable Amount", "GST No",
                                "CGST", "SGST", "IGST", "TDS 194C 1%", "TDS 194C 2%", "TDS 194J", "TDS 194I",
                                "Total Deduction", "File Link", "Entered By"
                            ]
                            
                            first_row = raw_data[0]
                            
                            # Check if first row is actually a header
                            if first_row and str(first_row[0]).strip() == "Timestamp":
                                headers = [h.strip() for h in first_row]
                                rows = raw_data[1:]
                                df = pd.DataFrame(rows, columns=headers)
                            else:
                                # First row looks like data (or empty header), treat all as data
                                # Ensure row length matches header length to prevent mismatch
                                # If row is shorter, pad with empty strings
                                padded_data = []
                                for row in raw_data:
                                    if len(row) < len(expected_headers):
                                        row += [""] * (len(expected_headers) - len(row))
                                    padded_data.append(row[:len(expected_headers)]) # Truncate if too long (unlikely but safe)
                                
                                df = pd.DataFrame(padded_data, columns=expected_headers)
                        else:
                            st.info("Sheet is empty.")
                            df = pd.DataFrame()
                        
                        if not df.empty:
                            # --- Role-Based Filtering ---
                            current_block = st.session_state.get("assigned_block", "All")
                            user_role = st.session_state.get("user_role", "Block User")
                            
                            if current_block != "All":
                                # Filter data to show ONLY the user's block
                                if "Block" in df.columns:
                                    df = df[df["Block"] == current_block]
                                    st.info(f"Filtering Report for Block: {current_block}")
                                else:
                                    st.warning("Block column missing in data. Showing all records (Admin check required).")
                            
                            # Fix for potential missing 'Project Name' column
                            proj_col = "Project Name"
                            # Double check if our fallback worked or if we have a mismatch
                            if "Project Name" not in df.columns:
                                if "Project" in df.columns:
                                     proj_col = "Project"
                                else:
                                    # This should theoretically not happen now with forced headers, but safe to keep
                                    df["Project Name"] = "Unknown" 
                            
                            # Ensure numeric columns
                            numeric_cols = ["Gross Amount", "Taxable Amount", "CGST", "SGST", "IGST", 
                                          "TDS 194C 1%", "TDS 194C 2%", "TDS 194J", "TDS 194I", "Total Deduction"]
                            
                            # Convert to numeric
                            for col in numeric_cols:
                                if col in df.columns:
                                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                                else:
                                    df[col] = 0.0 # Initialize missing numeric cols

                            # --- Process Report ---
                            st.subheader(f"TDS Report for {report_month} {report_year}")
                            
                            # Group by Project
                            grouped = df.groupby(proj_col)
                            
                            # Map for Project-wise Tax Totals
                            project_tax_totals = {}
                            
                            final_rows = []
                            grand_totals = {col: 0 for col in numeric_cols}

                            for project, group in grouped:
                                # Add Data Rows
                                for _, row in group.iterrows():
                                    final_rows.append(row.to_dict())
                                
                                # Calculate Subtotals
                                subtotal = group[numeric_cols].sum()
                                sub_row = {k: "" for k in df.columns}
                                sub_row[proj_col] = f"Total {project}"
                                for k, v in subtotal.items():
                                    sub_row[k] = v
                                    grand_totals[k] += v
                                
                                # Store Project Totals for Summary Section
                                project_tax_totals[project] = subtotal.to_dict()

                                final_rows.append(sub_row) # Add Subtotal Row
                            
                            # Add Grand Total Row
                            grand_row = {k: "" for k in df.columns}
                            grand_row[proj_col] = "GRAND TOTAL"
                            for k, v in grand_totals.items():
                                grand_row[k] = v
                            final_rows.append(grand_row)
                            
                            # Create Display DF
                            report_df = pd.DataFrame(final_rows)
                            
                            # Reorder columns to match requirement (Project first)
                            display_cols = [proj_col, "Block", "Vendor Name", "PAN", "Bill No", "Bill Date", 
                                          "Payment Head", "Gross Amount", "Taxable Amount", 
                                          "CGST", "SGST", "IGST", "TDS 194C 1%", "TDS 194C 2%", "TDS 194J", "TDS 194I", "Total Deduction"]
                            
                            # Filter only existing columns for display
                            valid_display_cols = [c for c in display_cols if c in report_df.columns]
                            
                            st.dataframe(report_df[valid_display_cols], hide_index=True)
                            
                            # --- Helper for Indian Currency Formatting ---
                            def format_indian_currency(value):
                                try:
                                    value = float(value)
                                    s, *d = str(value).partition(".")
                                    r = ",".join([s[x-2:x] for x in range(-3, -len(s), -2)][::-1] + [s[-3:]])
                                    formatted = "".join([r] + d)
                                    return f"₹ {formatted}"
                                except:
                                    return value

                            # --- Tax Summary Tables (Matrix View) ---
                            st.divider()
                            st.subheader("Project-wise Tax Summaries")
                            
                            # HTML Builder for Download - Re-initializing
                            html_report = f"""
                            <html>
                            <head>
<style>
    @page {{ margin: 2.8cm 4.0cm 3.1cm 1.4cm; size: landscape; }} /* Top Right Bottom Left */
    body {{ font-family: Arial, sans-serif; font-size: 9px; }}
    
    /* Strict Table Compaction */
    table.table {{ width: auto; border-collapse: collapse; margin-bottom: 10px; }}
    table.table th, table.table td {{ 
        border: 1px solid #ddd; 
        padding: 2px 4px; 
        text-align: left; 
        vertical-align: middle; 
        font-size: 9px; 
        white-space: nowrap; 
        width: 1%; /* Force shrink */
    }}
    
    th {{ background-color: #f2f2f2; font-weight: bold; }}
    .header {{ font-size: 14px; font-weight: bold; margin-bottom: 5px; }}
    .sub-header {{ font-size: 11px; font-weight: bold; margin-top: 10px; color: #333; }}
    .grand-total-row {{ font-weight: bold; background-color: #e6f3ff; }}
</style>
</head>
<body>
<div class='header'>TDS Report for {report_month} {report_year}</div>
"""

                            # Main Table HTML
                            html_report += report_df[valid_display_cols].to_html(index=False, classes='table')

                            html_report += "<div class='sub-header'>Project-wise Tax Matrix</div>"
                            
                            sorted_projects = sorted(project_tax_totals.keys())
                            
                            # Define Rows (Heads)
                            tax_heads_map = {
                                "TDS 194C 1%": "TDS 194C 1%",
                                "TDS 194C 2%": "TDS 194C 2%",
                                "TDS 194J 10%": "TDS 194J",
                                "TDS 194I 10%": "TDS 194I",
                                "CGST @1%": "CGST",
                                "SGST @1%": "SGST",
                                "IGST @2%": "IGST"
                            }
                            
                            matrix_data = []
                            col_totals = {proj: 0.0 for proj in sorted_projects}
                            col_totals["Total"] = 0.0

                            # Build Matrix Rows
                            for display_head, key in tax_heads_map.items():
                                row_data = {"Head": display_head}
                                row_total = 0.0
                                
                                for proj in sorted_projects:
                                    val = project_tax_totals[proj].get(key, 0)
                                    row_total += val
                                    col_totals[proj] += val # Add to column total
                                    
                                    # Format Value for Display
                                    row_data[proj] = format_indian_currency(val) if val > 0 else "0"
                                
                                row_data["Total"] = format_indian_currency(row_total)
                                col_totals["Total"] += row_total
                                matrix_data.append(row_data)

                            # Add Total Row
                            total_row = {"Head": "Total"}
                            for proj in sorted_projects:
                                total_row[proj] = format_indian_currency(col_totals[proj])
                            total_row["Total"] = format_indian_currency(col_totals["Total"])
                            
                            matrix_data.append(total_row)
                            
                            # Create DataFrame
                            matrix_df = pd.DataFrame(matrix_data)
                            
                            # Display in App
                            st.dataframe(matrix_df, hide_index=True)
                            
                            # Add to HTML
                            html_report += matrix_df.to_html(index=False, classes='table')
                            
                            html_report += "</body></html>"
                            
                            # --- Download Buttons ---
                            st.divider()
                            st.subheader("📥 Downloads")
                            dl_cols = st.columns(2)
                            with dl_cols[0]:
                                st.download_button(
                                    label="Download Excel (HTML Format)",
                                    data=html_report,
                                    file_name=f"TDS_Report_{report_month}_{report_year}.xls",
                                    mime="application/vnd.ms-excel",
                                    help="Opens in Excel with formatting preserved"
                                )
                            with dl_cols[1]:
                                st.download_button(
                                    label="Download Printable View (PDF)",
                                    data=html_report,
                                    file_name=f"TDS_Report_{report_month}_{report_year}.html",
                                    mime="text/html",
                                    help="Download HTML. Open and 'Print to PDF' for best results."
                                )
                                

                            
                            

                        else:
                            st.info("No data found.")
                            
                    except Exception as e:
                        st.error(f"Error generating report: {e}")

    elif menu == "⚙️ Settings":
        st.title("⚙️ Settings")
        st.write("Configure Google Sheet Connection here.")

