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
                "Timestamp", "Project Name", "Block", "Vendor Name", "PAN", "Bill No", 
                "Bill Date", "Payment Head", "Payment Date", "Gross Amount", "Taxable Amount", "GST No",
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
except Exception:
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
    print(f"Error syncing blocks: {e}")

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
                payment_head = st.selectbox("Payment Head", [
                    "Vehicle Hiring", "Meeting Cost", "Office Contingency", "Printing & Stationery",
                    "Residential Training", "Non Residential Training", "Computer equipment & Hiring",
                    "MIS Hono.", "Consultant", "Other"
                ], key="v_head")
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
                                            str(datetime.now()), project_name, block_name, vendor_name, pan_no, bill_no, 
                                            str(bill_date), payment_head, str(date_of_payment), bill_value, taxable_amount, gst_no,
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
        
        # report_month = st.selectbox("Select Month", ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"])
        # report_year = st.number_input("Select Year", min_value=2024, max_value=2030, value=datetime.now().year)
        
        # --- Date Range Picker ---
        today = datetime.now()
        first_day_current_month = today.replace(day=1)
        
        c1, c2 = st.columns(2)
        with c1:
            start_date = st.date_input("Start Date", value=first_day_current_month)
        with c2:
            end_date = st.date_input("End Date", value=today)

        if st.button("Generate Reports"): 
            
            # --- Shared Data Fetching Helper ---
            def get_data_as_df(worksheet_index_or_name):
                sheet = get_google_sheet()
                if not sheet: return pd.DataFrame()
                try:
                    if isinstance(worksheet_index_or_name, int):
                        ws = sheet.get_worksheet(worksheet_index_or_name)
                    else:
                        ws = sheet.worksheet(worksheet_index_or_name)
                    
                    data = ws.get_all_values()
                    if not data: return pd.DataFrame()
                    
                    headers = [h.strip() for h in data[0]] # Normalize headers
                    # Deduplicate headers
                    seen = {}
                    new_headers = []
                    for h in headers:
                        if h in seen:
                            seen[h] += 1
                            new_headers.append(f"{h}_{seen[h]}")
                        else:
                            seen[h] = 0
                            new_headers.append(h)
                            
                    rows = data[1:]
                    return pd.DataFrame(rows, columns=new_headers)
                except Exception:
                    return pd.DataFrame()

            # --- 1. Vendor Data (26Q) ---
            st.markdown("---")
            st.subheader("1. Vendor Report (26Q)")
            df_26q = pd.DataFrame() # Initialize

            with st.spinner("Fetching Vendor Data..."):
                df_26q = get_data_as_df(0) # First sheet
                
                if not df_26q.empty:
                        # --- Role-Based Filtering ---
                    current_block = st.session_state.get("assigned_block", "All")
                    if current_block != "All" and "Block" in df_26q.columns:
                        df_26q = df_26q[df_26q["Block"] == current_block]

                    # --- 1. Column Normalization (MOVED UP) ---
                    # Must normalize BEFORE filtering by date
                    
                    # --- 0. Strip Header Whitespace ---
                    df_26q.columns = df_26q.columns.str.strip()

                    def fuzzy_rename(df, target, keywords):
                        if target in df.columns: return
                        # Find candidate
                        for col in df.columns:
                            c_lower = col.lower().strip()
                            if all(k.lower() in c_lower for k in keywords):
                                df.rename(columns={col: target}, inplace=True)
                                return

                    # Canonical Normalization based on User Data Sample
                    # Sample Headers: Time Strap, Project, Block, Vendor Name, PAN no., Bill No., Bill Date, Payment Head, Payment date, 
                    # Gross Amount, amount on which TDS deducted, GST No., CGST, SGST, IGST, TDS @ 1%, TDS @ 2%, TDS 194J, TDS 194I, Total amount, File upload, Block
                    
                    normalization_map = {
                        "Payment Date": ["Payment date", "Payment Date"],
                        "Bill Date": ["Bill Date"],
                        "Bill No": ["Bill No.", "Bill No"],
                        "PAN": ["PAN no.", "PAN No.", "PAN"],
                        "GST No": ["GST No.", "GST No"],
                        "Taxable Amount": ["amount on which TDS deducted", "Taxable Amount"],
                        "Total Deduction": ["Total amount", "Total Deduction"],
                        "File Link": ["File upload", "File Link", "File"],
                        "Vendor Name": ["Vendor Name", "Party Name"],
                        "Project Name": ["Project", "Project Name"],
                        "Block": ["Block", "Site"],
                        # --- CRITICAL: Restore TDS Mappings ---
                        "TDS 194C 1%": ["TDS @ 1%", "TDS 194C 1%", "1% TDS"],
                        "TDS 194C 2%": ["TDS @ 2%", "TDS 194C 2%", "2% TDS"],
                        "TDS 194J": ["TDS 194J", "194J"],
                        "TDS 194I": ["TDS 194I", "194I"]
                    }

                    for canonical, variations in normalization_map.items():
                        if canonical in df_26q.columns: continue
                        for var in variations:
                            if var in df_26q.columns:
                                df_26q.rename(columns={var: canonical}, inplace=True)
                                break
                    
                    # Fuzzy Backups if exact match fails
                    fuzzy_rename(df_26q, "Payment Date", ["Payment", "Date"])
                    fuzzy_rename(df_26q, "Bill Date", ["Bill", "Date"])
                    
                    # Fix for potential missing 'Project Name'
                    if "Project Name" not in df_26q.columns and "Project" in df_26q.columns:
                         df_26q.rename(columns={"Project": "Project Name"}, inplace=True)

                    # --- 2. Date Range Filtering (Payment Date) ---
                    try:
                        # Normalize Payment Date
                        if "Payment Date" in df_26q.columns:
                            # Convert to datetime with versatile parsing
                            # User data is YYYY-MM-DD. dayfirst=True is bad for this. 
                            # Let pandas infer or use mixed.
                            df_26q['Payment Date Temp'] = pd.to_datetime(df_26q['Payment Date'], errors='coerce')
                            
                            # Filter by Range
                            mask = (df_26q['Payment Date Temp'].dt.date >= start_date) & (df_26q['Payment Date Temp'].dt.date <= end_date)
                            df_26q = df_26q[mask]
                            
                            # Format Date Columns for Display (DD-MM-YYYY)
                            # Apply to valid dates only
                            valid_dates = df_26q['Payment Date Temp'].notna()
                            df_26q.loc[valid_dates, 'Payment Date'] = df_26q.loc[valid_dates, 'Payment Date Temp'].dt.strftime('%d-%m-%Y')
                            
                            if "Bill Date" in df_26q.columns:
                                 df_26q['Bill Date'] = pd.to_datetime(df_26q['Bill Date'], errors='coerce').dt.strftime('%d-%m-%Y')

                            df_26q = df_26q.drop(columns=['Payment Date Temp'])
                        else:
                            st.warning("Column 'Payment Date' not found even after normalization. Please check sheet headers.")
                            # st.write("Available Columns:", df_26q.columns.tolist())
                            
                    except Exception as e: 
                        st.error(f"Date Filter Error (26Q): {e}")

                    if not df_26q.empty:
                        # --- 3. Numeric Conversion ---
                        numeric_cols = ["Gross Amount", "Taxable Amount", "CGST", "SGST", "IGST", 
                                      "TDS 194C 1%", "TDS 194C 2%", "TDS 194J", "TDS 194I", "Total Deduction"]
                        for col in numeric_cols:
                            if col in df_26q.columns:
                                df_26q[col] = pd.to_numeric(df_26q[col], errors='coerce').fillna(0)
                        
                        # --- 4. Display Selection ---
                        col_mapping = {
                            "Project Name": "Project",
                            "Block": "Block",
                            "Vendor Name": "Vendor Name",
                            "PAN": "PAN no.",
                            "GST No": "GST No.",
                            "Bill No": "Bill No.",
                            "Bill Date": "Bill Date",
                            "Payment Head": "Payment Head",
                            "Payment Date": "Payment date",
                            "Gross Amount": "Gross Amount",
                            "Taxable Amount": "amount on which TDS deducted",
                            "CGST": "CGST",
                            "SGST": "SGST",
                            "IGST": "IGST",
                            "TDS 194C 1%": "TDS @ 1%",
                            "TDS 194C 2%": "TDS @ 2%",
                            "TDS 194J": "TDS 194J",
                            "TDS 194I": "TDS 194I",
                            "Total Deduction": "Total amount"
                        }

                        desired_order = [
                            "Project Name", "Block", "Vendor Name", "PAN", "GST No", 
                            "Bill No", "Bill Date", "Payment Head", "Payment Date", 
                            "Gross Amount", "Taxable Amount", "CGST", "SGST", "IGST", 
                            "TDS 194C 1%", "TDS 194C 2%", "TDS 194J", "TDS 194I", "Total Deduction"
                        ]

                        # Force all required columns to exist
                        for col in desired_order:
                            if col not in df_26q.columns:
                                df_26q[col] = ""

                        # Create Display DataFrame
                        final_df = df_26q[desired_order].rename(columns=col_mapping)
                        
                        # --- ADD TOTALS LOGIC ---
                        # We want Project-wise totals and Grand Total
                        
                        # Identify numeric columns in final_df (mapped names)
                        # Mapped numeric columns:
                        disp_numeric_cols = [
                            "Gross Amount", "amount on which TDS deducted", "CGST", "SGST", "IGST", 
                            "TDS @ 1%", "TDS @ 2%", "TDS 194J", "TDS 194I", "Total amount"
                        ]

                        # Ensure they are numeric
                        for c in disp_numeric_cols:
                            final_df[c] = pd.to_numeric(final_df[c], errors='coerce').fillna(0)

                        # Create a list to build new DF with subtotals
                        new_rows = []
                        
                        # Groups
                        projects = final_df["Project"].unique()
                        grand_totals = {c: 0.0 for c in disp_numeric_cols}
                        
                        for proj in projects:
                            proj_df = final_df[final_df["Project"] == proj]
                            
                            # Add data rows
                            for _, row in proj_df.iterrows():
                                new_rows.append(row)
                                # Add to grand total
                                for c in disp_numeric_cols:
                                    grand_totals[c] += row[c]
                            
                            # Add Project Subtotal
                            subrow = {c: "" for c in final_df.columns}
                            subrow["Project"] = f"Total {proj}"
                            subrow["Vendor Name"] = "Subtotal"
                            
                            for c in disp_numeric_cols:
                                val = proj_df[c].sum()
                                subrow[c] = val
                            
                            new_rows.append(pd.Series(subrow))
                        
                        # Add Grand Total Row
                        grand_row = {c: "" for c in final_df.columns}
                        grand_row["Project"] = "GRAND TOTAL"
                        for c in disp_numeric_cols:
                            grand_row[c] = grand_totals[c]
                        
                        # Re-create DataFrame
                        final_df_display = pd.DataFrame(new_rows, columns=final_df.columns)
                        
                        # Format for display (optional, keeps numeric for styling or exports? 
                        # Usually better to keep numeric for export, but user might want formatting.
                        # Sticking to numeric for now, st.dataframe handles it well)
                        
                        st.subheader(f"1. Vendor Report (26Q) - Block: {current_block}")
                        st.dataframe(final_df_display, hide_index=True)
                        st.caption(f"Showing {len(final_df)} original records + totals.")
                        
                        # Update final_df reference for Export use later
                        final_df = final_df_display
                        
                    else:
                        st.subheader(f"1. Vendor Report (26Q) - Block: {current_block}")
                        st.warning(f"No Vendor records found for selected period.")
                else:
                    st.info("Vendor Sheet Empty")

            # --- 2. Salary Data (24Q) ---
            st.markdown("---")
            st.subheader("2. Salary Report (24Q)")
            df_24q = pd.DataFrame() # Initialize

            with st.spinner("Fetching Salary Data..."):
                df_24q = get_data_as_df("Salary Data") # Attempt to fetch
                
                if not df_24q.empty:
                    # --- Filtering ---
                    current_block = st.session_state.get("assigned_block", "All")
                    if current_block != "All" and "Block" in df_24q.columns:
                        df_24q = df_24q[df_24q["Block"] == current_block]
                    
                    # Date Range Filtering for Salary
                    if "Month" in df_24q.columns and "Year" in df_24q.columns:
                        try:
                            df_24q["Temp_Date"] = pd.to_datetime(df_24q["Month"] + " " + df_24q["Year"].astype(str), format="%B %Y", errors='coerce')
                            mask = (df_24q["Temp_Date"].dt.date >= start_date) & (df_24q["Temp_Date"].dt.date <= end_date)
                            df_24q = df_24q[mask]
                        except Exception as e:
                            st.warning(f"Salary Date Parse Error: {e}")

                    if not df_24q.empty:
                         # Numeric
                        sal_numeric = ["Gross Salary", "Taxable Amount", "TDS Deducted"]
                        for col in sal_numeric:
                            if col in df_24q.columns:
                                df_24q[col] = pd.to_numeric(df_24q[col], errors='coerce').fillna(0)

                        st.dataframe(df_24q[["Project", "Block", "Employee Name", "PAN", "Month", "Year", "Gross Salary", "Taxable Amount", "TDS Deducted"]], hide_index=True)
                        st.caption(f"Showing {len(df_24q)} records.")
                    else:
                         st.warning(f"No Salary records found for selection.")
                else:
                    # st.info("Salary Sheet Empty") 
                    pass

            # --- 3. Combined Tax Summary ---
            st.markdown("---")
            st.subheader("3. Combined Tax Summary")
            
            # Consolidated Tax Dictionary: {Project: {Head: Amount}}
            tax_summary = {}

            # Helper to add to summary
            def add_to_summary(df, group_col, head_col, amount_col=None):
                if df.empty or group_col not in df.columns: return
                
                for _, row in df.iterrows():
                    proj = row.get(group_col, "Unknown")
                    if proj not in tax_summary: tax_summary[proj] = {}
                    
                    # Try to get value, robustly
                    val = row.get(head_col, 0)
                    try:
                        # Clean string values like " 1,000 " -> 1000
                        if isinstance(val, str):
                            val = val.replace(",", "").strip()
                            if val == "": val = 0
                        val = float(val)
                    except:
                        val = 0
                        
                    if val > 0:
                        head_name = head_col 
                        if head_col == "TDS Deducted": head_name = "TDS u/s 192B"
                        
                        tax_summary[proj][head_name] = tax_summary[proj].get(head_name, 0) + val

            # Process Vendor Data
            if not df_26q.empty:
                p_col = "Project Name"
                vendor_heads = ["TDS 194C 1%", "TDS 194C 2%", "TDS 194J", "TDS 194I", "CGST", "SGST", "IGST"]
                for head in vendor_heads:
                    if head in df_26q.columns:
                        add_to_summary(df_26q, p_col, head)

            # Process Salary Data
            if not df_24q.empty:
                 add_to_summary(df_24q, "Project", "TDS Deducted")

            # --- Build Matrix DataFrame ---
            if tax_summary:
                all_projects = sorted(tax_summary.keys())
                all_heads = [
                    "TDS 194C 1%", "TDS 194C 2%", "TDS 194J", "TDS 194I", 
                    "TDS u/s 192B", # Salary Head
                    "CGST", "SGST", "IGST"
                ]
                
                matrix_rows = []
                col_totals = {p: 0.0 for p in all_projects}
                col_totals["Total"] = 0.0

                for head in all_heads:
                    row = {"Head": head}
                    row_total = 0.0
                    for p in all_projects:
                        val = tax_summary[p].get(head, 0)
                        row[p] = f"{val:,.2f}" if val > 0 else "0"
                        row_total += val
                        col_totals[p] += val
                    
                    row["Total"] = f"{row_total:,.2f}"
                    col_totals["Total"] += row_total
                    matrix_rows.append(row)
                
                # Totals Row
                tot_row = {"Head": "Grand Total"}
                for p in all_projects:
                    tot_row[p] = f"{col_totals[p]:,.2f}"
                tot_row["Total"] = f"{col_totals['Total']:,.2f}"
                matrix_rows.append(tot_row)

                st.dataframe(pd.DataFrame(matrix_rows), hide_index=True)
                
                # --- EXPORT LOGIC ---
                st.markdown("### Export Reports")
                
                # --- 1. Excel Export (Consolidated) ---
                import io
                import xlsxwriter

                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                    workbook = writer.book
                    worksheet = workbook.add_worksheet("TDS Report")
                    writer.sheets["TDS Report"] = worksheet
                    
                    # Formats
                    header_format = workbook.add_format({'bold': True, 'font_size': 14, 'align': 'center', 'valign': 'vcenter', 'border': 1})
                    sub_header_format = workbook.add_format({'bold': True, 'font_size': 12, 'border': 1, 'bg_color': '#D3D3D3'})
                    data_format = workbook.add_format({'border': 1})
                    
                    # Current Row Pointer
                    current_row = 0
                    
                    # Main Header
                    report_title = f"TDS & GST Report for period: {start_date.strftime('%d-%m-%Y')} to {end_date.strftime('%d-%m-%Y')}"
                    worksheet.merge_range(current_row, 0, current_row, 10, report_title, header_format)
                    current_row += 2
                    
                    # --- Section 1: Vendor 26Q ---
                    worksheet.write(current_row, 0, "1. Vendor Report (26Q)", sub_header_format)
                    current_row += 1
                    
                    if not df_26q.empty:
                        # Write Headers
                        for col_num, value in enumerate(final_df.columns.values):
                            worksheet.write(current_row, col_num, value, sub_header_format)
                        current_row += 1
                        
                        # Write Data
                        # Clean NaN for Excel
                        df_to_write = final_df.fillna("")
                        for row_num, row_data in enumerate(df_to_write.values):
                            for col_num, value in enumerate(row_data):
                                worksheet.write(current_row + row_num, col_num, value, data_format)
                        current_row += len(df_to_write) + 2
                    else:
                        worksheet.write(current_row, 0, "No Data", data_format)
                        current_row += 2

                    # --- Section 2: Salary 24Q ---
                    worksheet.write(current_row, 0, "2. Salary Report (24Q)", sub_header_format)
                    current_row += 1
                    
                    if not df_24q.empty:
                        # Prepare Salary DF for export (subset)
                        sal_exp_df = df_24q[["Project", "Block", "Employee Name", "PAN", "Month", "Year", "Gross Salary", "Taxable Amount", "TDS Deducted"]]
                        # Clean NaN
                        sal_exp_df = sal_exp_df.fillna("")
                        
                        # Write Headers
                        for col_num, value in enumerate(sal_exp_df.columns.values):
                            worksheet.write(current_row, col_num, value, sub_header_format)
                        current_row += 1
                        
                         # Write Data
                        for row_num, row_data in enumerate(sal_exp_df.values):
                            for col_num, value in enumerate(row_data):
                                worksheet.write(current_row + row_num, col_num, value, data_format)
                        current_row += len(sal_exp_df) + 2
                    else:
                        worksheet.write(current_row, 0, "No Data", data_format)
                        current_row += 2

                    # --- Section 3: Summary ---
                    worksheet.write(current_row, 0, "3. Combined Tax Summary", sub_header_format)
                    current_row += 1
                    
                    df_summary = pd.DataFrame(matrix_rows)
                    if not df_summary.empty:
                        df_summary = df_summary.fillna("")
                         # Write Headers
                        for col_num, value in enumerate(df_summary.columns.values):
                            worksheet.write(current_row, col_num, value, sub_header_format)
                        current_row += 1
                        
                        # Write Data
                        for row_num, row_data in enumerate(df_summary.values):
                            for col_num, value in enumerate(row_data):
                                worksheet.write(current_row + row_num, col_num, value, data_format)
                        current_row += len(df_summary) + 2
                
                st.download_button(
                    label="Download Excel Report",
                    data=excel_buffer.getvalue(),
                    file_name="TDS_Report_Consolidated.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.info("No Tax Data to summarize.")

    elif menu == "⚙️ Settings":
        st.title("⚙️ Settings")

