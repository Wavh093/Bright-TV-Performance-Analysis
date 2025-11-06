# ==============================================================================================
# This script will be used to clean the BrightTV Dataset, before the data is pushed to Snowflake
# ==============================================================================================

import pandas as pd
from datetime import timedelta
import unicodedata
import re

# Load data
file_path = 'Project Input\Raw Data.xlsx'

# load into data frame
excel = pd.ExcelFile(file_path)

print("Sheets available ", excel.sheet_names)

# load relevant sheets
vdf = excel.parse('Viewership')
pdf = excel.parse('User Profiles')

# standardise column names
vdf.columns = vdf.columns.str.strip().str.upper().str.replace(" ","_")
pdf.columns = pdf.columns.str.strip().str.upper().str.replace(" ","_")

# compare the two user_id columns 
user_cols = [col for col in vdf.columns if "USER_ID" in col]

if len(user_cols) >= 2:
    id_col_1, id_col_2 = user_cols[:2]
    print(f"\nComparing '{id_col_1}' and '{id_col_2}'...")
    
    # check equality between the two ID Columns
    comparison = (vdf[id_col_1].astype(str).str.strip == vdf[id_col_2].astype(str).str.strip())

    identical_count = comparison.sum()
    total = len(vdf)
    percent_identical = round((identical_count/total) * 100, 2)
    
    print(f" {percent_identical}% of USER_IDs are identical between columns.")
    print(f" {total - identical_count} rows have differences.")
    
    # decide which id to keep
    if percent_identical > 95:
        print("\nColumns are mostly identical. Keeping first USER_ID column only.")
        vdf.drop(columns=[id_col_2], inplace=True)
        vdf.rename(columns={id_col_1: "USER_ID"}, inplace=True)
    else:
        print("\nColumns differ significantly. Keeping both for manual inspection.")
        vdf.rename(columns={id_col_1: "USER_ID_APP", id_col_2: "USER_ID_DEVICE"}, inplace=True)
else:
    print("Could not find two USER_ID columns in Viewership sheet.")
  
  
## ===========================================================================================================
  
# === clean the blanks in email and social media handle ===

# == helper functions ==

# shows problematic characters, as blank is not a simple space
def show_problem_chars(series, n=10):
    """Show sample values and their problematic char codepoints for inspection."""
    sample = series.dropna().astype(str).head(n)
    rows = []
    for v in sample:
        # show repr and unicode codepoints for non-ascii/space chars
        cps = [(ch, ord(ch)) for ch in v if ord(ch) > 127 or ch.isspace()]
        rows.append({"value": v, "problem_chars": cps})
    return pd.DataFrame(rows)

# unicode normalize
def normalize_text(s):
    if pd.isna(s):
        return s
    s = str(s)
    # NFKC normalization tends to canonicalize fancy spaces/characters
    s = unicodedata.normalize("NFKC", s)
    return s

# remove ALL whitespace (spaces, tabs, newlines, NBSP, ZWNBSP, etc.)
def remove_all_whitespace(s):
    if pd.isna(s):
        return s
    s = normalize_text(s)
    # \s matches many whitespace characters; use regex to remove all of them
    return re.sub(r'\s+', '', s)

# collapse repeated whitespace to single space, and trim ends
def collapse_whitespace(s):
    if pd.isna(s):
        return s
    s = normalize_text(s)
    return re.sub(r'\s+', ' ', s).strip()


# the list of the two columns to clean
email_cols = [c for c in pdf.columns if "EMAIL" in c]
handle_cols =[c for c in pdf.columns if "SOCIAL_MEDIA_HANDLE" in c]

# show candidate problematic characters before cleaning (optional)
print("Sample problematic EMAIL rows before cleaning:")
if email_cols:
    print(show_problem_chars(pdf[email_cols[0]], n=10))
print("\nSample problematic HANDLE rows before cleaning:")
if handle_cols:
    print(show_problem_chars(pdf[handle_cols[0]], n=10))


# cleaning and count changes
for col in email_cols + handle_cols:
    if col not in pdf.columns:
        continue
    original = pdf[col].astype(str).fillna("")
    cleaned = original.apply(remove_all_whitespace).str.lower().replace({"nan": ""})
    pdf[col + "_CLEANED"] = cleaned
    changed = (original != cleaned).sum()
    total = len(original)
    print(f"Column '{col}': {changed} / {total} values changed (whitespace removed).")
    
preview_cols = (email_cols[:1] if email_cols else []) + (handle_cols[:1] if handle_cols else []) 
preview = pdf[[c for c in pdf.columns if c.endswith("_CLEANED")]].head(10)
print("\nPreview of cleaned columns:")
print(preview)

# data quality check
print("\nCleaned User Profiles Preview:")
print(pdf.head(5))

print("\nCleaned Viewership Columns:")
print(vdf.columns.tolist())

## =================================================================================================

# creating derived columns
if 'AGE' in pdf.columns:
    pdf['AGE_GROUP'] =pd.cut(
        pdf['AGE'],
        bins =[0,17,24,34,44,54,64,120],
        labels=['<18', '18-24', '25-34', '35-44', '45-54', '55-64', '65+']
    )


# renaming the two userID columns in the viewership table

# --- Detect and rename USER_ID columns properly ---
user_id_cols = [col for col in vdf.columns if "USERID" in col.upper()]
print(len(user_id_cols))


if len(user_id_cols) == 0:
    print("No USERID columns found in viewership data.")

elif len(user_id_cols) == 1:
    print(f"Found only one USERID column: {user_id_cols[0]}")
    # Standardize it to USER_ID
    vdf.rename(columns={user_id_cols[0]: "USERID"}, inplace=True)

else:
   # When there are multiple user ID columns
    main_id = user_id_cols[0]
    secondary_id = user_id_cols[1]
    
    # Only rename if they’re different columns
    if main_id != secondary_id:
        vdf.rename(columns={main_id: "USER_ID"}, inplace=True)
        vdf.rename(columns={secondary_id: "USER_ID2"}, inplace=True)
        print(f"Renamed '{main_id}' → USER_ID and '{secondary_id}' → USER_ID2")
    else:
        print("Both USER_ID columns appear to have the same name — check Excel headers.")

# exporting the data
vdf.to_csv("Project Output\BTV_Viewership_Cleaned.csv", index=False)
pdf.to_csv("Project Output\BTV_UserProfiles_Cleaned.csv", index=False)

print("\n Cleaned files saved successfully!")