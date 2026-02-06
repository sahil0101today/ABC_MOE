# -*- coding: utf-8 -*-
"""
Created on Tue Dec 30 10:51:34 2025

@author: sahil_pc
"""

import requests
import zipfile
import io
import pandas as pd
from datetime import date, timedelta
import imaplib
import email
import re

USERNAME = "sahil@0101.today"
PASSWORD = "eket kvjk qwew mlil"

mail = imaplib.IMAP4_SSL("imap.gmail.com")
mail.login(USERNAME, PASSWORD)
status, mailboxes = mail.list()
status, data = mail.select('"Tata Capital"') 

#Campaigns Reports 
status, messages = mail.search(None, '(SUBJECT "Your MoEngage data exports are ready")')

URL = None  # final extracted link

if status == "OK" and messages[0]:
    email_ids = messages[0].split()

    # Iterate from latest to oldest (important)
    for email_id in reversed(email_ids):
        res, msg_data = mail.fetch(email_id, "(RFC822)")
        if res != "OK":
            continue

        for response in msg_data:
            if not isinstance(response, tuple):
                continue

            msg_obj = email.message_from_bytes(response[1])

            # ---------- READ EMAIL BODY ----------
            email_body = ""

            if msg_obj.is_multipart():
                for part in msg_obj.walk():
                    content_type = part.get_content_type()
                    content_disposition = str(part.get("Content-Disposition"))

                    if content_type == "text/plain" and "attachment" not in content_disposition:
                        email_body = part.get_payload(decode=True).decode(errors="ignore")
                        break
            else:
                email_body = msg_obj.get_payload(decode=True).decode(errors="ignore")

            # ---------- FILTER: ABC T-1 ----------
            if "ABC T-1" not in email_body:
                continue

            # ---------- EXTRACT MOENGAGE EXPORT LINK ----------
            match = re.search(
                r"https://exports-03\.moengage\.com/v1/export/file/[a-zA-Z0-9\-]+",
                email_body
            )

            if match:
                URL = match.group(0)
                break

        if URL:
            break
else:
    print("No emails found")






response = requests.get(URL)
response.raise_for_status() 


with zipfile.ZipFile(io.BytesIO(response.content)) as z:
    # Step 3: Extract and read each CSV file into a DataFrame
    dataframes = {}
    for filename in z.namelist():
        if filename.endswith('.csv'):
            with z.open(filename) as f:
                df_name = filename.split('/')[-1].split('.')[0]  # Use the filename without extension as the key
                dataframes[df_name] = pd.read_csv(f)
                
# Store each DataFrame with its respective name
for df_name, df in dataframes.items():
    globals()[df_name] = df


FINAL_COLUMNS = [
    "Campaign ID",
    "Campaign Name",
    "Flows Name",
    "Parent Campaign ID",
    "Campaign Type",
    "Campaign Delivery Type",
    "Campaign Channel",
    "Campaign Status",
    "Campaign Sent Time",
    "Conversion Goal 1 Name",
    "Conversion Goal 1 Event",
    "Total Sent",
    "Total Delivered",
    "Delivery Rate",
    "Total Open",
    "Open Rate",
    "Total clicks",
    "Unique clicks",
    "CTOR",
    "CTR",
    "View Through Converted Users",
    "Click Through Converted Users",
    "Message title /subject line/Header",
    "Content/Body",
    "Footer",
    "Hard bounces",
    "Whatsapp Button 1 Text",
    "Total Hard bounces",
    "Hard bounce rate",
    "Soft bounces",
    "Total Soft bounces",
    "Soft bounce rate",
    "Unsubscribes",
    "Unsubscribe rate",
    "Complaints",
    "Complaints rate",
    "Drops",
    "Campaign Connector",
    "Billable Count",
    "Dataframe Name"
]

COLUMN_MAPPING = {
    
    "_FLOWS_EMAIL_": {
    "Campaign ID": "Campaign ID",
    "Campaign Name": "Campaign Name",
    "Flows Name": "Flows Name",
    "Parent Campaign ID": None,
    "Campaign Type": "Campaign Type",
    "Campaign Delivery Type": "Campaign Delivery Type",
    "Campaign Channel": "Campaign Channel",
    "Campaign Status": "Campaign Status",
    "Campaign Sent Time": "Campaign Sent Time",
    "Conversion Goal 1 Name": None,
    "Conversion Goal 1 Event": None,
    "Total Sent": "Total Sent",
    "Total Delivered": "Total Delivered",
    "Delivery Rate": "Delivery Rate",
    "Total Open": "Total Open",
    "Open Rate": "Open rate",
    "Total clicks": "Total clicks",
    "Unique clicks": "Unique clicks",
    "CTOR": None,
    "CTR": "CTR",
    "View Through Converted Users": "Goal 1 View Through Converted Users",
    "Click Through Converted Users":"Goal 1 Click Through Converted Users",
    "Message title /subject line/Header": "Email Subject",
    "Content/Body": None,
    "Footer": None,
    "Hard bounces": "Hard bounces",
    "Whatsapp Button 1 Text": None,
    "Total Hard bounces": "Total Hard bounces",
    "Hard bounce rate": "Hard bounce rate",
    "Soft bounces": "Soft bounces",
    "Total Soft bounces": "Total Soft bounces",
    "Soft bounce rate": "Soft bounce rate",
    "Unsubscribes": "Unsubscribes",
    "Unsubscribe rate": "Unsubscribe rate",
    "Complaints": "Complaints",
    "Complaints rate": "Complaints rate",
    "Drops": "Drops",
    "Campaign Connector": None,
    "Billable Count": None
},
    
    "_FLOWS_SMS_": {
    "Campaign ID": "Campaign ID",
    "Campaign Name": "Campaign Name",
    "Flows Name": "Flows Name",
    "Parent Campaign ID": None,
    "Campaign Type": "Campaign Type",
    "Campaign Delivery Type": "Campaign Delivery Type",
    "Campaign Channel": "Campaign Channel",
    "Campaign Status": "Campaign Status",
    "Campaign Sent Time": None,
    "Conversion Goal 1 Name": None,
    "Conversion Goal 1 Event": None,
    "Total Sent": "Sent",
    "Total Delivered": "Total Delivered",
    "Delivery Rate": "Delivery Rate",
    "Total Open": None,
    "Open Rate": None,
    "Total clicks": "Clicks",
    "Unique clicks": "Unique clicks",
    "CTOR": None,
    "CTR": "CTR",
    "View Through Converted Users": None,
    "Click Through Converted Users":None,
    "Goal 1 View Through Converted Users": "View Through Converted Users",
    "Goal 1 Click Through Converted Users": "Click Through Converted Users",
    "Message title /subject line/Header": None,
    "Content/Body": "Campaign Message",
    "Footer": None,
    "Hard bounces": None,
    "Whatsapp Button 1 Text": None,
    "Total Hard bounces": None,
    "Hard bounce rate": None,
    "Soft bounces": None,
    "Total Soft bounces": None,
    "Soft bounce rate": None,
    "Unsubscribes": None,
    "Unsubscribe rate": None,
    "Complaints": None,
    "Complaints rate": None,
    "Drops": None,
    "Campaign Connector": "Campaign Connector",
    "Billable Count": "Billable Count"
},
    
    "_FLOWS_WHATSAPP_": {
    "Campaign ID": "Campaign ID",
    "Campaign Name": "Campaign Name",
    "Flows Name": "Flows Name",
    "Parent Campaign ID": None,
    "Campaign Type": "Campaign Type",
    "Campaign Delivery Type": "Campaign Delivery Type",
    "Campaign Channel": "Campaign Channel",
    "Campaign Status": "Campaign Status",
    "Campaign Sent Time": None,
    "Conversion Goal 1 Name": None,
    "Conversion Goal 1 Event": None,
    "Total Sent": "Total Sent",
    "Total Delivered": "Total Delivered",
    "Delivery Rate": "Delivery Rate",
    "Total Open": "Total Read",
    "Open Rate": "Read Rate",
    "Total clicks": "Total clicks",
    "Unique clicks": "Unique clicks",
    "CTOR": "CTOR",
    "CTR": "CTR",
    "View Through Converted Users": "Goal 1 View Through Converted Users",
    "Click Through Converted Users":"Goal 1 Click Through Converted Users",
    "Message title /subject line/Header": "Header",
    "Content/Body": "Body",
    "Footer": None,
    "Hard bounces": None,
    "Whatsapp Button 1 Text": "Whatsapp Button 1 Text",
    "Total Hard bounces": None,
    "Hard bounce rate": None,
    "Soft bounces": None,
    "Total Soft bounces": None,
    "Soft bounce rate": None,
    "Unsubscribes": None,
    "Unsubscribe rate": None,
    "Complaints": None,
    "Complaints rate": None,
    "Drops": None,
    "Campaign Connector": None,
    "Billable Count": None
},
    
    "_FLOWS_PUSH_": {
    "Campaign ID": "Campaign ID",
    "Campaign Name": "Campaign Name",
    "Flows Name": "Flows Name",
    "Parent Campaign ID": None,
    "Campaign Type": "Campaign Type",
    "Campaign Delivery Type": "Campaign Delivery Type",
    "Campaign Channel": "Campaign Channel",
    "Campaign Status": "Campaign Status",
    "Campaign Sent Time": None,
    "Conversion Goal 1 Name": None,
    "Conversion Goal 1 Event": None,
    "Total Sent": "All Platform Sent",
    "Total Delivered": "All Platform Impressions",
    "Delivery Rate": "All Platform Impression Rate",
    "Total Open": None,
    "Open Rate": None,
    "Total clicks": "All Platform Clicks",
    "Unique clicks": None,
    "CTOR": "CTOR",
    "CTR": "CTR",
    "View Through Converted Users": None,
    "Click Through Converted Users":None,
    "Message title /subject line/Header": "Android Message Title (Android, Web), Title (iOS)",
    "Content/Body": "Android Message (Android, Web), Subtitle (iOS)",
    "Footer": None,
    "Hard bounces": None,
    "Whatsapp Button 1 Text": None,
    "Total Hard bounces": None,
    "Hard bounce rate": None,
    "Soft bounces": None,
    "Total Soft bounces": None,
    "Soft bounce rate": None,
    "Unsubscribes": None,
    "Unsubscribe rate": None,
    "Complaints": None,
    "Complaints rate": None,
    "Drops": None,
    "Campaign Connector": None,
    "Billable Count": None
},
    
    "_FLOWS_ON_SITE_": {
    "Campaign ID": "Campaign ID",
    "Campaign Name": "Campaign Name",
    "Flows Name": "Flows Name",
    "Parent Campaign ID": "Parent Campaign ID",
    "Campaign Type": "Campaign Type",
    "Campaign Delivery Type": None,
    "Campaign Channel": "Campaign Channel",
    "Campaign Status": "Campaign Status",
    "Campaign Sent Time": "Campaign Sent Time",
    "Conversion Goal 1 Name": "Conversion Goal 1 Name",
    "Conversion Goal 1 Event": "Conversion Goal 1 Event",
    "Total Sent": None,
    "Total Delivered": "All Platform Impressions",
    "Delivery Rate": None,
    "Total Open": None,
    "Open Rate": None,
    "Total clicks": "All Platform Clicks",
    "Unique clicks": None,
    "CTOR": "CTOR",
    "CTR": "All Platform CTR",
    "View Through Converted Users": "Goal 1 View Through Converted Users All Platform",
    "Click Through Converted Users":"Goal 1 Click Through Converted Users All Platform",
    "Message title /subject line/Header": None,
    "Content/Body": None,
    "Footer": None,
    "Hard bounces": None,
    "Whatsapp Button 1 Text": None,
    "Total Hard bounces": None,
    "Hard bounce rate": None,
    "Soft bounces": None,
    "Total Soft bounces": None,
    "Soft bounce rate": None,
    "Unsubscribes": None,
    "Unsubscribe rate": None,
    "Complaints": None,
    "Complaints rate": None,
    "Drops": None,
    "Campaign Connector": None,
    "Billable Count": None
},
    
    "EMAIL": {
    "Campaign ID": "Campaign ID",
    "Campaign Name": "Campaign Name",
    "Flows Name": None,
    "Parent Campaign ID": "Parent Campaign ID",
    "Campaign Type": "Campaign Type",
    "Campaign Delivery Type": "Campaign Delivery Type",
    "Campaign Channel": "Campaign Channel",
    "Campaign Status": None,
    "Campaign Sent Time": "Campaign Sent Time",
    "Conversion Goal 1 Name": None,
    "Conversion Goal 1 Event": None,
    "Total Sent": "Total Sent",
    "Total Delivered": "Total Delivered",
    "Delivery Rate": "Delivery Rate",
    "Total Open": "Total Open",
    "Open Rate": "Open rate",
    "Total clicks": "Total clicks",
    "Unique clicks": "Unique clicks",
    "CTOR": None,
    "CTR": "CTR",
    "View Through Converted Users": "Goal 1 View Through Converted Users",
    "Click Through Converted Users":"Goal 1 Click Through Converted Users",
    "Message title /subject line/Header": "Email Subject",
    "Content/Body": None,
    "Footer": None,
    "Hard bounces": "Hard bounces",
    "Whatsapp Button 1 Text": None,
    "Total Hard bounces": "Total Hard bounces",
    "Hard bounce rate": "Hard bounce rate",
    "Soft bounces": "Soft bounces",
    "Total Soft bounces": "Total Soft bounces",
    "Soft bounce rate": "Soft bounce rate",
    "Unsubscribes": "Unsubscribes",
    "Unsubscribe rate": "Unsubscribe rate",
    "Complaints": "Complaints",
    "Complaints rate": "Complaints rate",
    "Drops": "Drops",
    "Campaign Connector": None,
    "Billable Count": None
},

    "SMS": {
    "Campaign ID": "Campaign ID",
    "Campaign Name": "Campaign Name",
    "Flows Name": None,
    "Parent Campaign ID": "Parent Campaign ID",
    "Campaign Type": "Campaign Type",
    "Campaign Delivery Type": "Campaign Delivery Type",
    "Campaign Channel": "Campaign Channel",
    "Campaign Status": None,
    "Campaign Sent Time": "Campaign Sent Time",
    "Conversion Goal 1 Name": None,
    "Conversion Goal 1 Event": None,
    "Total Sent": "Sent",
    "Total Delivered": "Total Delivered",
    "Delivery Rate": "Delivery Rate",
    "Total Open": None,
    "Open Rate": None,
    "Total clicks": "Clicks",
    "Unique clicks": "Unique clicks",
    "CTOR": None,
    "CTR": "CTR",
    "View Through Converted Users": None,
    "Click Through Converted Users":None,
    "Message title /subject line/Header": None,
    "Content/Body": "Campaign Message",
    "Footer": None,
    "Hard bounces": None,
    "Whatsapp Button 1 Text": None,
    "Total Hard bounces": None,
    "Hard bounce rate": None,
    "Soft bounces": None,
    "Total Soft bounces": None,
    "Soft bounce rate": None,
    "Unsubscribes": None,
    "Unsubscribe rate": None,
    "Complaints": None,
    "Complaints rate": None,
    "Drops": None,
    "Campaign Connector": "Campaign Connector",
    "Billable Count": "Billable Count"
},

    "WHATSAPP": {
    "Campaign ID": "Campaign ID",
    "Campaign Name": "Campaign Name",
    "Flows Name": None,
    "Parent Campaign ID": "Parent Campaign ID",
    "Campaign Type": "Campaign Type",
    "Campaign Delivery Type": "Campaign Delivery Type",
    "Campaign Channel": "Campaign Channel",
    "Campaign Status": None,
    "Campaign Sent Time": "Campaign Sent Time",
    "Conversion Goal 1 Name": None,
    "Conversion Goal 1 Event": None,
    "Total Sent": "Total Sent",
    "Total Delivered": "Total Delivered",
    "Delivery Rate": "Delivery Rate",
    "Total Open": "Total Read",
    "Open Rate": "Read Rate",
    "Total clicks": "Total clicks",
    "Unique clicks": "Unique clicks",
    "CTOR": "CTOR",
    "CTR": "CTR",
    "View Through Converted Users": "Goal 1 View Through Converted Users",
    "Click Through Converted Users":"Goal 1 Click Through Converted Users",
    "Message title /subject line/Header": "Header",
    "Content/Body": "Body",
    "Footer": None,
    "Hard bounces": None,
    "Whatsapp Button 1 Text": "Whatsapp Button 1 Text",
    "Total Hard bounces": None,
    "Hard bounce rate": None,
    "Soft bounces": None,
    "Total Soft bounces": None,
    "Soft bounce rate": None,
    "Unsubscribes": None,
    "Unsubscribe rate": None,
    "Complaints": None,
    "Complaints rate": None,
    "Drops": None,
    "Campaign Connector": None,
    "Billable Count": None
},

    "PUSH": {
    "Campaign ID": "Campaign ID",
    "Campaign Name": "Campaign Name",
    "Flows Name": None,
    "Parent Campaign ID": "Parent Campaign ID",
    "Campaign Type": "Campaign Type",
    "Campaign Delivery Type": "Campaign Delivery Type",
    "Campaign Channel": "Campaign Channel",
    "Campaign Status": None,
    "Campaign Sent Time": "Campaign Sent Time",
    "Conversion Goal 1 Name": None,
    "Conversion Goal 1 Event": None,
    "Total Sent": "All Platform Sent",
    "Total Delivered": "All Platform Impressions",
    "Delivery Rate": "All Platform Impression Rate",
    "Total Open": None,
    "Open Rate": None,
    "Total clicks": "All Platform Clicks",
    "Unique clicks": None,
    "CTOR": None,
    "CTR": "All Platform CTR(Android+iOS)",
    "View Through Converted Users": None,
    "Click Through Converted Users":None,
    "Message title /subject line/Header": "Android Message Title (Android, Web), Title (iOS)",
    "Content/Body": "Android Message (Android, Web), Subtitle (iOS)",
    "Footer": None,
    "Hard bounces": None,
    "Whatsapp Button 1 Text": None,
    "Total Hard bounces": None,
    "Hard bounce rate": None,
    "Soft bounces": None,
    "Total Soft bounces": None,
    "Soft bounce rate": None,
    "Unsubscribes": None,
    "Unsubscribe rate": None,
    "Complaints": None,
    "Complaints rate": None,
    "Drops": None,
    "Campaign Connector": None,
    "Billable Count": None
},
    "In-App": {
    "Campaign ID": "Campaign ID",
    "Campaign Name": "Campaign Name",
    "Flows Name": None,
    "Parent Campaign ID": "Parent Campaign ID",
    "Campaign Type": "Campaign Type",
    "Campaign Delivery Type": None,
    "Campaign Channel": "Campaign Channel",
    "Campaign Status": None,
    "Campaign Sent Time": "Campaign Start Time",
    "Conversion Goal 1 Name": None,
    "Conversion Goal 1 Event": None,
    "Total Sent": None,
    "Total Delivered": "All Platform Impressions",
    "Delivery Rate": None,
    "Total Open": None,
    "Open Rate": None,
    "Total clicks": "All Platform Clicks",
    "Unique clicks": None,
    "CTOR": None,
    "CTR": "All Platform CTR(Android+iOS)",
    "View Through Converted Users": None,
    "Click Through Converted Users":None,
    "Message title /subject line/Header": None,
    "Content/Body": None,
    "Footer": None,
    "Hard bounces": None,
    "Whatsapp Button 1 Text": None,
    "Total Hard bounces": None,
    "Hard bounce rate": None,
    "Soft bounces": None,
    "Total Soft bounces": None,
    "Soft bounce rate": None,
    "Unsubscribes": None,
    "Unsubscribe rate": None,
    "Complaints": None,
    "Complaints rate": None,
    "Drops": None,
    "Campaign Connector": None,
    "Billable Count": None
},
    "ON_SITE": {
    "Campaign ID": "Campaign ID",
    "Campaign Name": "Campaign Name",
    "Flows Name": None,
    "Parent Campaign ID": "Parent Campaign ID",
    "Campaign Type": "Campaign Type",
    "Campaign Delivery Type": None,
    "Campaign Channel": "Campaign Channel",
    "Campaign Status": "Campaign Status",
    "Campaign Sent Time": "Campaign Sent Time",
    "Conversion Goal 1 Name": None,
    "Conversion Goal 1 Event": None,
    "Total Sent": None,
    "Total Delivered": "All Platform Impressions",
    "Delivery Rate": None,
    "Total Open": None,
    "Open Rate": None,
    "Total clicks": "All Platform Clicks",
    "Unique clicks": None,
    "CTOR": "CTOR",
    "CTR": "All Platform CTR",
    "View Through Converted Users": None,
    "Click Through Converted Users":None,
    "Message title /subject line/Header": None,
    "Content/Body": None,
    "Footer": None,
    "Hard bounces": None,
    "Whatsapp Button 1 Text": None,
    "Total Hard bounces": None,
    "Hard bounce rate": None,
    "Soft bounces": None,
    "Total Soft bounces": None,
    "Soft bounce rate": None,
    "Unsubscribes": None,
    "Unsubscribe rate": None,
    "Complaints": None,
    "Complaints rate": None,
    "Drops": None,
    "Campaign Connector": None,
    "Billable Count": None
}
}


final_dfs = []

for name, df in dataframes.items():
    print(name)
    # Detect channel from dataframe name
    channel = None
    for key in COLUMN_MAPPING.keys():
        if key in name.upper():
            channel = key
            print(key)
            break

    if channel is None:
        continue

    mapping = COLUMN_MAPPING[channel]

    temp = pd.DataFrame()

    for final_col, source_col in mapping.items():
        if source_col in df.columns:
            temp[final_col] = df[source_col]
        else:
            temp[final_col] = None

    # Ensure all final columns exist
    for col in FINAL_COLUMNS:
        if col not in temp.columns:
            temp[col] = None

    temp["Dataframe Name"] = name

    final_dfs.append(temp[FINAL_COLUMNS])

master_df = pd.concat(final_dfs, ignore_index=True)
yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
master_df["Date"] = yesterday
master_df.fillna("", inplace=True)


web_app_url = "https://script.google.com/macros/s/AKfycbzyMmyFWRLa6SvxV47vDlBA2jGGPckkXe_TtdM4KxH-8iFsOve5C-7J3CnxUoDEQMqh/exec"

data_dict = {
    'columns': master_df.columns.tolist(),
    'index': master_df.index.tolist(),
    'data': master_df.values.tolist()
}


sheet_name = 'Sheet1'

payload = {
    'sheet_name': sheet_name,
    'data': data_dict
}

# Make a POST request to the Web App URL
response = requests.post(web_app_url, json=payload)

# Print the response
print(response.text)

#import os
#path= r"C:\Users\sahil_pc\Documents\Python Scripts\ABC"
#os.chdir(path)
#master_df.to_csv("master_df.csv")


            

