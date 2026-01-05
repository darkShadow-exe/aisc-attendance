import re
from datetime import datetime
from typing import Optional, Dict, List, Tuple
import json
import os

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
from dotenv import load_dotenv

load_dotenv()

SPREADSHEET_NAME = "AISC Session Attendance 2025"
SHEET_ATTENDEES = "attendees"
SHEET_SESSIONS = "sessions"
SHEET_ATTENDANCE_LOG = "attendee_log"


def setup_google_sheets() -> gspread.Client:
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "/Users/rajani/Documents/GitHub/aisc_attendance/credentials.json",
        scope
    )
    
    return gspread.authorize(creds)


def validate_names_with_llm(names: List[str]) -> List[str]:
    if not names:
        return []
    
    names_str = "\n".join([f"{i+1}. {name}" for i, name in enumerate(names)])
    
    prompt = f"""You are a name validator. Given the following list of names from a quiz attendance sheet, identify which ones are REAL person names and which are NOT real names.

NOT real names include:
- Placeholder text (e.g., "Hi", "Ho", "Test")
- Single letters or very short text (e.g., "N", "A")
- System text or column headers (e.g., "Average Time per Question")
- Nonsensical combinations

REAL names include:
- Full names (first + last)
- Single names that could be real first names
- Names from any culture/language

Names to validate:
{names_str}

Respond with ONLY a JSON array of the real names. Example: ["John Doe", "Jane Smith"]"""
    
    try:
        api_key = os.getenv("HACKCLUB_API_KEY")
        if not api_key:
            env_path = "/Users/rajani/Documents/GitHub/aisc_attendance/.env"
            if os.path.exists(env_path):
                with open(env_path, 'r') as f:
                    api_key = f.read().strip()
        
        if not api_key:
            print("  No API key found, skipping LLM validation")
            return names
        
        response = requests.post(
            "https://ai.hackclub.com/proxy/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "google/gemini-2.5-flash",
                "messages": [
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.1
            },
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result["choices"][0]["message"]["content"].strip()
            
            if "```" in content:
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
            content = content.strip()
            
            valid_names = json.loads(content)
            return valid_names
        else:
            print(f"  LLM validation failed (status {response.status_code}), keeping all names")
            return names
            
    except Exception as e:
        print(f"  LLM validation error: {e}, keeping all names")
        return names


def parse_quiz_date(date_string: str) -> str:
    date_part = date_string.split(",")[0].strip()
    parsed_date = datetime.strptime(date_part, "%a %d %b %Y")
    return parsed_date.strftime("%Y-%m-%d")


def extract_participant_data(xlsx_file: str) -> List[str]:
    df = pd.read_excel(xlsx_file, sheet_name="Participant Data")
    
    participants = []
    seen_names = set()
    
    for _, row in df.iterrows():
        first_name = str(row.get("First Name", "")).strip()
        last_name = str(row.get("Last Name", "")).strip()
        
        if first_name and first_name != "nan":
            if last_name and last_name != "nan":
                full_name = f"{first_name} {last_name}"
            else:
                full_name = first_name
            
            if full_name not in seen_names:
                participants.append(full_name)
                seen_names.add(full_name)
    
    return participants


def extract_quiz_date(xlsx_file: str) -> str:
    df = pd.read_excel(xlsx_file, sheet_name="Quiz Details")
    
    game_started_row = df[df["Name"] == "Game Started On"]
    
    if game_started_row.empty:
        raise ValueError("Could not find 'Game Started On' in Quiz Details sheet")
    
    date_value = game_started_row.iloc[0]["Value"]
    
    return parse_quiz_date(date_value)


def extract_emails(xlsx_file: str) -> Dict[str, str]:
    df = pd.read_excel(xlsx_file, sheet_name="Overview")
    
    email_row_idx = None
    email_pattern = re.compile(r'e[-\s]?mail?', re.IGNORECASE)
    for idx, row in df.iterrows():
        question = str(row.get("Question", ""))
        if email_pattern.search(question):
            email_row_idx = idx
            break
    
    if email_row_idx is None:
        raise ValueError("Could not find email question in Overview sheet")
    
    email_mapping = {}
    
    for col in df.columns:
        if col == "Question":
            continue
        
        match = re.match(r"^(.+?)\s+\(", col)
        if match:
            full_name = match.group(1).strip()
            
            email_raw = str(df.at[email_row_idx, col])
            
            if "<br>" in email_raw:
                email = email_raw.split("<br>")[0].strip()
            else:
                email = email_raw.strip()
            
            if (email and 
                email != "nan" and 
                "@" in email and 
                " " not in email and
                len(email) > 3):
                email_mapping[full_name] = email
    
    return email_mapping


def get_or_create_attendee(
    sheet: gspread.Worksheet,
    name: str,
    email: str
) -> Optional[int]:
    import time
    
    all_values = sheet.get_all_values()
    
    if len(all_values) <= 1:
        records = []
    else:
        headers = all_values[0]
        records = []
        for row in all_values[1:]:
            record = {}
            for i, header in enumerate(headers):
                record[header] = row[i] if i < len(row) else ""
            records.append(record)
    
    if email:
        for idx, record in enumerate(records):
            existing_email = record.get("email", "").strip().lower()
            if existing_email == email.lower():
                attendee_id = record.get("id", "").strip()
                return int(attendee_id) if attendee_id else None
        
        name_lower = name.lower().strip()
        for idx, record in enumerate(records):
            existing_name = record.get("name", "").strip().lower()
            existing_email = record.get("email", "").strip()
            if existing_name == name_lower and not existing_email:
                row_num = idx + 2
                sheet.update_cell(row_num, 3, email)
                time.sleep(0.5)
                attendee_id = record.get("id", "").strip()
                return int(attendee_id) if attendee_id else None
    
    else:
        name_lower = name.lower().strip()
        for record in records:
            existing_name = record.get("name", "").strip().lower()
            if existing_name == name_lower:
                attendee_id = record.get("id", "").strip()
                return int(attendee_id) if attendee_id else None
    
    sheet.append_row(["", name, email if email else ""])
    
    time.sleep(2)
    
    all_values = sheet.get_all_values()
    
    if len(all_values) <= 1:
        return None
    
    headers = all_values[0]
    last_row = all_values[-1]
    last_record = {}
    for i, header in enumerate(headers):
        last_record[header] = last_row[i] if i < len(last_row) else ""
    
    attendee_id = last_record.get("id", "").strip()
    
    if not attendee_id:
        time.sleep(2)
        all_values = sheet.get_all_values()
        last_row = all_values[-1]
        for i, header in enumerate(headers):
            last_record[header] = last_row[i] if i < len(last_row) else ""
        attendee_id = last_record.get("id", "").strip()
    
    return int(attendee_id) if attendee_id else None


def create_session(
    sheet: gspread.Worksheet,
    url: str,
    title: str,
    date: str
) -> Optional[int]:
    import time
    
    sheet.append_row(["", url, title, date])
    
    time.sleep(2)
    
    all_values = sheet.get_all_values()
    if len(all_values) > 1:
        last_row = all_values[-1]
        session_id = last_row[0].strip() if last_row and last_row[0] else None
        
        if not session_id:
            time.sleep(2)
            all_values = sheet.get_all_values()
            last_row = all_values[-1]
            session_id = last_row[0].strip() if last_row and last_row[0] else None
        
        return int(session_id) if session_id else None
    
    return None


def log_attendance(
    sheet: gspread.Worksheet,
    member_id: int,
    session_id: int
) -> None:
    sheet.append_row([member_id, session_id])


def main():
    print("=" * 60)
    print("AISC Attendance Tracking Automation")
    print("=" * 60)
    print()
    
    xlsx_file = input("Enter path to Wayground XLSX file: ").strip()
    
    print("\nEnter session details:")
    session_url = input("Session URL: ").strip()
    session_title = input("Session title: ").strip()
    
    print("\nProcessing attendance data...")
    
    try:
        participants = extract_participant_data(xlsx_file)
        quiz_date = extract_quiz_date(xlsx_file)
        email_mapping = extract_emails(xlsx_file)
        
        print(f"Found {len(participants)} participants")
        
        print("\nValidating participant names with LLM...")
        valid_participants = validate_names_with_llm(participants)
        filtered_count = len(participants) - len(valid_participants)
        
        if filtered_count > 0:
            print(f"Filtered out {filtered_count} invalid names")
            filtered_names = set(participants) - set(valid_participants)
            for name in filtered_names:
                print(f"  - Removed: {name}")
        
        participants = valid_participants
        print(f"{len(participants)} valid participants after filtering")
        print(f"Quiz date: {quiz_date}")
        print(f"Found {len(email_mapping)} emails")
        
    except Exception as e:
        print(f"Error reading XLSX file: {e}")
        return
    
    # Connect to Google Sheets
    print("\nConnecting to Google Sheets...")
    try:
        client = setup_google_sheets()
        spreadsheet = client.open(SPREADSHEET_NAME)
        
        attendees_sheet = spreadsheet.worksheet(SHEET_ATTENDEES)
        sessions_sheet = spreadsheet.worksheet(SHEET_SESSIONS)
        attendance_log_sheet = spreadsheet.worksheet(SHEET_ATTENDANCE_LOG)
        
        print("Connected successfully")
        
    except Exception as e:
        print(f"Error connecting to Google Sheets: {e}")
        return
    
    # Show summary and ask for confirmation
    print()
    print("=" * 60)
    print("SUMMARY - PLEASE REVIEW")
    print("=" * 60)
    print(f"Session URL:    {session_url}")
    print(f"Session Title:  {session_title}")
    print(f"Session Date:   {quiz_date}")
    print(f"Total Participants: {len(participants)}")
    print(f"Participants with Emails: {len(email_mapping)}")
    print(f"Participants without Emails: {len(participants) - len(email_mapping)}")
    print()
    print("This will:")
    print("  1. Create a new session record")
    print("  2. Add new attendees (if they don't exist)")
    print(f"  3. Log {len(email_mapping)} attendance records")
    print("=" * 60)
    print()
    
    confirmation = input("Proceed with updating Google Sheets? (yes/no): ").strip().lower()
    if confirmation not in ['yes', 'y']:
        print("Operation cancelled by user.")
        return
    
    # Create session
    print("\nCreating session record...")
    try:
        session_id = create_session(
            sessions_sheet,
            session_url,
            session_title,
            quiz_date
        )
        
        if not session_id:
            print("Failed to create session")
            return
        
        print(f"Session created with ID: {session_id}")
        
    except Exception as e:
        print(f"Error creating session: {e}")
        return
    
    # Process attendees and log attendance
    print("\nProcessing attendees and logging attendance...")
    
    attendance_count = 0
    skipped_count = 0
    blank_email_count = 0
    
    for participant_name in participants:
        # Get email for this participant (or empty string if not found/invalid)
        email = email_mapping.get(participant_name, "")
        
        if not email:
            # No valid email found - still create/update attendee with blank email
            print(f"Processing {participant_name} (no valid email - creating with blank email)")
        
        try:
            # Get or create attendee (with or without email)
            member_id = get_or_create_attendee(
                attendees_sheet,
                participant_name,
                email
            )
            
            if not member_id:
                print(f"Failed to get/create attendee for {participant_name}")
                skipped_count += 1
                continue
            
            # Log attendance
            log_attendance(attendance_log_sheet, member_id, session_id)
            
            if email:
                attendance_count += 1
                print(f"Logged attendance for {participant_name}")
            else:
                blank_email_count += 1
                print(f"Logged attendance for {participant_name} (blank email)")
            
        except Exception as e:
            print(f"Error processing {participant_name}: {e}")
            skipped_count += 1
    
    # Summary
    print()
    print("=" * 60)
    print("COMPLETION SUMMARY")
    print("=" * 60)
    print(f"Session: {session_title}")
    print(f"Date: {quiz_date}")
    print(f"Attendance logged (with email): {attendance_count}")
    print(f"Attendance logged (blank email): {blank_email_count}")
    print(f"Failed: {skipped_count}")
    print()
    print("Attendance tracking completed successfully!")


if __name__ == "__main__":
    main()