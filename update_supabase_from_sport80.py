# update_supabase_from_sport80.py
import os
import requests
import logging
from datetime import datetime, timezone

# Assuming your sport80 library is in a package named 'sport80_scraper'
# located in the same parent directory as this script, or installed.
# If sport80.py and its package are in the current directory:
# from sport80_scraper import SportEighty
# If sport80.py is directly in the same directory (not as a package):
# from sport80 import SportEighty
from sport80 import SportEighty # Adjust if your structure differs

# --- Configuration ---
# Supabase Configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_TABLE_NAME = "lifting_results" # Your table name
# Column in Supabase that stores the unique meet name
SUPABASE_MEET_NAME_COLUMN = "meet"

# Sport80 Configuration
USAW_DOMAIN = "https://usaweightlifting.sport80.com"

# --- Logging Setup ---
# GitHub Actions will capture stdout/stderr, so basic config is usually fine.
# The sport80 library also has its own logging/print statements.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


def get_nested_value(data_dict, primary_key, column_name=None, sub_key="value"):
    """
    Helper to get values from potentially nested Sport80 data.
    Example: data_dict.get('columns', {}).get('Event', {}).get('value')
    Or: data_dict.get('name')
    """
    if column_name and "columns" in data_dict:
        return data_dict.get("columns", {}).get(column_name, {}).get(sub_key)
    return data_dict.get(primary_key)


def parse_event_date(event_data_dict):
    """
    Tries to parse a date string from event data into a datetime object.
    Sport80 API might return dates in various fields or formats.
    """
    date_str = get_nested_value(event_data_dict, "date", "Start Date") or \
               get_nested_value(event_data_dict, "start_date")

    if not date_str:
        return datetime.min.replace(
            tzinfo=timezone.utc
        ) # Default for sorting

    possible_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S", # Common UK/EU format
        "%d/%m/%Y",
        "%m/%d/%Y %H:%M:%S", # Common US format
        "%m/%d/%Y",
    ]
    for fmt in possible_formats:
        try:
            # Take only date part if time is included
            dt_str_part = str(date_str).split(" ")[0]
            dt = datetime.strptime(dt_str_part, fmt)
            return dt.replace(tzinfo=timezone.utc) # Assume UTC
        except (ValueError, TypeError):
            continue
    logging.warning(f"Could not parse date string: {date_str} for event.")
    return datetime.min.replace(tzinfo=timezone.utc)


def fetch_existing_data_from_supabase() -> tuple:
    """Fetch existing event IDs and meet names from the Supabase database."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        logging.error("Supabase URL or Key not configured.")
        return set(), set()

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE_NAME}?select=event_id,{SUPABASE_MEET_NAME_COLUMN}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        
        event_ids = set()
        meet_names = set()
        
        logging.info("Fetching existing data from Supabase...")
        data_from_db = resp.json()
        if not data_from_db:
            logging.info("No existing data found in Supabase table.")
            return set(), set()

        for i, row in enumerate(data_from_db):
            event_id_val = row.get("event_id")
            if event_id_val: # Ensure it's not None or empty
                event_ids.add(str(event_id_val).strip()) # Ensure string and strip whitespace
                if i < 5: # Log first 5 event_ids loaded
                    logging.info(f"  Loaded event_id from DB: '{str(event_id_val).strip()}' (type: {type(str(event_id_val).strip())})")
            
            meet_name_val = row.get(SUPABASE_MEET_NAME_COLUMN)
            if meet_name_val:
                meet_names.add(str(meet_name_val).lower().strip())
        
        logging.info(f"Loaded {len(event_ids)} unique event IDs and {len(meet_names)} unique meet names from database (after processing)")
        return event_ids, meet_names
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching existing data from Supabase: {e}")
        return set(), set()
    except ValueError: # JSONDecodeError
        logging.error(f"Error decoding JSON from Supabase: {resp.text if resp else 'No response'}")
        return set(), set()


def add_meet_results_to_supabase(results_to_insert: list):
    """Insert a batch of results for a new meet into Supabase."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        logging.error("Supabase URL or Key not configured for adding results.")
        return None
    if not results_to_insert:
        logging.info("No results to insert.")
        return None

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE_NAME}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    try:
        resp = requests.post(url, headers=headers, json=results_to_insert, timeout=60)
        resp.raise_for_status()
        logging.info(f"Successfully inserted {len(results_to_insert)} results via Supabase API.")
        return resp
    except requests.exceptions.RequestException as e:
        logging.error(f"Error inserting meet results to Supabase: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Supabase response content: {e.response.text}")
        return None


def fetch_recent_events_from_sport80(api_client: SportEighty, num_events: int = 30) -> list:
    """
    Fetches recent events from Sport80 from current and previous year,
    sorts them by date, and returns the most recent 'num_events'.
    """
    all_event_dictionaries = []
    current_year = datetime.now(timezone.utc).year
    years_to_check = [current_year, current_year - 1]

    for year in years_to_check:
        try:
            logging.info(f"Fetching event index from Sport80 for year {year}...")
            # SportEighty.event_index returns a dict: {0: event1, 1: event2, ...}
            events_dict_for_year = api_client.event_index(year=year)
            if isinstance(events_dict_for_year, dict):
                all_event_dictionaries.extend(list(events_dict_for_year.values()))
                logging.info(f"Fetched {len(events_dict_for_year)} event items for {year}.")
            else:
                logging.warning(
                    f"event_index for {year} did not return a dict: {type(events_dict_for_year)}"
                )
        except Exception as e:
            logging.error(f"Error fetching Sport80 events for year {year}: {e}", exc_info=True)
            continue

    if not all_event_dictionaries:
        logging.warning("No events fetched from Sport80.")
        return []

    all_event_dictionaries.sort(key=parse_event_date, reverse=True)
    
    logging.info(f"Total event items fetched and sorted: {len(all_event_dictionaries)}")
    return all_event_dictionaries[:num_events]


def fetch_meet_results_from_sport80(api_client: SportEighty, event_data_dict: dict) -> list:
    """
    Fetches results for a specific event.
    The event_data_dict MUST contain the necessary structure for event_id extraction
    (i.e., event_data_dict['action'][0]['route']) as used by the library.
    """
    meet_name_for_log = get_nested_value(event_data_dict, "name", "Event") or "Unknown Event"
    try:
        # Check if the critical 'action' key exists for ID extraction by the library
        if not (isinstance(event_data_dict.get("action"), list) and \
                len(event_data_dict["action"]) > 0 and \
                isinstance(event_data_dict["action"][0], dict) and \
                "route" in event_data_dict["action"][0]):
            logging.error(f"Event data for '{meet_name_for_log}' is missing 'action':'route' structure needed for fetching results. Skipping.")
            logging.debug(f"Problematic event_data_dict: {str(event_data_dict)[:500]}") # Log part of the dict
            return []

        logging.info(f"Fetching results for meet: {meet_name_for_log}")
        # SportEighty.event_results returns a dict: {0: result1, 1: result2, ...}
        results_dict = api_client.event_results(event_dict=event_data_dict)
        if isinstance(results_dict, dict):
            return list(results_dict.values()) # Return list of result row dictionaries
        logging.warning(f"event_results for {meet_name_for_log} did not return a dict: {type(results_dict)}")
        return []
    except Exception as e:
        logging.error(f"Error fetching results for {meet_name_for_log}: {e}", exc_info=True)
        return []


def fetch_max_id_from_supabase() -> int:
    """Fetch the highest ID value from the Supabase database."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        logging.error("Supabase URL or Key not configured.")
        return 0

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE_NAME}?select=id&order=id.desc&limit=1"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        results = resp.json()
        if results and len(results) > 0:
            return results[0]["id"]
        return 0
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching max ID from Supabase: {e}")
        return 0
    except (ValueError, KeyError, IndexError) as e:
        logging.error(f"Error processing max ID from Supabase: {e}")
        return 0


def main():
    logging.info("Starting Sport80 to Supabase sync process...")

    if not SUPABASE_URL or not SUPABASE_KEY:
        logging.critical("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set. Exiting.")
        return

    event_ids, meet_names = fetch_existing_data_from_supabase()
    logging.info(f"Found {len(event_ids)} unique event IDs and {len(meet_names)} unique meet names in Supabase.")
    
    # Fetch the highest existing ID to ensure we don't create duplicates
    max_id = fetch_max_id_from_supabase()
    next_id = max_id + 1
    logging.info(f"Highest existing ID in database: {max_id}. Next ID will be: {next_id}")

    # Initialize SportEighty client
    # Set debug in SportEighty to logging.DEBUG for more verbose output from the client if needed
    # Note: Your library uses print() for some debugs, which won't be controlled by this logging level.
    sport80_api = SportEighty(subdomain=USAW_DOMAIN, return_dict=True, debug=logging.WARNING)

    recent_events_from_sport80 = fetch_recent_events_from_sport80(sport80_api, num_events=1)
    if not recent_events_from_sport80:
        logging.info("No recent events fetched from Sport80. Exiting.")
        return
    logging.info(f"Fetched {len(recent_events_from_sport80)} most recent event items from Sport80.")

    new_events_processed_count = 0
    for event_data in recent_events_from_sport80:
        # Extract meet name - prioritize 'columns' then direct key
        meet_name = get_nested_value(event_data, "name", "Event") or \
                    get_nested_value(event_data, "title") or \
                    get_nested_value(event_data, "meet") # Add this line to check for 'meet' field

        # Extract a unique event identifier for your DB (if needed beyond meet_name)
        # The library uses event_data['action'][0]['route'].split('/')[-1] as an internal ID.
        # You might use event_data.get('id') if it's a stable display ID.
        db_event_id_str = None
        try:
            # Ensure the extracted ID is a string and stripped of whitespace
            db_event_id_str = str(event_data['action'][0]['route'].split('/')[-1]).strip()
        except (KeyError, IndexError, TypeError):
            db_event_id_str = str(event_data.get("id", "N/A")).strip()

        logging.info(f"Processing event: name='{meet_name}', db_event_id_str='{db_event_id_str}' (type: {type(db_event_id_str)})")

        if not meet_name:
            logging.warning(f"Skipping event due to missing name. Event ID: {db_event_id_str}. Data (first 100 chars): {str(event_data)[:100]}")
            continue

        # Check for duplicates, prioritizing event ID checking first as it's more reliable
        is_event_id_in_db = db_event_id_str in event_ids
        logging.info(f"Is event_id '{db_event_id_str}' in loaded event_ids set? {is_event_id_in_db}")
        if is_event_id_in_db:
            logging.info(f"Meet with ID '{db_event_id_str}' already exists in Supabase (ID match). Skipping.")
            continue
            
        # Also check by name as a backup
        is_meet_name_in_db = meet_name.lower().strip() in meet_names
        logging.info(f"Is meet_name '{meet_name.lower().strip()}' in loaded meet_names set? {is_meet_name_in_db}")
        if is_meet_name_in_db:
            logging.info(f"Meet '{meet_name}' already exists in Supabase (name match). Skipping.")
            continue

        logging.info(f"Processing new meet: '{meet_name}' (Event ID for DB: {db_event_id_str})")

        detailed_results_list = fetch_meet_results_from_sport80(sport80_api, event_data)

        if not detailed_results_list:
            logging.info(f"No detailed results found or fetched for meet: '{meet_name}'.")
            # Optional: Add meet_name to meet_names to prevent re-check if it truly has no results
            # meet_names.add(meet_name) # Be cautious with this if results might appear later
            continue

        formatted_results_for_supabase = []
        meet_date_obj = parse_event_date(event_data)
        meet_date_for_db = meet_date_obj.strftime("%Y-%m-%d") if meet_date_obj > datetime.min.replace(tzinfo=timezone.utc) else None

        for result_item in detailed_results_list:
            # Extract data for each field, checking 'columns' then direct keys
            # Adjust these keys based on actual data structure from your Sport80 instance
            lifter_name = get_nested_value(result_item, "lifter", "Athlete") or \
                          get_nested_value(result_item, "name", "Name")
            age_cat = get_nested_value(result_item, "age_category", "Age Category") or \
                      get_nested_value(result_item, "age", "Age") # 'age' is from your old script
            body_w = get_nested_value(result_item, "body_weight_kg", "Bodyweight") or \
                     get_nested_value(result_item, "body_weight_(kg)") # from your old script

            sn1 = get_nested_value(result_item, "snatch_lift_1", "Snatch 1")
            sn2 = get_nested_value(result_item, "snatch_lift_2", "Snatch 2")
            sn3 = get_nested_value(result_item, "snatch_lift_3", "Snatch 3")
            best_sn = get_nested_value(result_item, "best_snatch", "Best Snatch")
            
            cj1 = get_nested_value(result_item, "cj_lift_1", "Clean & Jerk 1") or \
                  get_nested_value(result_item, "c&j_lift_1") # from your old script
            cj2 = get_nested_value(result_item, "cj_lift_2", "Clean & Jerk 2") or \
                  get_nested_value(result_item, "c&j_lift_2")
            cj3 = get_nested_value(result_item, "cj_lift_3", "Clean & Jerk 3") or \
                  get_nested_value(result_item, "c&j_lift_3")
            best_cj = get_nested_value(result_item, "best_cj", "Best Clean & Jerk") or \
                      get_nested_value(result_item, "best_c&j")

            total_lifted = get_nested_value(result_item, "total", "Total")

            formatted_results_for_supabase.append({
                "id": next_id,  # Use the next available ID
                "event_id": db_event_id_str,
                SUPABASE_MEET_NAME_COLUMN: meet_name,
                "date": meet_date_for_db,
                "name": lifter_name,
                "age": age_cat, # Changed from 'age_category' to 'age' to match the database schema
                "body_weight": body_w,
                "snatch1": sn1,
                "snatch2": sn2,
                "snatch3": sn3,
                "snatch_best": best_sn,
                "cj1": cj1,
                "cj2": cj2,
                "cj3": cj3,
                "cj_best": best_cj,
                "total": total_lifted,
            })
            next_id += 1  # Increment the ID for the next record

        if formatted_results_for_supabase:
            add_meet_results_to_supabase(formatted_results_for_supabase)
            new_events_processed_count += 1
            # Add to sets to prevent re-processing in this run
            meet_names.add(meet_name.lower())
            event_ids.add(db_event_id_str)
        else:
            logging.info(f"No results formatted for Supabase for meet: '{meet_name}'.")

    logging.info(f"Finished Sport80 to Supabase sync. Processed and attempted to add results for {new_events_processed_count} new meets.")

if __name__ == "__main__":
    main()
