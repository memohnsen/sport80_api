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

# Discord Configuration
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

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


def filter_already_existing_event_ids(candidate_event_ids: list[str]) -> set[str]:
    """Given a list of candidate event IDs, query Supabase to find which ones already exist."""
    if not candidate_event_ids:
        logging.info("No candidate event IDs provided to check for existence.")
        return set()

    if not SUPABASE_URL or not SUPABASE_KEY:
        logging.error("Supabase URL or Key not configured for checking event IDs.")
        return set() # Or raise an error

    # Format for the "in" clause: (id1,id2,id3)
    # Supabase/PostgREST expects a comma-separated list for the `in` filter.
    # Ensure IDs are quoted if they are strings, but event_id seems to be stored as text/string non-quoted in db based on logs.
    # The event_ids extracted are already strings.
    event_ids_str = ",".join(candidate_event_ids)
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE_NAME}?select=event_id&event_id=in.({event_ids_str})"
    
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Accept": "application/json" # Explicitly ask for JSON
    }
    
    existing_ids_in_db = set()
    try:
        logging.info(f"Querying Supabase for existing event_ids: {candidate_event_ids}")
        resp = requests.get(url, headers=headers, timeout=45)
        resp.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
        
        results = resp.json()
        for row in results:
            if "event_id" in row and row["event_id"]:
                existing_ids_in_db.add(str(row["event_id"]).strip())
        logging.info(f"Supabase check found {len(existing_ids_in_db)} existing event IDs: {existing_ids_in_db}")
        return existing_ids_in_db
    except requests.exceptions.RequestException as e:
        logging.error(f"Error querying Supabase for existing event IDs: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Supabase response content for event_id check: {e.response.text}")
        return set() # Return empty set on error, so script might try to re-add
    except ValueError: # JSONDecodeError
        logging.error(f"Error decoding JSON from Supabase event_id check: {resp.text if resp else 'No response'}")
        return set()


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


def send_discord_notification(meets_added_count: int):
    """Send a Discord notification with the number of meets added and timestamp."""
    if not DISCORD_WEBHOOK_URL:
        logging.info("Discord webhook URL not configured. Skipping notification.")
        return

    # Get current timestamp in a readable format
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    
    # Create the message
    message = f"{meets_added_count} Meets Added to Supabase at {current_time}"
    
    payload = {
        "content": message
    }
    
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=30)
        response.raise_for_status()
        logging.info(f"Discord notification sent successfully: {message}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send Discord notification: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Discord webhook response: {e.response.text}")


def main():
    logging.info("Starting Sport80 to Supabase sync process...")

    if not SUPABASE_URL or not SUPABASE_KEY:
        logging.critical("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set. Exiting.")
        return

    sport80_api = SportEighty(subdomain=USAW_DOMAIN, return_dict=True, debug=logging.WARNING)
    # Keeping num_events=1 for this test, can be changed back to 30 later.
    recent_sport80_events_data = fetch_recent_events_from_sport80(sport80_api, num_events=20)

    if not recent_sport80_events_data:
        logging.info("No recent events fetched from Sport80. Exiting.")
        return
    logging.info(f"Fetched {len(recent_sport80_events_data)} event(s) from Sport80.")

    candidate_event_details = []
    for event_data_item in recent_sport80_events_data:
        meet_name = get_nested_value(event_data_item, "meet") # Primary source for meet name from Sport80
        event_id_str = "N/A"
        try:
            event_id_str = str(event_data_item['action'][0]['route'].split('/')[-1]).strip()
        except (KeyError, IndexError, TypeError):
            # Fallback if the primary path for event_id is not found
            event_id_from_data = event_data_item.get("id") # sport80 library might put it here
            if event_id_from_data:
                event_id_str = str(event_id_from_data).strip()
        
        if not meet_name:
            logging.warning(f"Event data missing 'meet' field. Event ID: {event_id_str}. Data: {str(event_data_item)[:200]}")
            # Decide if you want to skip or use a placeholder for meet_name
            # meet_name = f"Unknown Meet (ID: {event_id_str})" # Example placeholder
            # For now, let's rely on later checks to skip if name is truly essential elsewhere

        if event_id_str != "N/A":
            candidate_event_details.append({"id": event_id_str, "name": meet_name, "data": event_data_item})
        else:
            logging.warning(f"Could not extract a valid event_id for event: {meet_name if meet_name else 'Name N/A'}. Data: {str(event_data_item)[:200]}")

    if not candidate_event_details:
        logging.info("No valid candidate events with IDs to process after initial parsing. Exiting.")
        return

    candidate_ids_to_check_in_db = [details["id"] for details in candidate_event_details]
    
    # Query Supabase for which of these candidate IDs already exist
    already_existing_event_ids_in_db = filter_already_existing_event_ids(candidate_ids_to_check_in_db)
    logging.info(f"Checked {len(candidate_ids_to_check_in_db)} candidate event IDs. Found {len(already_existing_event_ids_in_db)} existing in DB: {already_existing_event_ids_in_db}")

    max_id_in_db = fetch_max_id_from_supabase()
    next_id_for_new_rows = max_id_in_db + 1
    logging.info(f"Highest existing primary ID in Supabase table: {max_id_in_db}. Next row ID will start from: {next_id_for_new_rows}")

    processed_event_ids_this_run = set() # To prevent re-processing if Sport80 API sends duplicates in one batch
    new_events_added_count = 0

    for event_details in candidate_event_details:
        current_event_id = event_details["id"]
        current_meet_name = event_details["name"]
        event_data_for_api = event_details["data"]

        logging.info(f"Processing candidate: Meet Name='{current_meet_name}', Event ID='{current_event_id}'")

        if not current_meet_name: # Final check for meet name
            logging.warning(f"Skipping event with ID '{current_event_id}' due to missing meet name after all parsing attempts.")
            continue

        if current_event_id in already_existing_event_ids_in_db:
            logging.info(f"Event ID '{current_event_id}' ('{current_meet_name}') already exists in Supabase (checked via DB query). Skipping.")
            continue
        
        if current_event_id in processed_event_ids_this_run:
            logging.info(f"Event ID '{current_event_id}' ('{current_meet_name}') was already processed in this script run. Skipping.")
            continue

        logging.info(f"Treating as new meet for DB: '{current_meet_name}' (Event ID: {current_event_id})")
        
        detailed_results_list = fetch_meet_results_from_sport80(sport80_api, event_data_for_api)

        if not detailed_results_list:
            logging.warning(f"No detailed results found/fetched for '{current_meet_name}' (ID: {current_event_id}). Adding ID to processed list to prevent re-check this run.")
            processed_event_ids_this_run.add(current_event_id)
            continue

        formatted_results_for_supabase = []
        meet_date_obj = parse_event_date(event_data_for_api)
        meet_date_for_db = meet_date_obj.strftime("%Y-%m-%d") if meet_date_obj > datetime.min.replace(tzinfo=timezone.utc) else None

        for result_item in detailed_results_list:
            lifter_name = get_nested_value(result_item, "lifter", "Athlete") or get_nested_value(result_item, "name", "Name")
            age_cat = get_nested_value(result_item, "age_category", "Age Category") or get_nested_value(result_item, "age", "Age")
            body_w = get_nested_value(result_item, "body_weight_kg", "Bodyweight") or get_nested_value(result_item, "body_weight_(kg)")
            sn1 = get_nested_value(result_item, "snatch_lift_1", "Snatch 1")
            sn2 = get_nested_value(result_item, "snatch_lift_2", "Snatch 2")
            sn3 = get_nested_value(result_item, "snatch_lift_3", "Snatch 3")
            best_sn = get_nested_value(result_item, "best_snatch", "Best Snatch")
            cj1 = get_nested_value(result_item, "cj_lift_1", "Clean & Jerk 1") or get_nested_value(result_item, "c&j_lift_1")
            cj2 = get_nested_value(result_item, "cj_lift_2", "Clean & Jerk 2") or get_nested_value(result_item, "c&j_lift_2")
            cj3 = get_nested_value(result_item, "cj_lift_3", "Clean & Jerk 3") or get_nested_value(result_item, "c&j_lift_3")
            best_cj = get_nested_value(result_item, "best_cj", "Best Clean & Jerk") or get_nested_value(result_item, "best_c&j")
            total_lifted = get_nested_value(result_item, "total", "Total")

            formatted_results_for_supabase.append({
                "id": next_id_for_new_rows,
                "event_id": current_event_id,
                SUPABASE_MEET_NAME_COLUMN: current_meet_name,
                "date": meet_date_for_db,
                "name": lifter_name,
                "age": age_cat,
                "body_weight": body_w,
                "snatch1": sn1, "snatch2": sn2, "snatch3": sn3, "snatch_best": best_sn,
                "cj1": cj1, "cj2": cj2, "cj3": cj3, "cj_best": best_cj,
                "total": total_lifted,
            })
            next_id_for_new_rows += 1

        if formatted_results_for_supabase:
            add_meet_results_to_supabase(formatted_results_for_supabase)
            new_events_added_count += 1
            logging.info(f"Successfully added {len(formatted_results_for_supabase)} results for '{current_meet_name}' (ID: {current_event_id}).")
        else:
            logging.warning(f"No results formatted for Supabase for meet: '{current_meet_name}' (ID: {current_event_id}).")
        
        processed_event_ids_this_run.add(current_event_id) # Add here after attempting to process

    logging.info(f"Finished Sport80 to Supabase sync. Added results for {new_events_added_count} new meet(s).")

    # Send Discord notification
    send_discord_notification(new_events_added_count)

if __name__ == "__main__":
    main()
