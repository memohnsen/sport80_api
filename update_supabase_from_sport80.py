import os
import requests
from datetime import datetime
from sport80 import SportEighty

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_ROLE_KEY']

def fetch_existing_meets():
    """Fetch all unique meet names from the database."""
    url = f"{SUPABASE_URL}/rest/v1/lifting_results?select=meet"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return set(row['meet'] for row in resp.json())

def add_meet_results(results):
    """Insert a batch of results for a new meet."""
    url = f"{SUPABASE_URL}/rest/v1/lifting_results"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    resp = requests.post(url, headers=headers, json=results)
    resp.raise_for_status()
    return resp.json()

def fetch_recent_meets_from_sport80(n=30):
    api = SportEighty("https://usaweightlifting.sport80.com")
    all_events = api.event_index(datetime.now().year)
    # Sort by date (descending), take the most recent n
    sorted_events = sorted(
        all_events,
        key=lambda e: e.get('date') or e.get('start_date') or '',
        reverse=True
    )
    return sorted_events[:n]

def fetch_meet_results(api, event):
    try:
        return api.event_results(event)
    except Exception as e:
        print(f"Error fetching results for {event.get('name')}: {e}")
        return []

def main():
    print("Fetching existing meets from Supabase...")
    existing_meets = fetch_existing_meets()
    print(f"Found {len(existing_meets)} meets in DB.")

    print("Fetching recent meets from Sport80...")
    api = SportEighty("https://usaweightlifting.sport80.com")
    recent_events = fetch_recent_meets_from_sport80(30)
    print(f"Fetched {len(recent_events)} recent meets from Sport80.")

    for event in recent_events:
        meet_name = event.get('name')
        if not meet_name:
            continue
        if meet_name in existing_meets:
            print(f"Skipping existing meet: {meet_name}")
            continue
        print(f"Adding new meet: {meet_name}")
        results = fetch_meet_results(api, event)
        formatted_results = []
        for result in results:
            formatted_results.append({
                'event_id': event.get('id'),
                'meet': meet_name,
                'date': event.get('date') or event.get('start_date'),
                'name': result.get('lifter'),
                'age': result.get('age_category'),
                'body_weight': result.get('body_weight_kg') or result.get('body_weight_(kg)'),
                'snatch1': result.get('snatch_lift_1'),
                'snatch2': result.get('snatch_lift_2'),
                'snatch3': result.get('snatch_lift_3'),
                'snatch_best': result.get('best_snatch'),
                'cj1': result.get('cj_lift_1') or result.get('c&j_lift_1'),
                'cj2': result.get('cj_lift_2') or result.get('c&j_lift_2'),
                'cj3': result.get('cj_lift_3') or result.get('c&j_lift_3'),
                'cj_best': result.get('best_cj') or result.get('best_c&j'),
                'total': result.get('total'),
            })
        if formatted_results:
            add_meet_results(formatted_results)
            print(f"Inserted {len(formatted_results)} results for {meet_name}")
        else:
            print(f"No results to insert for {meet_name}")

if __name__ == '__main__':
    main()
