from sport80 import SportEighty

new_funcs = SportEighty("https://usaweightlifting.sport80.com")

# Get all events for 2024
all_events = new_funcs.event_index(2024)
print(f"\nFound {len(all_events)} events for 2024")

# Process first 5 events as a test
for i in range(min(100, len(all_events))):
    event = all_events[i]
    print(f"\nFetching results for event {i+1}: {event.get('name', 'Unknown Event')}")
    event_results = new_funcs.event_results(event)
    print(f"Results: {event_results}")
