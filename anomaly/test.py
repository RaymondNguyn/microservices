import httpx
import asyncio
# response = httpx.get("http://localhost/analyzer/listWind",timeout=30.0)
# data = response.json()
# for i in data['Wind_events']:
#     print(i)

# response = httpx.get("http://localhost/analyzer/stats",timeout=60.0)
# data = response.json()
# fmt_response = {
#     "wind_count": data['num_wind'],
#     "temp_count": data['num_temp']
# }
# print(data)

# print(fmt_response)


def compare_storage_analyzer():
    base_url = "localhost"
    
    # Make synchronous requests with proper timeouts
    storage_wind_response = httpx.get(f"http://{base_url}/storage/listWind", timeout=30.0)
    analyzer_wind_response = httpx.get(f"http://{base_url}/analyzer/listWind", timeout=30.0)
    storage_temp_response = httpx.get(f"http://{base_url}/storage/listTemp", timeout=30.0)
    analyzer_temp_response = httpx.get(f"http://{base_url}/analyzer/listTemp", timeout=30.0)
    
    # Parse JSON responses
    storage_wind = storage_wind_response.json()
    analyzer_wind = analyzer_wind_response.json()
    storage_temp = storage_temp_response.json()
    analyzer_temp = analyzer_temp_response.json()

    print(storage_wind)
    print(analyzer_wind)
    
    # Extract all IDs and combine by source
    storage_ids = {event['trace_id'] for event in storage_wind["Wind_events"]}
    storage_ids.update({event['trace_id'] for event in storage_temp["Temp_events"]})
    
    analyzer_ids = {event['trace_id'] for event in analyzer_wind["Wind_events"]}
    analyzer_ids.update({event['trace_id'] for event in analyzer_temp["Temp_events"]})
    
    # Compare combined datasets
    in_analyzer_not_storage = list(analyzer_ids - storage_ids)
    in_storage_not_analyzer = list(storage_ids - analyzer_ids)
    
    return {
        "in_analyzer_not_storage": in_analyzer_not_storage,
        "in_storage_not_analyzer": in_storage_not_analyzer,
        "total_analyzer_events": len(analyzer_ids),
        "total_storage_events": len(storage_ids)
    }

# Run the function and print results
if __name__ == "__main__":
    try:
        result = compare_storage_analyzer()
        print(result)
    except Exception as e:
        print(f"Error: {type(e).__name__}: {str(e)}")