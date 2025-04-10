import httpx

response = httpx.get("http://localhost/analyzer/listWind",timeout=30.0)
data = response.json()
print(data)