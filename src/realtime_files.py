import requests

DATE_RANGE = []

for month,day,year in DATE_RANGE:
    file_name = f"subwaydatanyc_{year}-{month:02}-{day:02}_csv.tar.xz"
    url = f"https://subwaydata.nyc/data/{file_name}"
    response = requests.get(url)
    with open(file_name, "wb") as f:
        f.write(response.content)