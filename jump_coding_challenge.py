
import requests
import json
import datetime
import argparse

#   Wealth_management
# Retirement_planning
# Financial_adviser

# Total pageviews across the period
# Average daily pageviews
# The highest traffic day (date + pageviews)
# A “conversion_count” defined as:
# Number of days where pageviews >= average_daily_pageviews * 1.25


  
def call_wikimedia(article='Wealth_management', days=14):
    today = datetime.datetime.today()
    start = today - datetime.timedelta(14)
    start_str = start.strftime("%Y-%m-%d")
    today_str = today.strftime("%Y-%m-%d")
    
    start_str = start_str.replace('-', '')
    today_str = today_str.replace('-', '')
    
    ARTICLE = article
    START = start_str
    END = today_str
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }


    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia.org/all-access/user/{ARTICLE}/daily/{START}/{END}"

    res = requests.get(url, headers=headers).json()

    return res['items']

def calc_metrics(data):

    total_pageviews = 0
    values_dict = []
    conversion_count = 0

    values_dict = []


    for d in data: 

        values_dict.append((d['timestamp'], d['views']))
        total_pageviews += d['views']

    sorted_values = sorted(values_dict, key=lambda x: x[1])

    count = len(values_dict)
    avg = total_pageviews/count
    max = sorted_values[-1]

    for x in sorted_values: 
        if x[1] > avg*1.25: 
            conversion_count += 1

    return total_pageviews, avg, conversion_count, {"date": max[0], "pageviews": max[1]}


parser = argparse.ArgumentParser(description="wikimedia")

parser.add_argument("--days", type=str, nargs="?", help="xxxxxx")
parser.add_argument("--article", type=str, nargs="?", help="xxxxxx")

args = parser.parse_args()
days = args.days
article = args.article
# print(days, article)

data = call_wikimedia(days, article)
metrics = calc_metrics(data)

# print(metrics)

json_body = {
    "event_name":"campaign_spike_conversion", 
    "article": article, 
    "window_days": days, 
    "total_pageviews": metrics[0],
    "avg_daily_pageviews": metrics[1],
    "conversion_count": metrics[2],
    "top_day": metrics[3], 

}

# print(json_body)

out = requests.post("https://postman-echo.com/post", json=json_body)

[print('SUMMARY METRICS:   ', metrics)]
# print()

print("POST REQUEST STATUS CODE: " , out.status_code)

print("SNIPPET OF RESPONSE: ", out.text[:100])
