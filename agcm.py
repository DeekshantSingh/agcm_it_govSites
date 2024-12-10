import requests
from parsel import Selector
from concurrent.futures import ThreadPoolExecutor
import re
from datetime import datetime
import os
import pandas as pd
from threading import Lock

cookies = {
    'JSESSIONID': 'FB738C655D9978ECE254DC22392D479E',
    'opvc': '7df5dce6-a1b9-4d4a-83a6-ca9b917500ba',
    'sitevisitscookie': '1',
    'dmid': 'e9ab4a6b-d2ea-4e96-a647-1b80d4b09222',
    '_ga': 'GA1.1.1054392035.1733721777',
    '_ga_7HGDDEST1V': 'GS1.1.1733721776.1.1.1733722439.0.0.0',
}

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'content-type': 'application/x-www-form-urlencoded',
    # 'cookie': 'JSESSIONID=FB738C655D9978ECE254DC22392D479E; opvc=7df5dce6-a1b9-4d4a-83a6-ca9b917500ba; sitevisitscookie=1; dmid=e9ab4a6b-d2ea-4e96-a647-1b80d4b09222; _ga=GA1.1.1054392035.1733721777; _ga_7HGDDEST1V=GS1.1.1733721776.1.1.1733722439.0.0.0',
    'origin': 'https://en.agcm.it',
    'priority': 'u=0, i',
    'referer': 'https://en.agcm.it/en/media/press-releases/index',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

data = {
    'searchword': '',
    'separatore': '0',
    'anno': '',
    'limit': '0',
    'page': 'page',
    'filter_field': '',
    'order_value': '',
}
all_data = []
data_lock = Lock()
# Create a requests session for connection pooling
session = requests.Session()
session.cookies.update(cookies)
session.headers.update(headers)
def save_to_excel(filename='agcm_gov.xlsx'):
    """Save collected data to an Excel file."""
    df = pd.DataFrame(all_data).fillna('N/A')
    df.insert(0, 'id', range(1, len(df) + 1))
    os.makedirs('./output', exist_ok=True)
    df.to_excel(f'./output/{filename}', index=False, engine='openpyxl')
    print(f"Data saved to ./output/{filename}")

def extract_penalty_amounts(penalty_paragraphs):

    keywords = ['penalty', 'penalties', 'fine', 'fines', 'fined']
    pattern = r'(S?\$|EURO|€|EUR|EUROS|eur|euro|euros)\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(million|billion|trillion|quadrillion|quintillion)?|(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(million|billion|trillion|quadrillion|quintillion)?\s*(EURO|EUR|EUROS|euro|€|eur|euros)\s*'

    penalty_amounts = []
    for paragraph in penalty_paragraphs:
        if any(keyword in paragraph.lower() for keyword in keywords):
            matches = re.findall(pattern, paragraph, re.IGNORECASE)
            for match in matches:
                prefix = f"{match[0]} {match[1]}" if match[0] else f"{match[3]} {match[4]}"
                suffix = f" {match[2]}" if match[2] else ""
                penalty_amounts.append(f"{prefix}{suffix}".strip())

    return penalty_amounts


def convert_date_to_iso(date_str):
    try:
        date_obj = datetime.strptime(date_str, "%d/%m/%Y")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError as e:
        return f"Invalid date format: {e}"

def fetched_data(page_url, date):
    """Fetch and parse individual news data."""
    home_page = 'https://en.agcm.it'
    full_url = home_page + page_url

    try:
        # Retry mechanism
        for attempt in range(3):
            response2 = session.get(full_url, timeout=10)
            if response2.status_code == 200:
                break
        else:
            print(f"Failed to fetch {full_url} after 3 attempts.")
            return

        parsed_data2 = Selector(response2.text)
        news_heading = parsed_data2.xpath('//div[@id="div_print"]//h3//text()').getall()
        news_heading = ' '.join(news_heading).strip()

        news_details = [txt.strip() for txt in parsed_data2.xpath('//div[@id="corpocom"]//text()').getall()]
        val = extract_penalty_amounts(news_details)
        keywords = ('Eur', 'Euros', 'EURO', 'EUR', 'EUROS', '€','euro')
        val2 = [m if any(keyword in m for keyword in keywords) else f"{m} Euro" for m in val]
        penalty_amounts = '|'.join(val2)
        news_details = ' '.join(news_details).strip()
        external_url = ''
        pdf_url = parsed_data2.xpath('//div[@id="corpocom"]//li//a//@href').get()
        if pdf_url and pdf_url.endswith('.pdf'):
            external_url = pdf_url if 'http' in pdf_url else home_page + pdf_url

        # Append data safely with lock
        with data_lock:
            all_data.append({
                "news_url": full_url,
                "news_date": convert_date_to_iso(date) or 'N/A',
                "news_heading": news_heading or 'N/A',
                "penalty_amount": penalty_amounts or 'N/A',
                "news_details": news_details or 'N/A',
                "pdf_url": external_url or 'N/A'
            })
    except Exception as e:
        print(f"Error processing {full_url}: {e}")

# Fetch main page data
response = session.post('https://en.agcm.it/en/media/press-releases/index', data=data)
parsed_data = Selector(response.text)

news_date = parsed_data.xpath('//table[@class="table"]//td//i//text()').getall()
news_url = parsed_data.xpath('//table[@class="table"]//td//a//@href').getall()

# Multithreading with error handling
with ThreadPoolExecutor(max_workers=8) as executor:
    executor.map(lambda args: fetched_data(*args), zip(news_url, news_date))

# save_to_excel()
