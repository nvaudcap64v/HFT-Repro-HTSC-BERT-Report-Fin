import os
import time
import requests
import random
import json
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# ---------------------------------------------------------
# EastMoney Research Paper Downloader
# ---------------------------------------------------------
# This script automates the process of downloading research 
# reports from the EastMoney API. It retrieves reports 
# published on the previous working day (Monday to Friday) 
# and stores them as PDF files in the designated reports 
# folder on the server. The program:
# 
# 1. Retrieves a list of reports based on a specified date 
#    range (from the previous working day).
# 2. Downloads each report as a PDF file.
# 3. Saves the reports in the '/home/hft/eastmoney-research-paper/reports' 
#    folder, creating subdirectories as needed.
# 4. Logs errors and issues during execution to a log file 
#    located at '/home/hft/eastmoney-research-paper/logs/debug.log'.
# 
# The script is scheduled to run automatically at 2:00 PM 
# every weekday using cron (Linux task scheduler).
# 
# Dependencies:
# - requests (for making HTTP requests)
# - json (for parsing API response)
# - logging (for error logging)
# - concurrent.futures (for parallel downloading of reports)
# - os, time, random, datetime (for file handling and scheduling)
#
# Usage:
# - The script is designed to be executed on a Linux server.
#
# Jeff He @ Feb. 10
# ---------------------------------------------------------

### Param
log_file_path = 'D:\debug.log'                    # Change this as needed
local_folder = '/home/hft/eastmoney-research-paper/reports'

logger = logging.getLogger()
logger.setLevel(logging.ERROR)

file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.ERROR)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.ERROR)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

def get_current_timestamp():
    timestamp = int(time.time())
    logger.debug(f"Current timestamp: {timestamp}")
    return timestamp
   
def delete_special_character(data):
    special_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
    for i in special_chars:
        data = data.replace(i, '')
    logger.debug(f"Special characters removed from {data}")
    return data

# Notify the status of the job in Feishu
def send_flyingbook_message(message):
    webhook_url = 'self-defined-webhook-url'            # Change this as needed
    headers = {'Content-Type': 'application/json'}
    payload = {
        "msg_type": "text",
        "content": {
            "text": message
        }
    }
    response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))
#   if response.status_code == 200:
#       print("Message sent successfully.")
#   else:
#       print(f"Failed to send message: {response.status_code}")

header_pool = [
    "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14",
    "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Win64; x64; Trident/6.0)",
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11',
    'Opera/9.25 (Windows NT 5.1; U; en)',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',
    'Mozilla/5.0 (compatible; Konqueror/3.5; Linux) KHTML/3.5.5 (like Gecko) (Kubuntu)',
    'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12',
    'Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9',
    "Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.7 (KHTML, like Gecko) Ubuntu/11.04 Chromium/16.0.912.77 Chrome/16.0.912.77 Safari/535.7",
    "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0 "
]
headers = {'User-Agent': random.choice(header_pool)}

def get_page(url):
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    try:
        response = session.get(url, headers=headers)
        
        if response.status_code == 200:
            return response.content.decode("utf-8")
        else:
            logger.error(f"Request failed with status code: {response.status_code} for URL: {url}")
            return -1
    except requests.exceptions.SSLError as e:
        logger.error(f"SSL error occurred while fetching {url}: {e}")
        return -1
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error occurred while fetching {url}: {e}")
        return -1
# In order to escape from the anti-crawler, set wait_time to 10
def download_pdf_report(data_end, retries=3, wait_time=10):
    try:
        report_date = data_end.get('publishDate', None)
        if report_date:
            formatted_date = datetime.strptime(report_date.split(' ')[0], "%Y-%m-%d").strftime("%Y-%m-%d")
        else:
            formatted_date = datetime.now().strftime("%Y-%m-%d")
        
        os.makedirs(local_folder, exist_ok=True)

        excel_name = delete_special_character(data_end['title'])
        excel_orgsname = delete_special_character(data_end['orgSName'])
        industry_name = delete_special_character(data_end['industryName'])

        download_url = f"https://pdf.dfcfw.com/pdf/H3_{data_end['infoCode']}_1.pdf"
        file_name = f"{industry_name}-{excel_name}-{excel_orgsname}.pdf"
        full_path = os.path.join(local_folder, file_name)

        if os.path.isfile(full_path):
            logger.error(f"File already exists: {full_path}")
            return

        for attempt in range(retries):
            try:
                with open(full_path, 'wb') as file:
                    download_pdf = requests.get(download_url)
                    download_pdf.raise_for_status()  
                    file.write(download_pdf.content)
                    break
            except requests.exceptions.RequestException as e:
                logger.error(f"Download failed on attempt {attempt + 1} for {download_url}: {e}")
                if attempt < retries - 1:
                    time.sleep(wait_time)  
                else:
                    logger.error(f"Failed to download after {retries} attempts: {download_url}")
    except AttributeError:
        logger.error('Missing required fields in the data')
    except Exception as e:
        logger.error(f"Error in download_pdf_report: {e}")
# Get research paper from https://data.eastmoney.com/report/stock.jshtml using api
def get_report_list(pageno_num, start_date, end_date, timestamp):
    num_random_7 = random.randint(1000000, 9999999)
    html_url = f'https://reportapi.eastmoney.com/report/list?cb=datatable{num_random_7}&industryCode=*&pageSize=50&industry=*&rating=*&ratingChange=*&beginTime={start_date}&endTime={end_date}&pageNo={pageno_num}&fields=&qType=0&orgCode=*&code=*&rcode=*&_={timestamp}'
    html = get_page(html_url)
    
    if html == -1:
        logger.error("Failed to get the report list data. Retrying...")
        time.sleep(10)
        return get_report_list(pageno_num, start_date, end_date, get_current_timestamp())
    
    if html.startswith(f'datatable{num_random_7}('):
        html1 = html.strip(f'datatable{num_random_7}(')
        html2 = html1.rstrip(')')
        return json.loads(html2)
    else:
        logger.error(f"Failed to parse the report list data from {html_url}")
        return {}
        
def get_total_pages(start_date, end_date):
    data = get_report_list(1, start_date, end_date, get_current_timestamp())
    total_pages = data.get('TotalPage', 1)
    return total_pages

def get_previous_working_day():
    today = datetime.today()
    if today.weekday() == 0:
        prev_day = today - timedelta(days=3)
    else:
        prev_day = today - timedelta(days=1)
    return prev_day.strftime("%Y-%m-%d")
# Using muiti-thread to accelerate the whole process
def here_we_go():
    start_date = get_previous_working_day()
    end_date = start_date
    
    total_pages = get_total_pages(start_date, end_date)
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        
        for page in range(1, total_pages + 1):
            data = get_report_list(page, start_date, end_date, get_current_timestamp())
            reports = data.get('data', [])
            
            for report in reports:
                futures.append(executor.submit(download_pdf_report, report))
        
        for future in as_completed(futures):
            future.result()
            
def main_task():
    here_we_go()
    task_result = "Task completed successfully!"
    send_flyingbook_message(task_result)

if __name__ == '__main__':
    main_task()