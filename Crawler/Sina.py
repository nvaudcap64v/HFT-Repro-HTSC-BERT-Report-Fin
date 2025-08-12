import requests
import time
import os
import re
import pandas as pd
from lxml import etree
import random
import datetime
from tqdm import tqdm

# ---------------------------------------------------------
# Financial Analyst Report Scraper and Downloader
# ---------------------------------------------------------
# This script automates the process of scraping financial 
# analyst reports from the Sina Finance website. It retrieves 
# reports published within a specified date range and saves 
# them as CSV files, containing details like stock codes, 
# broker names, report titles, URLs, and the researchers' names.
#
# The program:
#
# 1. Loads the stock codes and company names from an existing 
#    Excel file to map companies to their respective stock codes.
# 2. Scrapes financial analyst reports for each day within a 
#    specified date range.
# 3. Downloads the reports and saves them in a designated folder 
#    ('/Reports'), organizing them by report date and type.
# 4. Handles errors and network failures by retrying requests 
#    with an exponential backoff mechanism.
# 5. Uses custom filters to download specific report types 
#    (e.g., "个股", "公司", "创业板").
#
# The script is designed to run on a server or local machine 
# with access to the internet and can be scheduled to run 
# periodically.
#
# Dependencies:
# - requests (for making HTTP requests)
# - lxml (for parsing HTML responses)
# - pandas (for handling data and saving it in CSV format)
# - tqdm (for displaying progress bars)
# - os, time, random, datetime (for file handling, scheduling, 
#   and request throttling)
#
# Usage:
# - The script is intended to be executed as a standalone 
#   Python program.
#
# Jeff He @ Feb. 10
# ---------------------------------------------------------

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
    "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0",
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0'
]
HEADERS = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
           'User-Agent': random.choice(header_pool)}

### Parameters
start_date = "2023-09-01"
end_date = "2023-12-31"
here_to_save = r"D:\Reports"
essential_comp_1_directory = r"D:\basicInfo.xlsx"
           
df = pd.read_excel(essential_comp_1_directory, header=None, names=["stock_code", "company_name"])
name_to_code = df.set_index('company_name')['stock_code'].to_dict()

def scrape_page(URL, HEADERS):
    result = retry_on_failure(
        lambda: requests.get(URL, headers=HEADERS, timeout=10).text)
    parsed_html = etree.HTML(result)
    return parsed_html

def unpack_and_standarise_response(parsed_html):
    try:
        file_url = parsed_html.xpath('//td[@class="tal f14"]/a/@href')
        file_url = [f"https:{url}" for url in file_url]
        file_title = parsed_html.xpath('//td[@class="tal f14"]/a/@title')
        file_type = parsed_html.xpath('//div[@class="main"]/table/tr/td[3]/text()')
        file_broker = parsed_html.xpath('//div[@class="main"]/table/tr/td[5]/a/div/span/text()')
        file_researcher = parsed_html.xpath('//div[@class="main"]/table/tr/td[6]/div/span/text()')
    except Exception as e:
        tqdm.write(f"unpack failed, exception：{e}")
        return []
    
    file_id = []
    for ty, ti in list(zip(*(file_type, file_title))):
        if ty in ["公司", "创业板"]:
            file_id.append(find_stock_code(ti))
        else:
            file_id.append(ty)

    file_info = [file_id, file_url, file_title, file_type, file_broker, file_researcher]
    file_info = list(zip(*file_info))
    return file_info

def find_stock_code(input_string):
    stock_code_pattern = re.compile(r'\d{6}')
    stock_code_match = stock_code_pattern.search(input_string)
    if stock_code_match:
        matched_code = stock_code_match.group()
        if matched_code in df['stock_code'].values:
            return matched_code
        return stock_code_match.group()

    for company_name in name_to_code:
        if company_name in input_string:
            return name_to_code[company_name]

    return None

class DateProcesser:
    def __init__(self, reportDate, saving_path, customerized_type=["个股"]):
        self.reportDate = reportDate
        self.csv_index = ["股票代码", "券商简称", "发布日期", "研报标题", "报告链接", "研报文本", "研究员"]
        self.saving_path = f"{saving_path}"
        self.customerized_type = customerized_type

    def process_page_for_downloads(self):
        exist_urls = []
        tqdm.write(f"start to process {self.reportDate}")
        for dt in ["个股"]:
            saving_file = f"{self.saving_path}\\分析师{dt}报告\\{self.reportDate[:7]}.csv"
            if not os.path.exists(saving_file):
                df = pd.DataFrame(columns=self.csv_index)
                df.to_csv(saving_file, index=False)
            df = pd.read_csv(saving_file, encoding='utf-8-sig',
                             encoding_errors="ignore", dtype=str)
            exist_urls += [str(row["报告链接"]) for index, row in df.iterrows()]

        first_url = f'https://stock.finance.sina.com.cn/stock/go.php/vReport_List/kind/search/index.phtml?t1=6&symbol=&p=1&pubdate={self.reportDate}'
        parsed_html_first = scrape_page(first_url, HEADERS)
        total_pages = parsed_html_first.xpath('//a[text()="最末页"]/@onclick')
        if not total_pages:
            tqdm.write(f"{self.reportDate} is empty, neglected")
            return
        total_pages = total_pages[0].split("set_page_num(\'")[1].split("\')")[0]
        total_pages = int(total_pages)
        for pageNum in tqdm(range(total_pages), desc=f"{self.reportDate}"):
            URL = f'https://stock.finance.sina.com.cn/stock/go.php/vReport_List/kind/search/index.phtml?t1=6&symbol=&p={pageNum + 1}&pubdate={self.reportDate}'
            parsed_html = scrape_page(URL, HEADERS)
            file_info = unpack_and_standarise_response(parsed_html)
            for files in file_info:
                self.download_file(files, exist_urls)

    def download_file(self, files, exist_urls):
        (file_id, url, title, type, broker, researcher) = files
        file_short_name = f"{file_id}_{broker}_{self.reportDate}"
        if self.customerized_type == ["个股"]:
            self.customerized_type = ["公司", "创业板"]
        else:
            self.customerized_type = self.customerized_type
        if type not in self.customerized_type:
#           tqdm.write(f"\t{file_short_name}：is not the pre-defined type")
            return
        if type in ["公司", "创业板"]:
            downloading_type = "个股"
        else:
            downloading_type = type
        saving_file = f"{self.saving_path}\\分析师{downloading_type}报告\\{self.reportDate[:7]}.csv"
        if not os.path.exists(os.path.dirname(saving_file)):
            os.makedirs(os.path.dirname(saving_file))
        if url in exist_urls:
            return

        csv_info_list = [file_id, broker, self.reportDate, title, url, [], researcher]
        csv_info_list[5] = get_file_content(url)
        df = pd.DataFrame([csv_info_list], columns=self.csv_index)
        df.to_csv(saving_file, mode='a', header=False, index=False)
        
        time.sleep(random.uniform(1, 3.5))
        
def retry_on_failure(func, retries=3, pause_time=3):
    attempt = 0
    while attempt < retries:
        try:
            result = func()
            return result
        except Exception as e:
            attempt += 1
            tqdm.write(f"request failed, retry count : {attempt}. Exception: {e}")
            time.sleep(pause_time)
    raise Exception("reached maximum limit, abandoning request...")
 
def create_date_intervals(start_date="2000-01-01", end_date=None) -> list:
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    if end_date is None:
        end = datetime.datetime.today()
    else:
        end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    date_intervals = []
    current_date = start
    while current_date <= end:
        date_intervals.append(current_date.strftime("%Y-%m-%d"))
        current_date += datetime.timedelta(days=1)
    return date_intervals

def get_file_content(url):
    repeat_times = 1
    while repeat_times <= 4:
        try:
            file_content = retry_on_failure(lambda:
                                            requests.get(url, headers=HEADERS, timeout=10).text)
            file_content = etree.HTML(file_content).xpath('//div[@class="blk_container"]/p//text()')
            file_content = [f.strip() for f in file_content]
            if file_content:
                return "\n".join(file_content)
            else:
                raise ValueError(f"empty response: {url}")
        except Exception as e:
            t = random.uniform(2, 5) * repeat_times
            tqdm.write(f"request failed when fetching {url}，exception：{e}，pause {t} sec(s)")
            time.sleep(t)
            repeat_times += 1
    tqdm.write(f"retry failed, file @ {url} is not fetched")
    return ""
    
def here_we_go():
    report_dates = create_date_intervals(start_date, end_date)
    for date in report_dates:
        processor = DateProcesser(reportDate=date, saving_path=r"D:\Reports")
        processor.process_page_for_downloads()
        

if __name__ == "__main__":
    here_we_go()
