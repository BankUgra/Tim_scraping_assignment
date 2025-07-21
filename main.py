import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy import signals
import base64
import json
import time
import requests
from datetime import timedelta
import threading
import multiprocessing


class ProxySpider(scrapy.Spider):
    name = "proxy_spider"
    start_urls = [
        "https://advanced.name/freeproxy?page=1",
        "https://advanced.name/freeproxy?page=2",
    ]
    proxy_list = []

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def parse(self, response):
        global proxy_http
        for row in response.css('table#table_proxies tbody tr'):
            if len(self.proxy_list) >= 150:
                break

            encoded_ip = row.css('td[data-ip]::attr(data-ip)').get()
            encoded_port = row.css('td[data-port]::attr(data-port)').get()
            if not encoded_ip or not encoded_port:
                continue

            ip = base64.b64decode(encoded_ip).decode()
            port = int(base64.b64decode(encoded_port).decode())
            protocols = row.css('td:nth-child(4) a::text').getall()

            if 'HTTP' in protocols:
                proxy_http.append({"ip": ip, "port": port, "protocols": protocols}) 
            self.proxy_list.append({"ip": ip, "port": port, "protocols": protocols})
            
            yield {"ip": ip, "port": port, "protocols": protocols}

        if len(self.proxy_list) < 150:
            next_page = response.css('ul.pagination li a::attr(href)').get()
            if next_page:
                yield response.follow(next_page, callback=self.parse)

    def spider_closed(self, spider):
        with open("proxies.json", "w", encoding="utf-8") as f:
            json.dump(self.proxy_list, f, ensure_ascii=False, indent=2)
        global proxy_global
        proxy_global = self.proxy_list.copy()

PROXY_THREAD_NUMBER = 3
TOKEN = "t_a3895d42"
BASE_URL = "https://test-rg8.ddns.net"
PAGE_URL = f"{BASE_URL}/task"
API_GET_TOKEN = f"{BASE_URL}/api/get_token"
API_POST_PROXIES = f"{BASE_URL}/api/post_proxies"

HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "DNT": "1",
    "Origin": BASE_URL,
    "Referer": PAGE_URL,
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0.0.0 Safari/537.36"
    ),
    "Content-Type": "application/json",
}


def make_session(proxy_entry=None):
    s = requests.Session()
    s.headers.update(HEADERS)
    if proxy_entry:
        scheme = "http"
        proxy_url = f"{scheme}://{proxy_entry['ip']}:{proxy_entry['port']}"
        s.proxies.update({
            "http": proxy_url,
            "https": proxy_url,
        })
    s.timeout = 10
    return s

def authenticate(sess):
    sess.get(PAGE_URL).raise_for_status()
    sess.get(API_GET_TOKEN).raise_for_status()

def send_block(sess, block):
    payload = {"user_id": TOKEN, "len": len(block), "proxies": ", ".join(block)}
    r = sess.post(API_POST_PROXIES, json=payload)
    r.raise_for_status()
    return r.json()["save_id"]

def worker_local(blocks, result_dict):    
    for i, block in enumerate(blocks, start=1):
        while True:
            sess = make_session()
            authenticate(sess)
            try:
                save_id = send_block(sess, block)
                result_dict[save_id] = block
                print(f"[LOCAL-{i}] OK → {save_id}")
                break  
            except requests.HTTPError as e:
                status = e.response.status_code
                if status == 429:
                    retry_after = e.response.headers.get("retry-after")
                    try:
                        wait_time = int(retry_after)
                    except ValueError:
                        wait_time = 30  
                    print(f"[LOCAL-{i}] 429 Too Many Requests → ждём {wait_time}s…")
                    time.sleep(wait_time)
                    continue  
                else:
                    print(f"[LOCAL-{i}] FAIL {type(e).__name__} {status}")
                    break
            except Exception as e:
                print(f"[LOCAL-{i}] FAIL {type(e).__name__}")
                break

def block_worker(proxy, block, return_dict):
    try:
        sess = make_session(proxy)
        authenticate(sess)
        save_id = send_block(sess, block)
        return_dict['save_id'] = save_id
        return_dict['proxies'] = block
    except Exception as e:
        return_dict['error'] = str(e)
    finally:
        sess.close()

def worker_proxy(blocks, proxy_pool, result_dict, start_idx, thread_num):
    proxy_idx = start_idx

    for i, block in enumerate(blocks, start=1):
        while True:
            if proxy_idx >= len(proxy_pool):
                proxy_idx = 0
            proxy = proxy_pool[proxy_idx]
            desc = f"{proxy['ip']}:{proxy['port']} {proxy['protocols']}"
            print(f"[{thread_num}-THREAD  PROXY-{i}] Попытка через {desc}…")

            manager = multiprocessing.Manager()
            return_dict = manager.dict()

            p = multiprocessing.Process(target=block_worker, args=(proxy, block, return_dict))
            p.start()
            p.join(20) 

            if p.is_alive():
                print(f"[{thread_num}-THREAD  PROXY-{i}] Таймаут 20c")
                p.terminate()
                p.join()
                proxy_idx += 1
                continue 

            if 'save_id' in return_dict:
                save_id = return_dict['save_id']
                result_dict[save_id] = block
                print(f"[{thread_num}-THREAD  PROXY-{i}] OK → {save_id}")
                break
            else:
                print(f"[{thread_num}-THREAD  PROXY-{i}] FAIL")
                proxy_idx += 1

if __name__ == '__main__':
    start_time = time.perf_counter()
    
    proxy_global = []
    proxy_http = []
    process = CrawlerProcess({
        "LOG_LEVEL": "INFO"
    })
    
    process.crawl(ProxySpider)
    process.start()
    
    all_blocks = [
        [f"{p['ip']}:{p['port']}" for p in proxy_global[i*10:(i+1)*10]]
        for i in range(15)
    ]

    result_global = {}

    
    
    t_list = []
    
    t_list.append(threading.Thread(target=worker_local, args=(all_blocks[:9], result_global)))
    proxy_blocks = all_blocks[9:]
    chunk_size = (len(proxy_blocks) + PROXY_THREAD_NUMBER - 1) // PROXY_THREAD_NUMBER
    for i in range(PROXY_THREAD_NUMBER):
        start = i * chunk_size
        end = start + chunk_size
        sub_blocks = proxy_blocks[start:end]
        http_start = (len(proxy_http) // PROXY_THREAD_NUMBER) * i
        if not sub_blocks:
            continue
        t_list.append(threading.Thread(
            target=worker_proxy,
            args=(sub_blocks, proxy_http, result_global, http_start, i+1)
        ))
    for t in t_list:
        t.start()
    for t in t_list:
        t.join()


    with open("results.json", "w", encoding="utf-8") as f:
        json.dump(result_global, f, indent=2, ensure_ascii=False)

    print("✅ Все запросы выполнены. Результат записан в results.json")
    end_time = time.perf_counter()
    elapsed = int(end_time - start_time)

    formatted_time = str(timedelta(seconds=elapsed))
    if elapsed < 36000:
        formatted_time = f"{formatted_time:0>8}"
    with open("time.txt", "w", encoding="utf-8") as tf:
        tf.write(f"{formatted_time}")
