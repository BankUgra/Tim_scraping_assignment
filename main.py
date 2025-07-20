import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy import signals
import base64
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import re
from datetime import timedelta
import threading


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


def get_proxy_string(proxy):
        return f"{proxy['ip']}:{proxy['port']}"

def local_requests(sixty_list):
    URL = "https://test-rg8.ddns.net/task"
    TOKEN = "t_a3895d42"
    global results_local

    j = 0
    while sixty_list:
        if j != 0 and j % 3 == 0:
            time.sleep(35)

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        driver = webdriver.Chrome(options=options)
        driver.get(URL)
        time.sleep(2)

        token_input = driver.find_element(By.NAME, "token")
        token_input.send_keys(TOKEN)

        current = []
        if sixty_list:
            list_to_fill = sixty_list.pop()
        
        else:
            break
        for i, proxy in enumerate(list_to_fill):
            try:
                inp = driver.find_element(By.NAME, f"proxies.{i}.value")
                proxy_str = get_proxy_string(proxy)
                inp.send_keys(proxy_str)
                current.append(proxy_str)
            except Exception as e:
                print(f"Не удалось заполнить proxy {i}: {e}")
                sixty_list.append(list_to_fill)

        try:
            btn = driver.find_element(
                By.XPATH, "//button[@type='submit' and contains(text(), 'Submit')]"
            )
            btn.click()
            print("Форма отправлена.")
        except Exception as e:
            print(f"Не удалось нажать кнопку Submit: {e}")
            sixty_list.append(list_to_fill)

        time.sleep(1)

        try:
            alert = driver.find_element(By.CSS_SELECTOR, "div.MuiAlert-message")
            text = alert.text
            m = re.search(r"save_id[:\s]*([0-9a-fA-F\-]+)", text)
            if m:
                sid = m.group(1)
                results_local[sid] = current
                print(f"Найден save_id local: {sid}")
            else:
                print("Не удалось распарсить save_id")
                sixty_list.append(list_to_fill)
        except Exception as e:
            print(f"Не нашли MUI‑алерт: {e}")
            sixty_list.append(list_to_fill)

        driver.quit()
        j += 1


def proxy_requests(sixty_list, proxy_connection, slice, num):
    URL = "https://test-rg8.ddns.net/task"
    TOKEN = "t_a3895d42"
    global results_proxy
    check_ind = slice*num
    while sixty_list:
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        print(f'{num}Подключение по {proxy_connection[check_ind]}')
        options.add_argument(f'--proxy-server=http://{get_proxy_string(proxy_connection[check_ind])}')

        driver = webdriver.Chrome(options=options)
        try:
            driver.set_page_load_timeout(30)
            driver.get(URL)
            time.sleep(10)

            token_input = driver.find_element(By.NAME, "token")
            token_input.send_keys(TOKEN)

            current = []
            if sixty_list:
                list_to_fill = sixty_list.pop()
            else:
                break
            for i, proxy in enumerate(list_to_fill):
                try:
                    inp = driver.find_element(By.NAME, f"proxies.{i}.value")
                    proxy_str = get_proxy_string(proxy)
                    inp.send_keys(proxy_str)
                    current.append(proxy_str)
                except Exception as e:
                    print(f"Не удалось заполнить proxy {i}: {e}")
                    sixty_list.append(list_to_fill)

            try:
                btn = driver.find_element(
                    By.XPATH, "//button[@type='submit' and contains(text(), 'Submit')]"
                )
                btn.click()
                print(f"{num}Форма отправлена.")
            except Exception as e:
                print(f"Не удалось нажать кнопку Submit: {e}")
                sixty_list.append(list_to_fill)

            time.sleep(5)

            try:
                alert = driver.find_element(By.CSS_SELECTOR, "div.MuiAlert-message")
                text = alert.text
                print(f'ПАРАПАМ {text}')
                m = re.search(r"save_id[:\s]*([0-9a-fA-F\-]+)", text)
                if m:
                    sid = m.group(1)
                    results_proxy[sid] = current
                    print(f"Найден save_id proxy: {sid}")
                else:
                    print("Не удалось распарсить save_id")
                    sixty_list.append(list_to_fill)
            except Exception as e:
                print(f"Не нашли MUI‑алерт: {e}")
                sixty_list.append(list_to_fill)
            driver.quit()

        except Exception as e:
            print(f'АШЫПКА {e}')
            driver.quit()
            check_ind += 1
            if check_ind >= len(proxy_connection):
                check_ind = 0
            continue

if __name__ == '__main__':
    start = time.perf_counter()
    process = CrawlerProcess({
        "LOG_LEVEL": "INFO"
    })
    
    proxy_global = []
    proxy_http = []
    results_local = {}
    results_proxy = {}
    
    process.crawl(ProxySpider)
    process.start()
    
    slice_list = []
    for i in range(15):
        slice_list.append([])
        for proxy in proxy_global[(i)*10 : (i+1)*10]:
            slice_list[i].append(proxy)
    
    
    local_thread = threading.Thread(target=local_requests, args=(slice_list, ))
    
    
    number_of_proxy_threads = 6
    slice = len(proxy_http) // number_of_proxy_threads
    proxy_thread_list = []
    for i in range(number_of_proxy_threads):
        proxy_thread_list.append(threading.Thread(target=proxy_requests, args=(slice_list, proxy_http, slice, i)))
    
    local_thread.start()
    for proxy_thread in proxy_thread_list:
        proxy_thread.start()
        
    local_thread.join()
    for proxy_thread in proxy_thread_list:
        proxy_thread.join()
    
    results_local.update(results_proxy)
    with open("results.json", "w", encoding="utf-8") as f:
            json.dump(results_local, f, ensure_ascii=False, indent=2)
    end = time.perf_counter()
    elapsed = int(end - start)

    formatted_time = str(timedelta(seconds=elapsed))
    if elapsed < 36000:
        formatted_time = f"{formatted_time:0>8}"
    with open("time.txt", "w", encoding="utf-8") as tf:
        tf.write(f"{formatted_time}")
