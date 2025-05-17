import requests
from bs4 import BeautifulSoup
import csv
import concurrent.futures
import threading

input_file = r"C:\Users\tursy\market-flyer\aldi_json\aldi_store_links.txt"
output_file = r"C:\Users\tursy\market-flyer\aldi_json\aldi_lang.csv"
lock = threading.Lock()  # CSV dosyasına erişimi kontrol etmek için

def get_weekly_ad(store_url):
    store_url = store_url.strip()
    if not store_url:
        return

    try:
        response = requests.get(store_url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')

        # View Weekly Ad linkini bul
        weekly_ad_tag = soup.find('a', string=lambda text: text and "View Weekly Ad" in text)

        if weekly_ad_tag and weekly_ad_tag.has_attr('href'):
            weekly_ad_url = weekly_ad_tag['href']
            if weekly_ad_url.startswith('/'):
                weekly_ad_url = 'https://www.aldi.us' + weekly_ad_url
        else:
            weekly_ad_url = "NOT FOUND"

        with lock:
            with open(output_file, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([store_url, weekly_ad_url])
                print(f"✅ {store_url} → {weekly_ad_url}")

    except Exception as e:
        with lock:
            with open(output_file, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([store_url, f"ERROR: {str(e)}"])
                print(f"❌ ERROR for {store_url}: {e}")

# Başlıkları CSV'ye yaz
with open(output_file, mode='w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['store_url', 'weekly_ad_url'])

# URL’leri oku
with open(input_file, 'r', encoding='utf-8') as f:
    urls = f.readlines()

# Aynı anda çalışan maksimum iş parçacığı sayısı
MAX_WORKERS = 5

# Thread havuzu ile paralel çalıştır
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    executor.map(get_weekly_ad, urls)
