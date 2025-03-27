import requests
from bs4 import BeautifulSoup
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import time
import random

processed_pages = 0


def get_soup(url, retries=3, delay=2):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
    }

    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except requests.exceptions.ConnectTimeout:
            print(f"[Timeout] Attempt {attempt + 1}/{retries} dla {url}")
        except requests.exceptions.RequestException as e:
            print(f"[Error] {e} (Attempt {attempt + 1}/{retries})")

        time.sleep(delay + random.uniform(0, 2))

    print(f"Cannot connect to {url} after {retries} attempts.")
    return None


def page_count(url):
    page_counter = 1
    found_archived = False

    def check_page(page_number):
        sub_url = f"{url}?p={page_number}"
        soup = get_soup(sub_url)
        return page_number if number_of_archives(soup) < 41 else None

    with ThreadPoolExecutor(max_workers=8) as executor:
        while not found_archived:
            pages_to_check = range(page_counter, page_counter + 8)
            results = list(executor.map(check_page, pages_to_check))

            valid_pages = [page for page in results if page is not None]
            if not valid_pages:
                found_archived = True
            else:
                page_counter = max(valid_pages) + 1
                print(f"\r{page_counter} pages valid.", end='', flush=True)

    last_valid_page = page_counter if page_counter > 1 else 1
    return last_valid_page


def number_of_archives(soup):
    archives = 0
    for search in soup.find_all('div', class_='tertiary'):
        archive = search.find('p', class_='abap')
        if archive is None:
            continue
        else:
            if archive.text.strip() == "Archiwalne":
                archives += 1
    return archives


def process_listings(listing):
    province_element = listing.find('a', class_='margin-right4')
    if province_element is None:
        province_element = listing.find('span', class_='margin-right4')
    province = province_element.text.strip() if province_element else None

    h2 = listing.find('h2', class_='name')
    if not h2:
        return None

    a = h2.find('a')
    if not a:
        return None

    title = a.text.strip()
    link = a.get('href')

    subsoup = get_soup(link)
    if subsoup is None:
        return None

    for sublisting in subsoup.find_all('div', class_='box-offer'):
        total_price_element = sublisting.find('span', class_='info-primary-price')
        total_price = total_price_element.text.strip() if total_price_element else None

        price_per_m2_element = sublisting.find('span', class_='info-secondary-price desktop-tablet-only')
        price_per_m2 = price_per_m2_element.text.strip() if price_per_m2_element else None

        area_element = sublisting.find('span', class_='info-area desktop-tablet-only')
        area = area_element.text.strip() if area_element else None

        room_no_element = sublisting.find(string=lambda text: text and "Liczba pokoi:" in text)
        room_no = room_no_element.find_next('span').text.strip() if room_no_element else None

        constr_date_element = sublisting.find(string=lambda text: text and 'Rok budowy:' in text)
        constr_date = constr_date_element.find_next('span').text.strip() if constr_date_element else None

        condition_element = sublisting.find(string=lambda text: text and "Stan mieszkania:" in text)
        condition = condition_element.find_next('span').text.strip() if condition_element else None

        market_element = sublisting.find(string=lambda text: text and "Rynek:" in text)
        market = market_element.find_next('span').text.strip() if market_element else None

        listing_type_element = sublisting.find(string=lambda text: text and "Typ oferty:" in text)
        listing_type = listing_type_element.find_next('span').text.strip() if listing_type_element else None

    return (title, link, province, total_price, price_per_m2, area, room_no, constr_date, condition, market, listing_type)


def scrape_page(url):
    global processed_pages
    soup = get_soup(url)
    if soup is None:
        print(f"Skipped: {url} (connection error)")
        return []

    listings_data = []
    listings = soup.find_all('div', class_='tertiary')

    with ThreadPoolExecutor(max_workers=10 ) as executor:
        results = executor.map(process_listings, listings)

    for result in results:
        if result:
            listings_data.append(result)

    processed_pages += 1
    print(f"\rPages processed: {processed_pages}", end='', flush=True)

    return listings_data


def main():
    main_url = 'https://wroclaw.nieruchomosci-online.pl/'

    now = datetime.now().strftime('%d-%m-%Y_%H-%M-%S')
    database_name = fr"listings_{now}.db"

    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    cursor.execute("\n"
                   "    CREATE TABLE IF NOT EXISTS listings (\n"
                   "        id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                   "        title TEXT,\n"
                   "        link TEXT,\n"
                   "        province TEXT,\n"
                   "        total_price TEXT,\n"
                   "        price_per_m2 TEXT, \n"
                   "        area TEXT,\n"
                   "        room_no TEXT,\n"
                   "        construction_date TEXT,\n"
                   "        condition TEXT,\n"
                   "        market TEXT,\n"
                   "        listing_type TEXT\n"
                   "    )\n"
                   "    ")

    start_time = time.time()

    print("Initializing page counter...")
    total_pages = page_count(main_url)

    data = []

    print("\nInitializing page scraper...")
    with ThreadPoolExecutor(max_workers=8) as executor:
        urls = [f'{main_url}?p={i}' for i in range(1, total_pages)]
        results = executor.map(scrape_page, urls)

    for page_data in results:
        data.extend(page_data)

    # SAVING DATA TO DATABASE
    if data:
        cursor.executemany(
            "INSERT INTO listings (title, link, province, total_price, price_per_m2, area, room_no, construction_date, condition, market, listing_type) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", data)

    end_time = time.time()
    print(f'\nEnd. Elapsed time: {end_time - start_time:.2f} seconds.')

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
