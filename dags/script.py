import requests
from bs4 import BeautifulSoup
import csv
import os
from textblob import TextBlob
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import csv
import os

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'
}

# Fungsi scraping dari Detik
def scrape_detik(hal, keyword, search_url):
    a = 1  # Counter untuk artikel
    # Membuat dataframe kosong untuk menyimpan hasil
    data = []

    for page in range(1, hal + 1):
        url = search_url.format(page=page)
        print(f"Scraping page {page} from Detik for keyword '{keyword}': {url}")

        try:
            ge = requests.get(url, headers=headers)
            ge.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error saat mengakses halaman {page}: {e}")
            continue

        soup = BeautifulSoup(ge.text, 'lxml')
        articles = soup.find_all('article', class_='list-content__item')
        if not articles:
            print(f"No articles found on page {page}")
            continue

        for article in articles:
            # Ambil headline dan link
            headline_detik_elem = article.find('h3', {'class': 'media__title'})
            if headline_detik_elem:
                link_detik_elem = headline_detik_elem.find('a')
                headline_detik = link_detik_elem.text.strip() if link_detik_elem else "Headline not found"
                link_detik = link_detik_elem['href'] if link_detik_elem else "Link not found"
            else:
                headline_detik = "Headline not found"
                link_detik = "Link not found"

            # Ambil tanggal
            date_detik_elem = article.find('div', {'class': 'media__date'})
            date_detik = date_detik_elem.text.strip() if date_detik_elem else "Date not found"

            # Scrape konten
            content_detik = "Content not found"
            if link_detik != "Link not found":
                try:
                    article_response = requests.get(link_detik, headers=headers)
                    article_response.raise_for_status()
                    article_soup = BeautifulSoup(article_response.text, 'lxml')

                    # Coba beberapa elemen untuk konten
                    content_detik_elem = article_soup.find('div', class_='detail__body-text itp_bodycontent')
                    if not content_detik_elem:
                        content_detik_elem = article_soup.find('div', class_='detail__body-text')
                    if not content_detik_elem:
                        content_detik_elem = article_soup.find('div', class_='detailFoto__title m-0 text-center')

                    if content_detik_elem:
                        paragraphs = content_detik_elem.find_all('p') or [content_detik_elem]
                        content_detik = ' '.join(p.text.strip() for p in paragraphs).replace('\n', '').replace('ADVERTISEMENT', '').replace('SCROLL TO RESUME CONTENT', '')
                except requests.exceptions.RequestException as e:
                    print(f"Error saat mengakses artikel: {e}")
                    content_detik = "Content not found"

            # Analisis sentimen
            sentiment = analyze_sentiment(content_detik)

            print(f"Detik: Article {a} for keyword '{keyword}' scraped.")
            a += 1

            # Simpan hasil ke dalam dataframe
            data.append(['Detik', keyword, headline_detik, date_detik, link_detik, content_detik, sentiment])

    # Mengembalikan DataFrame
    return pd.DataFrame(data, columns=['source', 'keyword', 'headline', 'date', 'link', 'content', 'sentiment'])


# Fungsi scraping dari Pikiran Rakyat
def scrape_pikiran_rakyat(hal, keyword, search_url):
    a = 1  # Counter untuk artikel
    # Membuat dataframe kosong untuk menyimpan hasil
    data = []

    for page in range(1, hal + 1):
        url = search_url.format(page=page)
        print(f"Scraping page {page} from Pikiran Rakyat for keyword '{keyword}': {url}")

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page}: {e}")
            continue

        soup = BeautifulSoup(response.text, 'lxml')
        articles = soup.find_all('div', class_='latest__item')
        if not articles:
            print(f"No articles found on page {page}")
            continue

        for article in articles:
            # Ambil headline dan link
            headline_pikiranrakyat_elem = article.find('a', class_='latest__link')
            headline_pikiranrakyat = headline_pikiranrakyat_elem.text.strip() if headline_pikiranrakyat_elem else "Headline not found"
            link_pikiranrakyat = headline_pikiranrakyat_elem['href'] if headline_pikiranrakyat_elem else "Link not found"

            # Ambil tanggal
            date_pikiranrakyat_elem = article.find('div', class_='latest__date')
            date_pikiranrakyat = date_pikiranrakyat_elem.text.strip() if date_pikiranrakyat_elem else "Date not found"

            # Scrape konten
            content_pikiranrakyat = "Content not found"
            if link_pikiranrakyat != "Link not found":
                try:
                    article_response = requests.get(link_pikiranrakyat, headers=headers)
                    article_response.raise_for_status()
                    article_soup = BeautifulSoup(article_response.text, 'lxml')
                    content_pikiranrakyat_elem = article_soup.find('article', class_='read__content clearfix')

                    if content_pikiranrakyat_elem:
                        paragraphs = content_pikiranrakyat_elem.find_all('p')
                        content_pikiranrakyat = ' '.join(p.text.strip() for p in paragraphs).replace('\n', '').replace('ADVERTISEMENT', '').replace('SCROLL TO RESUME CONTENT', '')
                except requests.exceptions.RequestException as e:
                    print(f"Error fetching article content: {e}")
                    content_pikiranrakyat = "Content not found"

            # Analisis sentimen
            sentiment = analyze_sentiment(content_pikiranrakyat)

            print(f"Pikiran Rakyat: Article {a} for keyword '{keyword}' scraped.")
            a += 1

            # Simpan hasil ke dalam dataframe
            data.append(['Pikiran Rakyat', keyword, headline_pikiranrakyat, date_pikiranrakyat, link_pikiranrakyat, content_pikiranrakyat, sentiment])

    # Mengembalikan DataFrame
    return pd.DataFrame(data, columns=['source', 'keyword', 'headline', 'date', 'link', 'content', 'sentiment'])

def analyze_sentiment(content):
    if content == "Content not found":
        return "Neutral"
    analysis = TextBlob(content)
    if analysis.sentiment.polarity > 0:
        return "Positive"
    elif analysis.sentiment.polarity < 0:
        return "Negative"
    else:
        return "Neutral"

# Gunakan fungsi scraping untuk mendapatkan DataFrame
filename = 'combined_semua.csv'

# Detik
detik_df = scrape_detik(1, 'Andika Perkasa', 'https://www.detik.com/search/searchall?query=andika%20perkasa&page={page}&result_type=latest')
detik_df_2 = scrape_detik(1, 'Ahmad Luthfi', 'https://www.detik.com/search/searchall?query=ahmad%20luthfi&page={page}&result_type=latest')

# Pikiran Rakyat
pikiranrakyat_df = scrape_pikiran_rakyat(1,'Andika Perkasa', 'https://www.pikiran-rakyat.com/search/?q=andika+perkasa&page={page}#gsc.tab=0&gsc.q=andika%20perkasa&gsc.page={page}')
pikiranrakyat_df_2 = scrape_pikiran_rakyat(1, 'Ahmad Luthfi', 'https://www.pikiran-rakyat.com/search/?q=ahmad+luthfi&page={page}#gsc.tab=0&gsc.q=ahmad%20luthfi&gsc.page={page}')

# Gabungkan DataFrame
combined_df = pd.concat([detik_df, detik_df_2, pikiranrakyat_df, pikiranrakyat_df_2], ignore_index=True)

# Lihat hasilnya
print(combined_df)