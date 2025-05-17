import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import datetime
import numpy as np

from src.supabase_utils import save_to_supabase

# Nettoyage du prix
MAX_PRIX = 1_000_000_000  # 1 milliard
MAX_PRIX_M2 = 500_000     # 500 000 DH/m²


def clean(text):
    return re.sub(r"\s+", " ", text).strip()


def extract_price(text):
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else ""


def extract_number(text):
    match = re.search(r"\d+", text)
    return int(match.group()) if match else ""


def extract_features(listing):
    superficie = pieces = chambres = sdb = None
    clim = ascenseur = garage = False

    features = listing.find_all("div", class_="adDetailFeature")
    for f in features:
        icon = f.find("i")
        value = f.find("span").text if f.find("span") else None
        if not icon:
            continue
        class_icon = icon["class"]
        if "icon-triangle" in class_icon:
            superficie = extract_number(value)
        elif "icon-house-boxes" in class_icon:
            pieces = extract_number(value)
        elif "icon-bed" in class_icon:
            chambres = extract_number(value)
        elif "icon-bath" in class_icon:
            sdb = extract_number(value)

    extras = listing.find_all("div", class_="adFeature")
    for e in extras:
        icon = e.find("i")
        if not icon:
            continue
        cls = icon["class"]
        if "icon-airConditioning" in cls:
            clim = True
        elif "icon-elevator" in cls:
            ascenseur = True
        elif "icon-garage" in cls:
            garage = True

    return superficie, pieces, chambres, sdb, clim, ascenseur, garage


def clean_df_for_supabase(df):
    numeric_cols = ["prix", "superficie", "pieces", "chambres", "sdb", "prix_m2"]

    # Remplace "NaN" string et np.nan par None
    df = df.replace(["NaN", "nan", ""], None)
    df = df.replace({np.nan: None})

    # Convertir en float/int les colonnes numériques
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")  # NaN si échec
            df[col] = df[col].astype("Int64")  # Type int nullable

    return df


def scrape_mubawab():
    base_url = "https://www.mubawab.ma/fr/sc/appartements-a-vendre:o:n:p:{}"
    all_data = []
    nb_page = 473  # Tu peux augmenter le nombre de pages ici

    # Configuration de la session avec retries
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Connection": "keep-alive",
    }

    for page in range(200, nb_page):
        url = base_url.format(page)
        print(f"Scraping page {page}: {url}")
        try:
            response = session.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"❌ Erreur sur la page {page}: {e}")
            continue

        if response.status_code != 200:
            print(f"Erreur HTTP: {response.status_code}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        listings = soup.find_all("div", class_="listingBox")

        for listing in listings:
            title_tag = listing.find("h2")
            title = clean(title_tag.get_text(strip=True)) if title_tag else ""

            link_tag = listing.find("a", href=True)
            link = clean(link_tag["href"]) if link_tag else ""

            price_tag = listing.find("span", class_="priceTag hardShadow float-left")
            price_str = clean(price_tag.get_text(strip=True)) if price_tag else ""
            price = extract_price(price_str) if price_str else None
            if price and price > MAX_PRIX:
                price = None

            location = None
            neighborhood, city = None, None
            loc_div = listing.find("span", class_="listingH3")
            if loc_div:
                loc_text = loc_div.get_text(strip=True)
                location = clean(loc_text) if loc_text else ""
                if location:
                    splitted_text = location.split(",")
                    if len(splitted_text) == 2:
                        neighborhood, city = splitted_text
                        neighborhood, city = clean(neighborhood), clean(city)
                    else:
                        city = clean(splitted_text[0])

            superficie, pieces, chambres, sdb, clim, ascenseur, garage = extract_features(listing)

            prix_m2 = None
            if price and superficie:
                try:
                    prix_m2 = int(price / superficie)
                    if prix_m2 > MAX_PRIX_M2:
                        prix_m2 = None
                except:
                    prix_m2 = None
            if link:
                all_data.append({
                    "titre": title,
                    "prix": price,
                    "lien": link,
                    "localisation": location,
                    "quartier": neighborhood,
                    "ville": city,
                    "superficie": superficie,
                    "pieces": pieces,
                    "chambres": chambres,
                    "sdb": sdb,
                    "clim": clim,
                    "ascenseur": ascenseur,
                    "garage": garage,
                    "prix_m2": prix_m2,
                    "date_scraping": str(pd.Timestamp.today().date())
                })

        time.sleep(2)

    df = pd.DataFrame(all_data)
    df = clean_df_for_supabase(df)

    return df


if __name__ == "__main__":
    df = scrape_mubawab()
    print(df)

    save_to_supabase(df)