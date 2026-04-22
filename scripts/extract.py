import requests
import pandas as pd 
import pandas as pd 
from datetime import datetime
import logging 
import time 


## Configuration du logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Headers HTTP ( simuler un navigateur )
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" ,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    
} 
# URL BVC
BASE_URL = "https://www.casablanca-bourse.com"
URLS = {
    "actions":  f"{BASE_URL}/fr/live-market/marche-actions-groupement",
    "indices":  f"{BASE_URL}/fr/live-market/overview",
    "transactions": f"{BASE_URL}/fr/live-market/transactions-actions",
}


# def fetch_page()
def fetch_page(url: str, retries: int = 3, delay: int = 2) -> BeautifulSoup | None:
    """
    Télécharge une page web et retourne un objet BeautifulSoup.
    Réessaie automatiquement en cas d'erreur (max `retries` fois).
    """
    for attempt in range(1, retries + 1):
        try:
            logger.info(f" Requête [{attempt}/{retries}] → {url}")
            response = requests.get(url, headers=HEADERS, timeout=15)
            response.raise_for_status()  # Lève une exception si status != 200
            logger.info(f" Page reçue — status {response.status_code}")
            return BeautifulSoup(response.text, "lxml")
 
        except requests.exceptions.HTTPError as e:
            logger.warning(f"  Erreur HTTP : {e}")
        except requests.exceptions.ConnectionError:
            logger.warning(f"  Connexion impossible à {url}")
        except requests.exceptions.Timeout:
            logger.warning(f"  Timeout sur {url}")
 
        if attempt < retries:
            logger.info(f" Pause {delay}s avant nouvelle tentative...")
            time.sleep(delay)
 
    logger.error(f" Échec après {retries} tentatives pour {url}")
    return None
 
 
# ============================================================
#  FONCTION UTILITAIRE : nettoyer un nombre textuel
# ============================================================
def parse_number(text: str) -> float | None:
    """
    Convertit une chaîne comme '1 234,56' ou '1,234.56' en float.
    Retourne None si la conversion est impossible.
    """
    if not text or text.strip() in ("-", "N/A", ""):
        return None
    try:
        # Supprimer espaces, remplacer virgule décimale par point
        cleaned = text.strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
        # Gérer le cas '1.234.567' (séparateur de milliers = point)
        if cleaned.count(".") > 1:
            cleaned = cleaned.replace(".", "", cleaned.count(".") - 1)
        return float(cleaned)
    except ValueError:
        return None
 
 
# ============================================================
#  EXTRACT 1 : Cours des actions BVC
# ============================================================
def extract_stock_prices() -> pd.DataFrame:
    """
    Scrape le tableau des cours des actions sur casablanca-bourse.com.
    Retourne un DataFrame avec colonnes :
      ticker, name, close, variation_pct, open, high, low, volume, date
    """
    logger.info("Extraction des cours des actions BVC...")
 
    soup = fetch_page(URLS["actions"])
    if soup is None:
        logger.error(" Impossible de récupérer les cours des actions.")
        return pd.DataFrame()
 
    records = []
    today = datetime.now().strftime("%Y-%m-%d")
 
    # Chercher tous les tableaux de données
    tables = soup.find_all("table")
    logger.info(f"   → {len(tables)} tableau(x) trouvé(s) sur la page")
 
    # Chercher le tableau principal des cours
    target_table = None
    for table in tables:
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        # Le tableau des cours contient typiquement "Valeur", "Cours", "Variation"
        if any(kw in " ".join(headers) for kw in ["Cours", "Valeur", "Variation", "Volume"]):
            target_table = table
            logger.info(f"   → Tableau cible trouvé : {headers[:5]}")
            break
 
    if target_table is None:
        logger.warning(" Tableau des cours non trouvé — la structure HTML a peut-être changé.")
        # Fallback : chercher des divs ou sections avec données
        _debug_save(soup, "debug_actions.html")
        return pd.DataFrame()
 
    # Parser chaque ligne du tableau
    rows = target_table.find_all("tr")[1:]  # Ignorer l'en-tête
    logger.info(f"   → {len(rows)} ligne(s) d'actions trouvée(s)")
 
    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) < 4:
            continue
 
        text = [c.get_text(strip=True) for c in cells]
 
        try:
            record = {
                "ticker":        text[0] if len(text) > 0 else None,
                "name":          text[1] if len(text) > 1 else None,
                "close":         parse_number(text[2]) if len(text) > 2 else None,
                "variation_pct": parse_number(text[3]) if len(text) > 3 else None,
                "open":          parse_number(text[4]) if len(text) > 4 else None,
                "high":          parse_number(text[5]) if len(text) > 5 else None,
                "low":           parse_number(text[6]) if len(text) > 6 else None,
                "volume":        parse_number(text[7]) if len(text) > 7 else None,
                "date":          today,
                "scraped_at":    datetime.now().isoformat(),
            }
            # Ignorer les lignes sans ticker ni cours
            if record["ticker"] and record["close"]:
                records.append(record)
        except Exception as e:
            logger.warning(f" Erreur parsing ligne : {e}")
            continue
 
    df = pd.DataFrame(records)
    logger.info(f" {len(df)} actions extraites.")
    return df
 
 
# ============================================================
#  EXTRACT 2 : Indices MASI / MADEX
# ============================================================
def extract_indices() -> pd.DataFrame:
    """
    Scrape les valeurs des indices MASI et MADEX.
    Retourne un DataFrame avec colonnes :
      index_name, value, variation_pct, variation_pts, date
    """
    logger.info("📈 Extraction des indices MASI / MADEX...")
 
    soup = fetch_page(URLS["indices"])
    if soup is None:
        logger.error("❌ Impossible de récupérer les indices.")
        return pd.DataFrame()
 
    records = []
    today = datetime.now().strftime("%Y-%m-%d")
 
    # Chercher les blocs d'indices (cards, divs ou tableau)
    # Le site BVC affiche MASI et MADEX en haut de page dans des widgets
    index_names = ["MASI", "MADEX", "MSI20", "FTSE CSE Morocco 15"]
 
    for name in index_names:
        # Chercher le texte de l'indice dans la page
        element = soup.find(string=lambda t: t and name in t)
        if element:
            parent = element.find_parent()
            # Remonter pour trouver les valeurs associées
            container = parent.find_parent() if parent else None
            if container:
                numbers = container.find_all(
                    string=lambda t: t and any(c.isdigit() for c in str(t))
                )
                values = [parse_number(n) for n in numbers if parse_number(n)]
 
                if values:
                    records.append({
                        "index_name":    name,
                        "value":         values[0] if len(values) > 0 else None,
                        "variation_pct": values[1] if len(values) > 1 else None,
                        "date":          today,
                        "scraped_at":    datetime.now().isoformat(),
                    })
                    logger.info(f"   → {name} : {values[0]}")
 
    # Si aucun indice trouvé via texte, essayer les tableaux
    if not records:
        logger.warning("⚠️  Indices non trouvés via texte — tentative via tableaux...")
        tables = soup.find_all("table")
        for table in tables:
            headers = [th.get_text(strip=True) for th in table.find_all("th")]
            if any(kw in " ".join(headers) for kw in ["Indice", "Valeur", "Variation"]):
                for row in table.find_all("tr")[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all("td")]
                    if len(cells) >= 2:
                        records.append({
                            "index_name":    cells[0],
                            "value":         parse_number(cells[1]),
                            "variation_pct": parse_number(cells[2]) if len(cells) > 2 else None,
                            "date":          today,
                            "scraped_at":    datetime.now().isoformat(),
                        })
 
    df = pd.DataFrame(records)
    logger.info(f" {len(df)} indice(s) extrait(s).")
    return df
 
 
# ============================================================
#  FONCTION DEBUG : sauvegarder le HTML pour inspection
# ============================================================
def _debug_save(soup: BeautifulSoup, filename: str):
    """Sauvegarde le HTML dans /tmp pour inspection manuelle."""
    try:
        with open(f"/tmp/{filename}", "w", encoding="utf-8") as f:
            f.write(str(soup))
        logger.info(f" HTML sauvegardé dans /tmp/{filename} pour débogage.")
    except Exception:
        pass
 
 
# ============================================================
#  FONCTION PRINCIPALE : extraire toutes les données
# ============================================================
def extract_all() -> dict:
    """
    Point d'entrée principal du module Extract.
    Appelé par le DAG Airflow à chaque exécution.
 
    Retourne un dictionnaire :
    {
        "stocks":  DataFrame des cours actions,
        "indices": DataFrame des indices,
    }
    """
    logger.info("=" * 50)
    logger.info(" DÉMARRAGE EXTRACTION BVC")
    logger.info(f"   Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)
 
    result = {
        "stocks":  extract_stock_prices(),
        "indices": extract_indices(),
    }
 
    # Résumé
    logger.info("─" * 50)
    logger.info(f" Résultat extraction :")
    logger.info(f"    Actions  : {len(result['stocks'])} lignes")
    logger.info(f"    Indices  : {len(result['indices'])} lignes")
    logger.info("─" * 50)
 
    return result

if __name__ == "__main__":
    data = extract_all()
 
    print("\n Aperçu — Actions BVC :")
    print(data["stocks"].head(10).to_string(index=False))
 
    print("\n Aperçu — Indices :")
    print(data["indices"].to_string(index=False))