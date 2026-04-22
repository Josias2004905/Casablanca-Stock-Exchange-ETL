"""
============================================================
  load.py — Chargement des données BVC dans PostgreSQL
  Reçoit les DataFrames propres de transform.py et les
  insère dans la base de données PostgreSQL.

  Fonctionnalités :
    1. Connexion PostgreSQL robuste avec retry
    2. Insertion avec gestion des doublons (UPSERT)
    3. Logs détaillés par opération
    4. Rollback automatique en cas d'erreur
============================================================
"""

import psycopg2
import psycopg2.extras
import pandas as pd
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

# ─────────────────────────────────────────
#  Logger
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
#  Charger les variables d'environnement
# ─────────────────────────────────────────
load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     os.getenv("DB_PORT",     "5432"),
    "dbname":   os.getenv("DB_NAME",     "bvc_db"),
    "user":     os.getenv("DB_USER",     "bvc_admin"),
    "password": os.getenv("DB_PASSWORD", "bvc_secret_2024"),
}


# ============================================================
#  CONNEXION PostgreSQL
# ============================================================
def get_connection():
    """
    Crée et retourne une connexion PostgreSQL.
    Lève une exception explicite si la connexion échoue.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info(f" Connecté à PostgreSQL — {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f" Connexion PostgreSQL impossible : {e}")
        raise


# ============================================================
#  LOAD 1 : Cours des actions → table stock_prices
# ============================================================
def load_stocks(df: pd.DataFrame, conn) -> int:
   
    if df.empty:
        logger.warning("  DataFrame actions vide — rien à charger.")
        return 0

    logger.info(f" Chargement de {len(df)} actions dans stock_prices...")

    # Requête UPSERT PostgreSQL
    # ON CONFLICT : si (ticker, date) existe → mise à jour
    sql = """
        INSERT INTO stock_prices
            (ticker, name, date, open, high, low, close, volume,
             variation_pct, variation_abs, amplitude_pct, tendance, scraped_at)
        VALUES
            (%(ticker)s, %(name)s, %(date)s, %(open)s, %(high)s, %(low)s,
             %(close)s, %(volume)s, %(variation_pct)s, %(variation_abs)s,
             %(amplitude_pct)s, %(tendance)s, %(scraped_at)s)
        ON CONFLICT (ticker, date)
        DO UPDATE SET
            close         = EXCLUDED.close,
            open          = EXCLUDED.open,
            high          = EXCLUDED.high,
            low           = EXCLUDED.low,
            volume        = EXCLUDED.volume,
            variation_pct = EXCLUDED.variation_pct,
            variation_abs = EXCLUDED.variation_abs,
            amplitude_pct = EXCLUDED.amplitude_pct,
            tendance      = EXCLUDED.tendance,
            scraped_at    = EXCLUDED.scraped_at;
    """

    # Préparer les lignes à insérer
    rows = _prepare_rows(df, [
        "ticker", "name", "date", "open", "high", "low", "close",
        "volume", "variation_pct", "variation_abs", "amplitude_pct",
        "tendance", "scraped_at"
    ])

    return _execute_batch(conn, sql, rows, table="stock_prices")


# ============================================================
#  LOAD 2 : Indices → table market_indices
# ============================================================
def load_indices(df: pd.DataFrame, conn) -> int:
    """
    Insère les indices MASI/MADEX dans la table market_indices.
    UPSERT sur (index_name, date).

    Retourne le nombre de lignes insérées/mises à jour.
    """
    if df.empty:
        logger.warning(" DataFrame indices vide — rien à charger.")
        return 0

    logger.info(f" Chargement de {len(df)} indices dans market_indices...")

    sql = """
        INSERT INTO market_indices
            (index_name, date, value, variation_pct, variation_abs, tendance, scraped_at)
        VALUES
            (%(index_name)s, %(date)s, %(value)s, %(variation_pct)s,
             %(variation_abs)s, %(tendance)s, %(scraped_at)s)
        ON CONFLICT (index_name, date)
        DO UPDATE SET
            value         = EXCLUDED.value,
            variation_pct = EXCLUDED.variation_pct,
            variation_abs = EXCLUDED.variation_abs,
            tendance      = EXCLUDED.tendance,
            scraped_at    = EXCLUDED.scraped_at;
    """

    rows = _prepare_rows(df, [
        "index_name", "date", "value", "variation_pct",
        "variation_abs", "tendance", "scraped_at"
    ])

    return _execute_batch(conn, sql, rows, table="market_indices")


# ============================================================
#  UTILITAIRE : préparer les lignes pour l'insertion
# ============================================================
def _prepare_rows(df: pd.DataFrame, colonnes: list) -> list[dict]:
    
    now = datetime.now().isoformat()

    # Ajouter scraped_at si absent du DataFrame
    if "scraped_at" not in df.columns:
        df = df.copy()
        df["scraped_at"] = now

    # Ne garder que les colonnes demandées qui existent
    cols_presentes = [c for c in colonnes if c in df.columns]
    rows = [
        {k: (None if (hasattr(v, "__class__") and str(v) == "nan") or v != v else v) for k, v in row.items()}
        for row in df[cols_presentes].to_dict("records")
    ]

    return rows


# ============================================================
#  UTILITAIRE : exécution batch avec transaction
# ============================================================
def _execute_batch(conn, sql: str, rows: list[dict], table: str) -> int:
    """
    Exécute une insertion batch dans une transaction.
    Rollback automatique en cas d'erreur.
    Retourne le nombre de lignes traitées.
    """
    if not rows:
        return 0

    cursor = conn.cursor()
    try:
        # execute_batch est plus rapide que execute() en boucle
        psycopg2.extras.execute_batch(cursor, sql, rows, page_size=100)
        conn.commit()
        nb = len(rows)
        logger.info(f"   ✅ {nb} ligne(s) insérées/mises à jour dans '{table}'")
        return nb

    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"   ❌ Erreur insertion dans '{table}' : {e}")
        logger.error(f"   → Rollback effectué.")
        raise

    finally:
        cursor.close()


# ============================================================
#  VÉRIFICATION : compter les lignes en base
# ============================================================
def verify_load(conn) -> dict:
    """
    Vérifie le contenu des tables après chargement.
    Retourne un dict avec les compteurs.
    """
    cursor = conn.cursor()
    stats = {}

    queries = {
        "stock_prices":   "SELECT COUNT(*), MAX(date) FROM stock_prices;",
        "market_indices": "SELECT COUNT(*), MAX(date) FROM market_indices;",
    }

    logger.info("🔍 Vérification post-chargement :")
    for table, query in queries.items():
        try:
            cursor.execute(query)
            count, last_date = cursor.fetchone()
            stats[table] = {"count": count, "last_date": str(last_date)}
            logger.info(f"   → {table:<20} : {count} lignes | Dernière date : {last_date}")
        except psycopg2.Error as e:
            logger.warning(f"   ⚠️  Impossible de vérifier '{table}' : {e}")

    cursor.close()
    return stats


# ============================================================
#  FONCTION PRINCIPALE
# ============================================================
def load_all(clean_data: dict) -> dict:
    """
    Point d'entrée principal du module Load.
    Appelé par le DAG Airflow après transform_all().

    Paramètre :
        clean_data : dict retourné par transform_all()
                     {"stocks": DataFrame, "indices": DataFrame}

    Retourne :
        dict avec les statistiques d'insertion
    """
    logger.info("=" * 50)
    logger.info(" DÉMARRAGE CHARGEMENT BVC → PostgreSQL")
    logger.info("=" * 50)

    conn = get_connection()
    stats = {}

    try:
        stats["stocks_loaded"]  = load_stocks(clean_data.get("stocks",  pd.DataFrame()), conn)
        stats["indices_loaded"] = load_indices(clean_data.get("indices", pd.DataFrame()), conn)
        stats["verification"]   = verify_load(conn)
        stats["status"]         = "success"
        stats["loaded_at"]      = datetime.now().isoformat()

    except Exception as e:
        logger.error(f" Erreur critique lors du chargement : {e}")
        stats["status"] = "failed"
        stats["error"]  = str(e)
        raise

    finally:
        conn.close()
        logger.info("🔌 Connexion PostgreSQL fermée.")

    logger.info("─" * 50)
    logger.info(f" Bilan chargement :")
    logger.info(f"   → Actions  chargées : {stats.get('stocks_loaded',  0)}")
    logger.info(f"   → Indices  chargés  : {stats.get('indices_loaded', 0)}")
    logger.info(f"   → Statut            : {stats.get('status')}")
    logger.info("─" * 50)

    return stats


# ============================================================
#  TEST LOCAL (python load.py)
# ============================================================
if __name__ == "__main__":
    logger.info(" Mode test — vérification connexion PostgreSQL")

    try:
        conn = get_connection()

        # Test simple : lister les tables
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()

        if tables:
            logger.info(f" Tables disponibles dans la base :")
            for (t,) in tables:
                logger.info(f"   → {t}")
        else:
            logger.warning("  Aucune table trouvée — as-tu lancé docker-compose up ?")

        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f" Test échoué : {e}")
        logger.info(" Conseil : lance d'abord  docker-compose up  pour démarrer PostgreSQL.")