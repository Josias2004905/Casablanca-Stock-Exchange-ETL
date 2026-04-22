"""
============================================================
  transform.py — Nettoyage & Enrichissement des données BVC
  Reçoit les DataFrames bruts d'extract.py et retourne
  des données propres, enrichies et prêtes pour PostgreSQL.

  Transformations appliquées :
    1. Nettoyage        → types, doublons, valeurs manquantes
    2. Normalisation    → formats dates, noms colonnes
    3. Enrichissement   → rendement journalier, variation abs.
    4. Validation       → cohérence des données financières
============================================================
"""

import pandas as pd
import numpy as np
from datetime import datetime
import logging

# ─────────────────────────────────────────
#  Logger
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# ============================================================
#  TRANSFORM 1 : Cours des actions
# ============================================================
def transform_stocks(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie et enrichit le DataFrame des cours d'actions BVC.

    Entrée  (colonnes brutes) :
        ticker, name, close, variation_pct, open, high, low, volume, date

    Sortie  (colonnes enrichies) :
        ticker, name, date, open, high, low, close, volume,
        variation_pct, variation_abs, amplitude, is_hausse, scraped_at
    """
    logger.info("🔄 Transformation — Actions BVC...")

    if df_raw.empty:
        logger.warning("⚠️  DataFrame vide reçu — aucune transformation effectuée.")
        return pd.DataFrame()

    df = df_raw.copy()

    # ── ÉTAPE 1 : Renommer les colonnes ──────────────────────
    logger.info("   [1/6] Normalisation des noms de colonnes...")
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # ── ÉTAPE 2 : Typage des colonnes numériques ─────────────
    logger.info("   [2/6] Conversion des types...")
    numeric_cols = ["open", "high", "low", "close", "volume", "variation_pct"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── ÉTAPE 3 : Nettoyage des valeurs aberrantes ───────────
    logger.info("   [3/6] Nettoyage des valeurs aberrantes...")

    avant = len(df)

    # Supprimer les lignes sans ticker ni cours de clôture
    df.dropna(subset=["ticker", "close"], inplace=True)

    # Supprimer les doublons (même ticker + même date)
    df.drop_duplicates(subset=["ticker", "date"], keep="last", inplace=True)

    # Cours négatifs ou nuls → incohérents
    df = df[df["close"] > 0]

    # High doit être >= Low (cohérence OHLCV)
    if "high" in df.columns and "low" in df.columns:
        masque_incoherent = (df["high"] < df["low"]) & df["high"].notna() & df["low"].notna()
        nb_incoherents = masque_incoherent.sum()
        if nb_incoherents > 0:
            logger.warning(f"   ⚠️  {nb_incoherents} ligne(s) avec high < low — corrigées (swap)")
            df.loc[masque_incoherent, ["high", "low"]] = df.loc[masque_incoherent, ["low", "high"]].values

    apres = len(df)
    logger.info(f"   → {avant - apres} ligne(s) supprimée(s) ({avant} → {apres})")

    # ── ÉTAPE 4 : Normalisation des données ──────────────────
    logger.info("   [4/6] Normalisation...")

    # Ticker en majuscules, sans espaces
    df["ticker"] = df["ticker"].str.upper().str.strip()

    # Nom de la société : casse titre
    if "name" in df.columns:
        df["name"] = df["name"].str.strip().str.title()

    # Date au format standard
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # ── ÉTAPE 5 : Enrichissement financier ───────────────────
    logger.info("   [5/6] Calcul des indicateurs financiers...")

    # Variation absolue en points (si variation_pct connue)
    if "variation_pct" in df.columns:
        df["variation_abs"] = (
            df["close"] / (1 + df["variation_pct"] / 100) * (df["variation_pct"] / 100)
        ).round(4)

    # Amplitude journalière = (High - Low) / Low * 100
    if "high" in df.columns and "low" in df.columns:
        df["amplitude_pct"] = (
            (df["high"] - df["low"]) / df["low"] * 100
        ).round(4)

    # Signal directionnel
    if "variation_pct" in df.columns:
        df["tendance"] = df["variation_pct"].apply(
            lambda x: "↑ Hausse" if x > 0 else ("↓ Baisse" if x < 0 else "= Stable")
            if pd.notna(x) else None
        )

    # Timestamp de transformation
    df["transformed_at"] = datetime.now().isoformat()

    # ── ÉTAPE 6 : Sélection et ordre final des colonnes ──────
    logger.info("   [6/6] Sélection des colonnes finales...")

    colonnes_finales = [
        "ticker", "name", "date",
        "open", "high", "low", "close",
        "volume", "variation_pct", "variation_abs",
        "amplitude_pct", "tendance",
        "scraped_at", "transformed_at"
    ]
    # Ne garder que les colonnes qui existent
    colonnes_presentes = [c for c in colonnes_finales if c in df.columns]
    df = df[colonnes_presentes].reset_index(drop=True)

    logger.info(f"✅ Transformation actions terminée — {len(df)} lignes propres.")
    return df


# ============================================================
#  TRANSFORM 2 : Indices MASI / MADEX
# ============================================================
def transform_indices(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie et enrichit le DataFrame des indices boursiers.

    Entrée  : index_name, value, variation_pct, date
    Sortie  : index_name, date, value, variation_pct,
              variation_abs, tendance, transformed_at
    """
    logger.info("🔄 Transformation — Indices BVC...")

    if df_raw.empty:
        logger.warning("⚠️  DataFrame indices vide — aucune transformation.")
        return pd.DataFrame()

    df = df_raw.copy()

    # ── Typage ───────────────────────────────────────────────
    df["value"]         = pd.to_numeric(df["value"],         errors="coerce")
    df["variation_pct"] = pd.to_numeric(df.get("variation_pct", pd.Series(dtype=float)),
                                         errors="coerce")

    # ── Nettoyage ────────────────────────────────────────────
    df.dropna(subset=["index_name", "value"], inplace=True)
    df.drop_duplicates(subset=["index_name", "date"], keep="last", inplace=True)
    df = df[df["value"] > 0]

    # ── Normalisation ────────────────────────────────────────
    df["index_name"] = df["index_name"].str.upper().str.strip()
    df["date"]       = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # ── Enrichissement ───────────────────────────────────────
    if "variation_pct" in df.columns:
        df["variation_abs"] = (
            df["value"] / (1 + df["variation_pct"] / 100) * (df["variation_pct"] / 100)
        ).round(2)

        df["tendance"] = df["variation_pct"].apply(
            lambda x: "↑ Hausse" if x > 0 else ("↓ Baisse" if x < 0 else "= Stable")
            if pd.notna(x) else None
        )

    df["transformed_at"] = datetime.now().isoformat()

    logger.info(f"✅ Transformation indices terminée — {len(df)} indices propres.")
    return df


# ============================================================
#  RAPPORT DE QUALITÉ des données
# ============================================================
def data_quality_report(df_stocks: pd.DataFrame, df_indices: pd.DataFrame):
    """
    Affiche un rapport de qualité des données transformées.
    Utile pour détecter des problèmes avant le chargement.
    """
    logger.info("=" * 50)
    logger.info(" RAPPORT DE QUALITÉ DES DONNÉES")
    logger.info("=" * 50)

    for name, df in [("Actions", df_stocks), ("Indices", df_indices)]:
        logger.info(f"\n── {name} ──────────────────────────")
        if df.empty:
            logger.warning(f"     DataFrame {name} vide !")
            continue

        logger.info(f"   Lignes         : {len(df)}")
        logger.info(f"   Colonnes       : {list(df.columns)}")

        # Valeurs manquantes
        nulls = df.isnull().sum()
        nulls = nulls[nulls > 0]
        if not nulls.empty:
            logger.warning(f"   Valeurs nulles :\n{nulls.to_string()}")
        else:
            logger.info("   Valeurs nulles : ✅ Aucune")

        # Statistiques clés
        if "close" in df.columns:
            logger.info(f"   Close min/max  : {df['close'].min():.2f} / {df['close'].max():.2f}")
        if "variation_pct" in df.columns:
            logger.info(f"   Variation moy  : {df['variation_pct'].mean():.2f}%")
        if "tendance" in df.columns:
            dist = df["tendance"].value_counts()
            logger.info(f"   Distribution   :\n{dist.to_string()}")

    logger.info("=" * 50)


# ============================================================
#  FONCTION PRINCIPALE
# ============================================================
def transform_all(raw_data: dict) -> dict:
    """
    Point d'entrée principal du module Transform.
    Appelé par le DAG Airflow après extract_all().

    Paramètre :
        raw_data : dict retourné par extract_all()
                   {"stocks": DataFrame, "indices": DataFrame}

    Retourne :
        dict {"stocks": DataFrame propre, "indices": DataFrame propre}
    """
    logger.info("=" * 50)
    logger.info("🚀 DÉMARRAGE TRANSFORMATION BVC")
    logger.info("=" * 50)

    result = {
        "stocks":  transform_stocks(raw_data.get("stocks",  pd.DataFrame())),
        "indices": transform_indices(raw_data.get("indices", pd.DataFrame())),
    }

    # Rapport de qualité
    data_quality_report(result["stocks"], result["indices"])

    return result


# ============================================================
#  TEST LOCAL (python transform.py)
# ============================================================
if __name__ == "__main__":
    # Données simulées pour tester sans scraping
    logger.info("🧪 Mode test — données simulées")

    mock_stocks = pd.DataFrame([
        {"ticker": "atw",  "name": "attijariwafa bank",         "close": 480.50, "variation_pct":  1.20,
         "open": 478.00, "high": 482.00, "low": 476.00, "volume": 12500, "date": "2024-10-15"},
        {"ticker": "bcp",  "name": "banque centrale populaire", "close": 280.00, "variation_pct": -0.50,
         "open": 281.40, "high": 282.00, "low": 279.00, "volume": 8300,  "date": "2024-10-15"},
        {"ticker": "iam",  "name": "itissalat al-maghrib",      "close": 130.10, "variation_pct":  0.00,
         "open": 130.10, "high": 131.00, "low": 129.50, "volume": 45000, "date": "2024-10-15"},
        {"ticker": "atw",  "name": "attijariwafa bank",         "close": 480.50, "variation_pct":  1.20,
         "open": 478.00, "high": 482.00, "low": 476.00, "volume": 12500, "date": "2024-10-15"},  # doublon
        {"ticker": "xxx",  "name": "action invalide",           "close": None,   "variation_pct": None,
         "open": None,   "high": None,   "low": None,   "volume": None,  "date": "2024-10-15"},  # invalide
    ])

    mock_indices = pd.DataFrame([
        {"index_name": "MASI",  "value": 13245.67, "variation_pct": 0.35, "date": "2024-10-15"},
        {"index_name": "MADEX", "value": 10812.34, "variation_pct": 0.28, "date": "2024-10-15"},
    ])

    result = transform_all({"stocks": mock_stocks, "indices": mock_indices})

    print("\n📊 Résultat — Actions transformées :")
    print(result["stocks"].to_string(index=False))

    print("\n📈 Résultat — Indices transformés :")
    print(result["indices"].to_string(index=False))