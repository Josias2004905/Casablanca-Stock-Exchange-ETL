"""
============================================================
  tests/test_pipeline.py — Tests unitaires ETL BVC
  Valide chaque module sans connexion à la BVC ni PostgreSQL.

  Lancer : pytest tests/ -v
============================================================
"""

import pytest
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))


# ============================================================
#  FIXTURES — Données de test réutilisables
# ============================================================

@pytest.fixture
def raw_stocks():
    """DataFrame brut simulant la sortie d'extract.py"""
    return pd.DataFrame([
        {"ticker": "atw",  "name": "attijariwafa bank",         "close": 480.50, "variation_pct":  1.20,
         "open": 478.00, "high": 482.00, "low": 476.00, "volume": 12500, "date": "2024-10-15"},
        {"ticker": "bcp",  "name": "banque centrale populaire", "close": 280.00, "variation_pct": -0.50,
         "open": 281.40, "high": 282.00, "low": 279.00, "volume": 8300,  "date": "2024-10-15"},
        {"ticker": "iam",  "name": "itissalat al-maghrib",      "close": 130.10, "variation_pct":  0.00,
         "open": 130.10, "high": 131.00, "low": 129.50, "volume": 45000, "date": "2024-10-15"},
        # Doublon → doit être supprimé
        {"ticker": "atw",  "name": "attijariwafa bank",         "close": 480.50, "variation_pct":  1.20,
         "open": 478.00, "high": 482.00, "low": 476.00, "volume": 12500, "date": "2024-10-15"},
        # Ligne invalide (sans cours) → doit être supprimée
        {"ticker": "xxx",  "name": "action invalide",           "close": None,   "variation_pct": None,
         "open": None,     "high": None,   "low": None,   "volume": None,  "date": "2024-10-15"},
    ])


@pytest.fixture
def raw_indices():
    """DataFrame brut simulant la sortie d'extract.py pour les indices"""
    return pd.DataFrame([
        {"index_name": "MASI",  "value": 13245.67, "variation_pct":  0.35, "date": "2024-10-15"},
        {"index_name": "MADEX", "value": 10812.34, "variation_pct": -0.12, "date": "2024-10-15"},
    ])


@pytest.fixture
def clean_stocks(raw_stocks):
    """DataFrame propre après transform"""
    from transform import transform_stocks
    return transform_stocks(raw_stocks)


@pytest.fixture
def clean_indices(raw_indices):
    """DataFrame propre après transform"""
    from transform import transform_indices
    return transform_indices(raw_indices)


# ============================================================
#  TESTS — MODULE EXTRACT (parse_number)
# ============================================================

class TestExtractParseNumber:
    """Tests de la fonction parse_number (conversion numérique)"""

    def setup_method(self):
        from extract import parse_number
        self.parse = parse_number

    def test_format_francais(self):
        """'1 234,56' doit devenir 1234.56"""
        assert self.parse("1 234,56") == 1234.56

    def test_format_anglais(self):
        """'1234.56' doit devenir 1234.56"""
        assert self.parse("1234.56") == 1234.56

    def test_entier(self):
        assert self.parse("480") == 480.0

    def test_negatif(self):
        assert self.parse("-0,50") == -0.50

    def test_vide_retourne_none(self):
        assert self.parse("") is None
        assert self.parse("-") is None
        assert self.parse("N/A") is None

    def test_none_retourne_none(self):
        assert self.parse(None) is None


# ============================================================
#  TESTS — MODULE TRANSFORM (stocks)
# ============================================================

class TestTransformStocks:
    """Tests des transformations sur les actions"""

    def test_supprime_doublons(self, clean_stocks):
        """ATW apparaît 2 fois en entrée → 1 seule fois en sortie"""
        assert clean_stocks["ticker"].value_counts().max() == 1

    def test_supprime_lignes_sans_cours(self, clean_stocks):
        """La ligne XXX sans cours doit être absente"""
        assert "XXX" not in clean_stocks["ticker"].values

    def test_ticker_en_majuscules(self, clean_stocks):
        """Tous les tickers doivent être en majuscules"""
        assert all(t == t.upper() for t in clean_stocks["ticker"])

    def test_colonne_tendance_presente(self, clean_stocks):
        """La colonne 'tendance' doit exister"""
        assert "tendance" in clean_stocks.columns

    def test_tendance_hausse(self, clean_stocks):
        """ATW variation +1.20% → tendance ↑ Hausse"""
        row = clean_stocks[clean_stocks["ticker"] == "ATW"].iloc[0]
        assert row["tendance"] == "↑ Hausse"

    def test_tendance_baisse(self, clean_stocks):
        """BCP variation -0.50% → tendance ↓ Baisse"""
        row = clean_stocks[clean_stocks["ticker"] == "BCP"].iloc[0]
        assert row["tendance"] == "↓ Baisse"

    def test_tendance_stable(self, clean_stocks):
        """IAM variation 0.00% → tendance = Stable"""
        row = clean_stocks[clean_stocks["ticker"] == "IAM"].iloc[0]
        assert row["tendance"] == "= Stable"

    def test_amplitude_calculee(self, clean_stocks):
        """L'amplitude (high-low)/low doit être calculée et positive"""
        assert "amplitude_pct" in clean_stocks.columns
        assert (clean_stocks["amplitude_pct"] >= 0).all()

    def test_variation_abs_calculee(self, clean_stocks):
        """variation_abs doit être présente"""
        assert "variation_abs" in clean_stocks.columns

    def test_format_date(self, clean_stocks):
        """La date doit être au format YYYY-MM-DD"""
        import re
        pattern = r"^\d{4}-\d{2}-\d{2}$"
        assert all(re.match(pattern, d) for d in clean_stocks["date"])

    def test_close_positif(self, clean_stocks):
        """Tous les cours de clôture doivent être > 0"""
        assert (clean_stocks["close"] > 0).all()

    def test_nombre_lignes_final(self, clean_stocks):
        """5 lignes en entrée → 3 lignes propres (1 doublon + 1 invalide supprimés)"""
        assert len(clean_stocks) == 3

    def test_dataframe_vide_retourne_vide(self):
        """Un DataFrame vide en entrée → DataFrame vide en sortie"""
        from transform import transform_stocks
        result = transform_stocks(pd.DataFrame())
        assert result.empty


# ============================================================
#  TESTS — MODULE TRANSFORM (indices)
# ============================================================

class TestTransformIndices:
    """Tests des transformations sur les indices"""

    def test_nb_indices(self, clean_indices):
        """2 indices en entrée → 2 indices en sortie"""
        assert len(clean_indices) == 2

    def test_index_name_majuscules(self, clean_indices):
        """MASI et MADEX doivent être en majuscules"""
        assert set(clean_indices["index_name"]) == {"MASI", "MADEX"}

    def test_valeurs_positives(self, clean_indices):
        """Les valeurs des indices doivent être > 0"""
        assert (clean_indices["value"] > 0).all()

    def test_tendance_masi(self, clean_indices):
        """MASI +0.35% → ↑ Hausse"""
        row = clean_indices[clean_indices["index_name"] == "MASI"].iloc[0]
        assert row["tendance"] == "↑ Hausse"

    def test_tendance_madex(self, clean_indices):
        """MADEX -0.12% → ↓ Baisse"""
        row = clean_indices[clean_indices["index_name"] == "MADEX"].iloc[0]
        assert row["tendance"] == "↓ Baisse"

    def test_dataframe_vide_retourne_vide(self):
        """Un DataFrame vide en entrée → DataFrame vide en sortie"""
        from transform import transform_indices
        result = transform_indices(pd.DataFrame())
        assert result.empty


# ============================================================
#  TESTS — MODULE LOAD (_prepare_rows)
# ============================================================

class TestLoadPrepareRows:
    """Tests de la préparation des données avant insertion SQL"""

    def test_nan_converti_en_none(self):
        """Les NaN pandas doivent devenir None (NULL SQL)"""
        from load import _prepare_rows
        df = pd.DataFrame([{"ticker": "ATW", "close": float("nan"), "date": "2024-10-15"}])
        rows = _prepare_rows(df, ["ticker", "close", "date"])
        assert rows[0]["close"] is None

    def test_scrapedat_ajoute_si_absent(self):
        """scraped_at doit être ajouté si absent du DataFrame"""
        from load import _prepare_rows
        df = pd.DataFrame([{"ticker": "ATW", "close": 480.5}])
        rows = _prepare_rows(df, ["ticker", "close", "scraped_at"])
        assert rows[0]["scraped_at"] is not None

    def test_colonnes_manquantes_ignorees(self):
        """Les colonnes demandées mais absentes du DataFrame sont ignorées"""
        from load import _prepare_rows
        df = pd.DataFrame([{"ticker": "ATW"}])
        rows = _prepare_rows(df, ["ticker", "colonne_inexistante"])
        assert "colonne_inexistante" not in rows[0]
        assert rows[0]["ticker"] == "ATW"

    def test_dataframe_vide_retourne_liste_vide(self):
        """Un DataFrame vide → liste vide"""
        from load import _prepare_rows
        rows = _prepare_rows(pd.DataFrame(), ["ticker", "close"])
        assert rows == []