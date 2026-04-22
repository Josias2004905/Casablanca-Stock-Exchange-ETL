"""
============================================================
  bvc_pipeline.py — DAG Airflow ETL Bourse de Casablanca
  Orchestre les 3 étapes du pipeline :
    Extract  → Transform  → Load

  Schedule : Chaque jour ouvré à 18h00 (après clôture BVC)
  Clôture BVC : 15h30 heure de Casablanca (UTC+1)
============================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging
import sys
import os

# Ajouter le dossier scripts au path Python
sys.path.insert(0, "/opt/airflow/scripts")

logger = logging.getLogger(__name__)


# ============================================================
#  CONFIGURATION DU DAG
# ============================================================
DEFAULT_ARGS = {
    "owner":            "bvc-etl",
    "depends_on_past":  False,          # Ne pas attendre le run précédent
    "retries":          2,              # 2 nouvelles tentatives en cas d'échec
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,          # Mettre True + configurer SMTP pour alertes
    "email_on_retry":   False,
}


# ============================================================
#  TÂCHE 1 : EXTRACT
# ============================================================
def task_extract(**context):
    """
    Tâche Airflow : Scrape les données BVC.
    Pousse les DataFrames sérialisés dans XCom
    pour transmission à la tâche suivante.
    """
    from extract import extract_all
    import pandas as pd

    logger.info("=" * 50)
    logger.info("🚀 TÂCHE 1 — EXTRACT")
    logger.info("=" * 50)

    # Lancer l'extraction
    raw_data = extract_all()

    # Vérification : données non vides ?
    nb_stocks  = len(raw_data.get("stocks",  pd.DataFrame()))
    nb_indices = len(raw_data.get("indices", pd.DataFrame()))

    if nb_stocks == 0 and nb_indices == 0:
        raise ValueError("❌ Extraction vide — aucune donnée récupérée depuis la BVC.")

    logger.info(f"✅ Extract terminé : {nb_stocks} actions, {nb_indices} indices")

    # Pousser dans XCom (sérialisation JSON)
    # XCom = mécanisme Airflow pour passer des données entre tâches
    context["ti"].xcom_push(
        key="stocks_json",
        value=raw_data["stocks"].to_json(orient="records", date_format="iso")
        if nb_stocks > 0 else "[]"
    )
    context["ti"].xcom_push(
        key="indices_json",
        value=raw_data["indices"].to_json(orient="records", date_format="iso")
        if nb_indices > 0 else "[]"
    )

    # Métriques pour les logs Airflow
    return {"stocks": nb_stocks, "indices": nb_indices}


# ============================================================
#  TÂCHE 2 : TRANSFORM
# ============================================================
def task_transform(**context):
    """
    Tâche Airflow : Nettoie et enrichit les données.
    Récupère les données depuis XCom (tâche précédente),
    applique les transformations, repousse dans XCom.
    """
    from transform import transform_all
    import pandas as pd

    logger.info("=" * 50)
    logger.info("🚀 TÂCHE 2 — TRANSFORM")
    logger.info("=" * 50)

    ti = context["ti"]

    # Récupérer les données de la tâche Extract via XCom
    stocks_json  = ti.xcom_pull(task_ids="extract", key="stocks_json")
    indices_json = ti.xcom_pull(task_ids="extract", key="indices_json")

    # Désérialiser JSON → DataFrame
    df_stocks  = pd.read_json(stocks_json,  orient="records") if stocks_json  != "[]" else pd.DataFrame()
    df_indices = pd.read_json(indices_json, orient="records") if indices_json != "[]" else pd.DataFrame()

    logger.info(f"   Données reçues : {len(df_stocks)} actions, {len(df_indices)} indices")

    # Appliquer les transformations
    clean_data = transform_all({"stocks": df_stocks, "indices": df_indices})

    nb_stocks  = len(clean_data["stocks"])
    nb_indices = len(clean_data["indices"])

    logger.info(f"✅ Transform terminé : {nb_stocks} actions propres, {nb_indices} indices propres")

    # Pousser les données propres dans XCom
    ti.xcom_push(
        key="clean_stocks_json",
        value=clean_data["stocks"].to_json(orient="records", date_format="iso")
        if nb_stocks > 0 else "[]"
    )
    ti.xcom_push(
        key="clean_indices_json",
        value=clean_data["indices"].to_json(orient="records", date_format="iso")
        if nb_indices > 0 else "[]"
    )

    return {"stocks_clean": nb_stocks, "indices_clean": nb_indices}


# ============================================================
#  TÂCHE 3 : LOAD
# ============================================================
def task_load(**context):
    """
    Tâche Airflow : Charge les données dans PostgreSQL.
    Récupère les données propres depuis XCom et les insère.
    """
    from load import load_all
    import pandas as pd

    logger.info("=" * 50)
    logger.info("🚀 TÂCHE 3 — LOAD")
    logger.info("=" * 50)

    ti = context["ti"]

    # Récupérer les données propres depuis XCom
    clean_stocks_json  = ti.xcom_pull(task_ids="transform", key="clean_stocks_json")
    clean_indices_json = ti.xcom_pull(task_ids="transform", key="clean_indices_json")

    df_stocks  = pd.read_json(clean_stocks_json,  orient="records") if clean_stocks_json  != "[]" else pd.DataFrame()
    df_indices = pd.read_json(clean_indices_json, orient="records") if clean_indices_json != "[]" else pd.DataFrame()

    logger.info(f"   Données à charger : {len(df_stocks)} actions, {len(df_indices)} indices")

    # Charger dans PostgreSQL
    stats = load_all({"stocks": df_stocks, "indices": df_indices})

    logger.info(f"✅ Load terminé — statut : {stats.get('status')}")
    return stats


# ============================================================
#  TÂCHE 4 : VÉRIFICATION FINALE
# ============================================================
def task_verify(**context):
    """
    Tâche finale : vérifie que le pipeline s'est bien déroulé
    et affiche un résumé dans les logs Airflow.
    """
    ti = context["ti"]

    # Récupérer les résultats des tâches précédentes
    extract_result   = ti.xcom_pull(task_ids="extract")
    transform_result = ti.xcom_pull(task_ids="transform")
    load_result      = ti.xcom_pull(task_ids="load")

    logger.info("=" * 50)
    logger.info("📋 RAPPORT FINAL DU PIPELINE ETL")
    logger.info(f"   Date d'exécution : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)
    logger.info(f"   [Extract]   Actions : {extract_result.get('stocks', 0)}  | Indices : {extract_result.get('indices', 0)}")
    logger.info(f"   [Transform] Actions : {transform_result.get('stocks_clean', 0)} | Indices : {transform_result.get('indices_clean', 0)}")
    logger.info(f"   [Load]      Statut  : {load_result.get('status', 'inconnu')}")

    if load_result.get("verification"):
        for table, info in load_result["verification"].items():
            logger.info(f"   [DB] {table:<20} : {info['count']} lignes | Dernière date : {info['last_date']}")

    logger.info("=" * 50)
    logger.info("✅ Pipeline ETL BVC terminé avec succès !")

    return {"pipeline_status": "success", "run_date": datetime.now().isoformat()}


# ============================================================
#  DÉFINITION DU DAG
# ============================================================
with DAG(
    dag_id="bvc_etl_pipeline",

    description="Pipeline ETL quotidien — Bourse de Casablanca",

    # ── Planification ──────────────────────────────────────
    # "0 17 * * 1-5" = chaque jour ouvré (lun-ven) à 17h UTC
    # = 18h heure de Casablanca (UTC+1), après clôture à 15h30
    schedule_interval="0 17 * * 1-5",

    start_date=datetime(2024, 10, 1),
    catchup=False,                  # Ne pas remonter les runs passés

    default_args=DEFAULT_ARGS,

    # ── Tags (pour filtrer dans l'UI Airflow) ──────────────
    tags=["bvc", "finance", "etl", "maroc"],

    # ── Documentation du DAG ───────────────────────────────
    doc_md="""
    ## Pipeline ETL — Bourse de Casablanca (BVC)

    Ce DAG orchestre le pipeline de données financières :

    | Tâche     | Description                          | Fréquence     |
    |-----------|--------------------------------------|---------------|
    | extract   | Scraping BVC (actions + indices)     | Quotidienne   |
    | transform | Nettoyage, calculs financiers        | Quotidienne   |
    | load      | Insertion PostgreSQL (UPSERT)        | Quotidienne   |
    | verify    | Rapport de qualité des données       | Quotidienne   |

    **Schedule** : Lundi–Vendredi à 18h00 (heure de Casablanca)
    """,

) as dag:

    # ── Définition des tâches ──────────────────────────────

    extract = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
        doc_md="Scrape les cours et indices depuis casablanca-bourse.com",
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
        doc_md="Nettoie les données et calcule les indicateurs financiers",
    )

    load = PythonOperator(
        task_id="load",
        python_callable=task_load,
        doc_md="Insère les données propres dans PostgreSQL via UPSERT",
    )

    verify = PythonOperator(
        task_id="verify",
        python_callable=task_verify,
        doc_md="Vérifie le bon déroulement du pipeline et affiche le rapport",
    )

    # ── Ordre d'exécution ──────────────────────────────────
    #
    #   extract  →  transform  →  load  →  verify
    #
    extract >> transform >> load >> verify