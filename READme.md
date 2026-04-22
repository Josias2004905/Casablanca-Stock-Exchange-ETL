# 📈 ETL Bourse de Casablanca (BVC)

Pipeline ETL automatisé pour l'analyse des données du marché financier marocain.

## 🏗️ Architecture

```
[BVC Website] → Extract → Transform → Load → [PostgreSQL]
                  ↑            ↑          ↑
            BeautifulSoup   pandas    psycopg2
                        orchestré par Airflow
```

## 📁 Structure du projet

```
bvc-etl/
├── dags/
│   └── bvc_pipeline.py      # DAG Airflow (orchestration)
├── scripts/
│   ├── extract.py            # Scraping BVC
│   ├── transform.py          # Nettoyage & enrichissement
│   └── load.py               # Chargement PostgreSQL
├── sql/
│   └── init.sql              # Schéma de la base de données
├── tests/
│   └── test_pipeline.py      # Tests unitaires (29 tests)
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env                      # Variables privées (ne pas committer)
```

## 🚀 Déploiement en 5 étapes

### Étape 1 — Prérequis

Installe [Docker Desktop](https://www.docker.com/products/docker-desktop/) sur ta machine.

```bash
docker --version        # Docker 24+
docker compose version  # Docker Compose 2+
```

---

### Étape 2 — Cloner et configurer

```bash
# Cloner le projet
git clone https://github.com/ton-username/bvc-etl.git
cd bvc-etl

# Créer le fichier de configuration (déjà fourni, vérifie les valeurs)
cat .env
```

Le fichier `.env` contient :
```env
POSTGRES_DB=bvc_db
POSTGRES_USER=bvc_admin
POSTGRES_PASSWORD=bvc_secret_2024
AIRFLOW_USER=admin
AIRFLOW_PASSWORD=admin123
AIRFLOW_SECRET_KEY=une_cle_secrete_aleatoire_ici
```

> ⚠️ Change les mots de passe avant tout déploiement en production.

---

### Étape 3 — Lancer les services

```bash
# Construire les images et démarrer tous les services
docker compose up --build

# (optionnel) Mode détaché (tourne en arrière-plan)
docker compose up --build -d
```

Ce que tu verras dans les logs :
```
bvc_postgres          | database system is ready to accept connections
bvc_airflow_init      | DB initialized ✓
bvc_airflow_webserver | Listening at: http://0.0.0.0:8080
bvc_airflow_scheduler | Scheduler started
```

---

### Étape 4 — Accéder à l'interface Airflow

Ouvre ton navigateur sur **http://localhost:8080**

| Champ    | Valeur     |
|----------|------------|
| Username | `admin`    |
| Password | `admin123` |

Tu verras le DAG `bvc_etl_pipeline` dans la liste.

---

### Étape 5 — Lancer le pipeline

**Option A — Via l'interface web Airflow :**
1. Clique sur le DAG `bvc_etl_pipeline`
2. Clique sur ▶️ **Trigger DAG** (bouton Play)
3. Surveille l'exécution dans le Graph View

**Option B — Via la ligne de commande :**
```bash
docker exec bvc_airflow_scheduler \
  airflow dags trigger bvc_etl_pipeline
```

---

## 🔍 Vérifier les données dans PostgreSQL

```bash
# Ouvrir un terminal PostgreSQL
docker exec -it bvc_postgres psql -U bvc_admin -d bvc_db

# Requêtes utiles
SELECT COUNT(*) FROM stock_prices;
SELECT COUNT(*) FROM market_indices;

SELECT ticker, close, variation_pct, tendance
FROM stock_prices
WHERE date = CURRENT_DATE
ORDER BY variation_pct DESC
LIMIT 10;

SELECT * FROM market_indices ORDER BY date DESC LIMIT 5;
```

---

## 🧪 Lancer les tests

```bash
# Installer les dépendances
pip install -r requirements.txt pytest

# Lancer tous les tests
pytest tests/ -v

# Résultat attendu : 29 passed
```

---

## ⏰ Schedule du pipeline

Le DAG s'exécute automatiquement **chaque jour ouvré à 18h00** (heure de Casablanca) :

```
Clôture BVC  : 15h30
Pipeline ETL : 18h00  (après clôture + marge de sécurité)
```

Expression cron : `0 17 * * 1-5` (17h UTC = 18h Casablanca UTC+1)

---

## 🛠️ Commandes utiles

```bash
# Voir les logs en temps réel
docker compose logs -f airflow-scheduler

# Voir l'état des services
docker compose ps

# Arrêter tous les services
docker compose down

# Arrêter et supprimer les données (reset complet)
docker compose down -v

# Relancer après modification du code
docker compose restart airflow-scheduler
```

---

## 📦 Stack technique

| Outil          | Version | Rôle                    |
|----------------|---------|-------------------------|
| Python         | 3.12    | Langage principal        |
| Apache Airflow | 2.8.0   | Orchestration du DAG     |
| PostgreSQL     | 15      | Stockage des données     |
| BeautifulSoup  | 4.12    | Web scraping             |
| pandas         | 2.1     | Transformation des données|
| psycopg2       | 2.9     | Connexion PostgreSQL     |
| Docker Compose | 2.x     | Containerisation         |