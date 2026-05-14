# Big Data — Système de Recommandation en Temps Réel
## Architecture : Kafka → Spark ALS → Airflow → Dashboard

```
bigdata-recommender/
├── docker-compose.yml
├── data/                        ← Placer Reviews.csv ici
├── kafka/
│   └── producer/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── producer.py          ← Stream vers Kafka
├── spark/
│   ├── training/
│   │   └── train_als.py         ← Entraînement ALS (80/10/10)
│   └── streaming/
│       └── streaming_recommender.py  ← Consumer Kafka + Recs temps réel
├── airflow/
│   └── dags/
│       └── recommendation_pipeline.py ← DAG Airflow
└── dashboard/
    ├── Dockerfile
    ├── app.py                   ← API Flask REST
    ├── init.sql                 ← Schéma PostgreSQL
    └── templates/index.html    ← Interface web interactive
```

---

## 1. Prérequis

```bash
# Docker + Docker Compose
docker --version          # >= 24.x
docker compose version    # >= 2.x

# RAM recommandée : 8 Go minimum
```

---

## 2. Dataset

Télécharger depuis Kaggle :
```
https://www.kaggle.com/snap/amazon-fine-food-reviews
```
Placer `Reviews.csv` dans le dossier `./data/` :
```bash
mkdir -p data
cp ~/Downloads/Reviews.csv data/
```

---

## 3. Lancer le projet

```bash
# Cloner / extraire le projet
cd bigdata-recommender

# Construire et démarrer tous les services
docker compose up --build -d

# Vérifier l'état
docker compose ps
```

---

## 4. Services et ports

| Service          | URL                        | Description              |
|------------------|----------------------------|--------------------------|
| Dashboard        | http://localhost:5000      | Interface de recommandation |
| Kafka UI         | http://localhost:8080      | Monitor les topics Kafka |
| Spark Master UI  | http://localhost:8081      | Monitor les jobs Spark   |
| Airflow UI       | http://localhost:8082      | Orchestration (admin/admin) |

---

## 5. Workflow manuel

### 5.1 Lancer l'entraînement ALS
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 2g \
  /opt/spark-apps/training/train_als.py
```

### 5.2 Lancer le streaming
```bash
docker exec -d spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0 \
  /opt/spark-apps/streaming/streaming_recommender.py
```

### 5.3 Via Airflow (recommandé)
1. Ouvrir http://localhost:8082 (admin/admin)
2. Activer le DAG `bigdata_recommendation_pipeline`
3. Cliquer ▶ Trigger DAG

---

## 6. API REST

```bash
# Recommandations pour un utilisateur
curl http://localhost:5000/api/recommendations/A3SGXH7AUHU8GW

# Liste des utilisateurs
curl http://localhost:5000/api/users

# Métriques du modèle
curl http://localhost:5000/api/metrics

# Statistiques globales
curl http://localhost:5000/api/stats
```

Réponse type :
```json
{
  "user_id": "A3SGXH7AUHU8GW",
  "recommendations": ["B001E4KFG0", "B00813GRG4", "B000LQOCH0"],
  "details": [
    {"product_id": "B001E4KFG0", "predicted_rating": 4.712},
    {"product_id": "B00813GRG4", "predicted_rating": 4.501}
  ]
}
```

---

## 7. Architecture technique

```
Reviews.csv
    │
    ▼
[Kafka Producer] ──stream──► [Kafka Topic: food-reviews]
                                       │
                   ┌───────────────────┘
                   │
    ┌──────────────▼──────────────────────────────┐
    │              Apache Spark                    │
    │  ┌─────────────────┐  ┌────────────────────┐│
    │  │  Batch Training  │  │  Structured Stream ││
    │  │  ALS MLlib       │  │  Kafka Consumer    ││
    │  │  80/10/10 split  │  │  Top-N Recs/batch  ││
    │  └────────┬────────┘  └────────┬───────────┘│
    └───────────┼────────────────────┼────────────┘
                │                    │
                ▼                    ▼
         [Model saved]        [PostgreSQL: recommendations]
         /opt/spark-models          │
                                    ▼
                             [Flask API :5000]
                                    │
                                    ▼
                             [Dashboard Web]

Orchestration : Apache Airflow (DAG planifié @daily)
```

---

## 8. Variables d'environnement

| Variable               | Défaut                  | Description            |
|------------------------|-------------------------|------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092`        | Broker Kafka           |
| `KAFKA_TOPIC`          | `food-reviews`          | Topic de streaming     |
| `DATA_PATH`            | `/data/Reviews.csv`     | Chemin dataset         |
| `DELAY_SECONDS`        | `0.1`                   | Délai entre messages   |
| `MODEL_PATH`           | `/opt/spark-models/als_model` | Modèle ALS        |
| `TOP_N`                | `10`                    | Nb de recommandations  |

---

## 9. Arrêter le projet

```bash
docker compose down          # Arrêter
docker compose down -v       # Arrêter + supprimer volumes
```
