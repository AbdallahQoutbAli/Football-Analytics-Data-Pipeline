# ⚽ Football Analytics Data Pipeline

<div align="center">

![Azure](https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)

**End-to-end data engineering pipeline for football analytics**

[Architecture](#-architecture) •
[Features](#-features) •
[Setup](#-setup) •
[Pipeline](#-pipeline-workflow) •
[Data Model](#-data-model)

</div>

---

## 📋 Overview

A production-grade data engineering pipeline that processes **50M+ football events** from StatsBomb open data, transforming raw JSON into analytics-ready insights using modern cloud technologies.

### 🎯 Key Highlights

- **900+ matches** processed from multiple competitions
- **50M+ events** transformed and loaded
- **Star schema** optimized for analytics
- **Automated pipeline** with Azure Data Factory
- **Real-time dashboards** with Power BI

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA SOURCE                               │
│              StatsBomb Open Data (GitHub)                    │
│                 JSON Files • 900+ Matches                    │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                  AZURE BLOB STORAGE                          │
│                    (Raw Data Lake)                           │
│         competitions.json │ matches/ │ events/               │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│               AZURE DATA FACTORY                             │
│                  (Orchestration)                             │
│     ┌─────────────┐    ┌─────────────┐    ┌─────────────┐   │
│     │  Schedule   │───▶│   Trigger   │───▶│   Monitor   │   │
│     │  Pipeline   │    │  Databricks │    │  Execution  │   │
│     └─────────────┘    └─────────────┘    └─────────────┘   │
└─────────────────────────┬───────────────────────────────────┘
                          │ triggers
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    DATABRICKS                                │
│               (Transformation Layer)                         │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  📓 01_Load_data_from_Statsbomb                      │   │
│  │     • Clone StatsBomb repo                           │   │
│  │     • Copy to DBFS                                   │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  📓 02_Transform_Load_Snowflake                      │   │
│  │     • PySpark transformations                        │   │
│  │     • Calculate xG, pass stats                       │   │
│  │     • Load to Snowflake                              │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────┬───────────────────────────────────┘
                          │ writes to
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   SNOWFLAKE ❄️                               │
│                 (Data Warehouse)                             │
│                                                              │
│    ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │
│    │    DIM_     │  │   FACT_     │  │   FACT_     │       │
│    │COMPETITIONS │  │  MATCHES    │  │  EVENTS     │       │
│    └─────────────┘  └─────────────┘  └─────────────┘       │
│    ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │
│    │   FACT_     │  │   FACT_     │  │  MANUTD_    │       │
│    │   PASSES    │  │   SHOTS     │  │  MATCHES    │       │
│    └─────────────┘  └─────────────┘  └─────────────┘       │
│                                                              │
│    ✓ Star Schema  ✓ Time Travel  ✓ Auto-scaling            │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                  ANALYTICS LAYER                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │  Power BI   │  │  Streamlit  │  │   FastAPI   │         │
│  │ Dashboards  │  │    App      │  │    API      │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
```

---

## ✨ Features

| Feature | Description |
|---------|-------------|
| 🔄 **Automated Ingestion** | Clone and process StatsBomb data automatically |
| ⚡ **Distributed Processing** | PySpark on Databricks for 50M+ events |
| 📊 **Advanced Metrics** | xG, pass completion %, shot conversion rates |
| ❄️ **Cloud Data Warehouse** | Snowflake with star schema design |
| 📈 **Interactive Dashboards** | Power BI for real-time analytics |
| 🔗 **Pipeline Orchestration** | Azure Data Factory for scheduling |

---

## 🚀 Setup

### Prerequisites

- Azure Subscription
- Databricks Workspace  
- Snowflake Account
- Power BI Desktop (for dashboards)

### 1️⃣ Clone Repository

```bash
git clone https://github.com/yourusername/football-analytics-pipeline.git
cd football-analytics-pipeline
```

### 2️⃣ Snowflake Setup

```sql
-- Create database and schema
CREATE DATABASE FOOTBALL_ANALYTICS;
CREATE SCHEMA FOOTBALL_ANALYTICS.MANUTD;

-- Create warehouse
CREATE WAREHOUSE ANALYTICS_WH 
    WITH WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;
```

### 3️⃣ Databricks Setup

1. Import notebooks from `/notebooks` folder to your Databricks workspace
2. Update Snowflake credentials in `02_Transform_Load_Snowflake.py`:
   ```python
   SNOWFLAKE_OPTIONS = {
       "sfUrl": "YOUR_ACCOUNT.snowflakecomputing.com",   
       "sfUser": "YOUR_USERNAME",           
       "sfPassword": "YOUR_PASSWORD",                
       "sfDatabase": "FOOTBALL_ANALYTICS",
       "sfSchema": "MANUTD",
       "sfWarehouse": "ANALYTICS_WH"
   }
   ```

### 4️⃣ Azure Data Factory

1. Create ADF instance in Azure Portal
2. Create Linked Service for Databricks
3. Create Pipeline with Notebook activity
4. Configure trigger (schedule/manual)

---

## 📁 Project Structure

```
football-analytics-pipeline/
│
├── 📂 notebooks/                          # Databricks notebooks
│   ├── 01_Load_data_from_Statsbomb.ipynb  # Data ingestion
│   └── 02_Transform_Load_Snowflake.py     # ETL & load
│
├── 📂 sql/                                # Snowflake scripts
│   ├── 01_create_database.sql
│   └── 02_create_tables.sql
│
├── 📂 adf-pipelines/                      # ADF ARM templates
│   └── pipeline_football_etl.json
│
├── 📂 dashboard/                          # Visualization
│   ├── powerbi/
│   └── streamlit/
│
├── 📂 docs/                               # Documentation
│   └── images/
│
├── 📂 config/                             # Configuration
│   └── .env.example
│
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 🔄 Pipeline Workflow

### Notebook 1: Data Ingestion

```python
# Clone StatsBomb open data from GitHub
%sh
git clone --depth 1 https://github.com/statsbomb/open-data.git

# Copy to Databricks File System (DBFS)
dbutils.fs.cp("file:/tmp/open-data/data", "dbfs:/statsbomb-data", recurse=True)
```

**Output:**
```
📥 Downloading from GitHub...
✅ Download complete!
📦 Copying to DBFS...
✅ Data ready in DBFS!
```

### Notebook 2: Transform & Load

```python
# Read competitions
competitions_df = spark.read.json(f"{DATA_PATH}/competitions.json")

# Read all matches
matches_df = spark.read.json(f"{DATA_PATH}/matches/*/*.json")

# Transform matches with calculated fields
matches_clean = matches_df.select(
    col("match_id"),
    col("home_team.home_team_name").alias("home_team_name"),
    col("away_team.away_team_name").alias("away_team_name"),
    col("home_score"),
    col("away_score")
).withColumn(
    "winner",
    when(col("home_score") > col("away_score"), "HOME")
    .when(col("away_score") > col("home_score"), "AWAY")
    .otherwise("DRAW")
).withColumn(
    "total_goals",
    col("home_score") + col("away_score")
)

# Load to Snowflake
matches_clean.write.format("snowflake").options(**SNOWFLAKE_OPTIONS).save()
```

---

## 📊 Data Model

### Star Schema Design

```
                    ┌─────────────────┐
                    │ DIM_COMPETITIONS│
                    ├─────────────────┤
                    │ competition_id  │◄─────────────┐
                    │ competition_name│              │
                    │ season_id       │              │
                    │ season_name     │              │
                    │ country         │              │
                    └─────────────────┘              │
                                                    │
         ┌──────────────────────────────────────────┼──────────────────┐
         │                                          │                  │
         ▼                                          │                  ▼
┌─────────────────┐                                 │         ┌─────────────────┐
│  FACT_MATCHES   │                                 │         │  FACT_EVENTS    │
├─────────────────┤                                 │         ├─────────────────┤
│ match_id (PK)   │─────────────────────────────────┤         │ event_id (PK)   │
│ competition_id  │                                 │         │ match_id (FK)   │
│ season_id       │                                 │         │ team_id         │
│ home_team_id    │                                 │         │ player_id       │
│ away_team_id    │                                 │         │ player_name     │
│ home_score      │                                 │         │ event_type      │
│ away_score      │                                 │         │ minute          │
│ winner          │                                 │         │ location_x      │
│ total_goals     │                                 │         │ location_y      │
└─────────────────┘                                 │         └─────────────────┘
                                                    │                  │
                                                    │         ┌────────┴────────┐
                                                    │         ▼                 ▼
                                                    │  ┌─────────────┐   ┌─────────────┐
                                                    │  │ FACT_PASSES │   │ FACT_SHOTS  │
                                                    │  ├─────────────┤   ├─────────────┤
                                                    │  │ event_id    │   │ event_id    │
                                                    │  │ match_id    │   │ match_id    │
                                                    │  │ player_name │   │ player_name │
                                                    │  │ team_name   │   │ team_name   │
                                                    │  │ pass_length │   │ xg          │
                                                    │  │ pass_angle  │   │ shot_outcome│
                                                    │  │ pass_outcome│   │ shot_type   │
                                                    │  │ recipient   │   │ body_part   │
                                                    └──│ under_press │   │ under_press │
                                                       └─────────────┘   └─────────────┘
```

### Snowflake Tables

| Table | Type | Records | Description |
|-------|------|---------|-------------|
| `DIM_COMPETITIONS` | Dimension | ~50 | Leagues & tournaments |
| `FACT_MATCHES` | Fact | 900+ | Match results & scores |
| `FACT_EVENTS` | Fact | 50M+ | All match events |
| `FACT_PASSES` | Fact | 30M+ | Pass details & outcomes |
| `FACT_SHOTS` | Fact | 500K+ | Shots with xG values |
| `MANUTD_MATCHES` | Fact | Varies | Man United specific |

---

## 📈 Key Metrics Calculated

| Metric | Formula | Description |
|--------|---------|-------------|
| **xG (Expected Goals)** | StatsBomb model | Probability of shot becoming goal |
| **Pass Completion %** | `completed / total * 100` | Successful pass rate |
| **Shot Conversion %** | `goals / shots * 100` | Goals per shot |
| **Total Goals** | `home_score + away_score` | Match total |
| **Winner** | Logic based on scores | HOME/AWAY/DRAW |

---

## 🔍 Sample Queries

### Top Scorers by xG
```sql
SELECT 
    player_name,
    team_name,
    COUNT(*) as total_shots,
    ROUND(SUM(xg), 2) as total_xg,
    SUM(CASE WHEN shot_outcome = 'Goal' THEN 1 ELSE 0 END) as goals,
    ROUND(SUM(CASE WHEN shot_outcome = 'Goal' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as conversion_rate
FROM FACT_SHOTS
GROUP BY player_name, team_name
ORDER BY total_xg DESC
LIMIT 20;
```

### Pass Completion by Team
```sql
SELECT 
    team_name,
    COUNT(*) as total_passes,
    SUM(CASE WHEN pass_outcome IS NULL THEN 1 ELSE 0 END) as completed,
    ROUND(SUM(CASE WHEN pass_outcome IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as completion_rate
FROM FACT_PASSES
GROUP BY team_name
ORDER BY completion_rate DESC
LIMIT 20;
```

### Event Distribution
```sql
SELECT 
    event_type,
    COUNT(*) as event_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM FACT_EVENTS
GROUP BY event_type
ORDER BY event_count DESC;
```

---

## 🛠 Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Data Source** | StatsBomb Open Data | Football event data |
| **Storage** | Azure Blob / DBFS | Raw data lake |
| **Orchestration** | Azure Data Factory | Pipeline scheduling & monitoring |
| **Processing** | Databricks (PySpark) | Distributed transformation |
| **Warehouse** | Snowflake | Analytics-optimized storage |
| **Visualization** | Power BI | Interactive dashboards |
| **Language** | Python, SQL | ETL & queries |

---

## 📊 Pipeline Output Summary

```
============================================================
✅ DATA PIPELINE COMPLETE!
============================================================

📊 Data Processed:
   Competitions: 50
   Matches: 900+
   Events (sample): 500,000+
   Passes: 300,000+
   Shots: 50,000+

❄️  Snowflake Tables Created:
   • DIM_COMPETITIONS
   • FACT_MATCHES
   • FACT_EVENTS
   • FACT_PASSES
   • FACT_SHOTS
   • MANUTD_MATCHES

🚀 Ready for Analytics & Visualization!
============================================================
```

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

---

## 📄 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file.

---

## 🙏 Acknowledgments

- [StatsBomb](https://statsbomb.com/) for providing open football data
- [Databricks](https://databricks.com/) for unified analytics platform
- [Snowflake](https://snowflake.com/) for cloud data warehouse

---

<div align="center">

**⭐ Star this repo if you find it useful!**

Made with ❤️ for Football & Data Engineering

</div>
