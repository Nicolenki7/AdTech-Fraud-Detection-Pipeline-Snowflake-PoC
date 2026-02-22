# 🛡️ AdTech Fraud Detection Pipeline — Snowflake PoC

**End-to-end fraud detection workflow processing 10TB+ simulated logs with SQL filtering, Snowpark Python, and native UDF deployment.**

---

## 📖 Business Context

AdTech fraud costs the industry **billions annually** through click fraud, impression fraud, and bot traffic. This Proof of Concept demonstrates a scalable approach to identifying and classifying fraudulent activity at massive scale.

### Key Business Questions Answered:
- **Which IPs exhibit fraudulent behavior patterns?** (High-velocity clicks, UA rotation)
- **What is the risk score for each suspect?** (Feature engineering + classification)
- **What action should be taken?** (BLOCK immediately vs. MANUAL REVIEW)
- **How do we process 10TB efficiently?** (Multi-stage filtering funnel)

### Business Impact:
- **99%+ data reduction:** From 10TB raw logs to <1% actionable suspects
- **Cost optimization:** X-SMALL Snowflake warehouse for filtering
- **Real-time classification:** Python UDFs deployed natively in Snowflake
- **Actionable output:** Clear BLOCK/REVIEW recommendations

---

## 🎯 Features

### Multi-Stage Filtering Funnel
```
10TB Raw Logs
     ↓
[SQL Phase 1] High-Volume Filtering (COUNT/HAVING)
     ↓
[SQL Phase 2] High-Velocity Filtering (LAG/Window Functions)
     ↓
~1% Suspect IPs
     ↓
[Snowpark] Feature Engineering + Risk Scoring
     ↓
[UDF] Classification (BLOCK/REVIEW)
     ↓
Actionable Alerts Table
```

### Technical Highlights
- **SQL Heuristics:** Window functions, CTEs, HAVING clauses for pattern detection
- **Snowpark DataFrames:** Lazy evaluation, feature engineering with `when/otherwise`
- **Python UDFs:** Native deployment in Snowflake for scalable classification
- **Cost Efficiency:** Process terabytes with minimal compute resources

---

## 🏗️ Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                    Snowflake Platform                          │
├────────────────────────────────────────────────────────────────┤
│  RAW_LOGS Schema                                               │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ BID_LOGS (10TB simulated)                                │  │
│  │ - ip, timestamp, user_agent, event_type, campaign_id     │  │
│  └──────────────────────────────────────────────────────────┘  │
├────────────────────────────────────────────────────────────────┤
│  SQL Filtering Phase                                           │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ SUSPECT_IPS (<1% of original)                            │  │
│  │ - Heuristic flags: HIGH_VELOCITY_BOT, UA_ROTATION_BOT    │  │
│  └──────────────────────────────────────────────────────────┘  │
├────────────────────────────────────────────────────────────────┤
│  Snowpark Python Phase                                         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ FINAL_RISK_SCORES                                        │  │
│  │ - RISK_SCORE (0-1), fraud_flag, country                  │  │
│  └──────────────────────────────────────────────────────────┘  │
├────────────────────────────────────────────────────────────────┤
│  UDF Classification Phase                                      │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ ACTIONABLE_ALERTS                                        │  │
│  │ - FINAL_ACTION: BLOCK_IP_IMMEDIATELY | SEND_TO_REVIEW    │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
| :--- | :--- | :--- |
| **Data Platform** | Snowflake | Unified data + compute platform |
| **SQL Filtering** | Snowflake SQL | High-volume pattern detection |
| **Feature Engineering** | Snowpark (Python) | DataFrame transformations, risk scoring |
| **Classification** | Python UDFs | Native deployment for scalable inference |
| **Dashboard** | Streamlit | Interactive visualization of results |
| **Best Practices** | DataOps | Modular code, CTEs, context management |

---

## 📁 Repository Structure

```
AdTech-Fraud-Detection-Pipeline-Snowflake-PoC/
├── README.md
├── setup_environment.sql          # Database/schema setup
├── SQL Phase_2025-11-22-2213.csv  # SQL filtering output sample
├── feature_engineering.py         # Snowpark feature engineering
├── udf_deployment_action.py       # Python UDF for classification
├── streamlit_code_Dashboard.py    # Streamlit dashboard code
└── dashboard/
    ├── fraud_detection_fixed.py   # Streamlit Cloud-ready dashboard
    └── requirements.txt           # Python dependencies
```

---

## 🚀 How to Run

### Prerequisites
- Snowflake account with appropriate permissions
- Python 3.8+ (for Snowpark and Streamlit)
- Snowpark Python library installed

### Step 1: Setup Environment

```sql
-- Execute setup_environment.sql
CREATE DATABASE IF NOT EXISTS FRAUD_DETECTION_DB;
CREATE SCHEMA IF NOT EXISTS RAW_LOGS;
USE DATABASE FRAUD_DETECTION_DB;
USE SCHEMA RAW_LOGS;
```

### Step 2: SQL Filtering Phase

Execute SQL filtering queries to identify suspect IPs:
```sql
-- High-volume filtering (example)
SELECT ip, COUNT(*) as click_count
FROM BID_LOGS
GROUP BY ip
HAVING COUNT(*) > 1000;

-- High-velocity filtering (example)
SELECT ip, 
       LAG(timestamp) OVER (PARTITION BY ip ORDER BY timestamp) as prev_ts,
       timestamp - LAG(timestamp) OVER (PARTITION BY ip ORDER BY timestamp) as time_diff
FROM BID_LOGS;
```

### Step 3: Snowpark Feature Engineering

```python
# Execute feature_engineering.py in Snowflake Python Worksheet
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F

# Create Snowpark DataFrame and apply transformations
df = session.table('SUSPECT_IPS')
df = df.withColumn('RISK_SCORE', 
    F.when(F.col('HIGH_VELOCITY_BOT') == 1, 0.8)
     .when(F.col('UA_ROTATION_BOT') == 1, 0.6)
     .otherwise(0.2)
)
```

### Step 4: Deploy Python UDF

```python
# Execute udf_deployment_action.py
# Registers determine_action() UDF in Snowflake

@udf
def determine_action(risk_score: float) -> str:
    if risk_score >= 0.7:
        return 'BLOCK_IP_IMMEDIATELY'
    elif risk_score >= 0.4:
        return 'SEND_TO_MANUAL_REVIEW'
    else:
        return 'MONITOR'
```

### Step 5: Run Dashboard (Optional)

```bash
cd dashboard/
pip install -r requirements.txt
streamlit run fraud_detection_fixed.py
```

---

## 📊 Pipeline Results

### Filtering Efficiency

| Stage | Input Volume | Output Volume | Reduction |
| :--- | :--- | :--- | :--- |
| Raw Logs | 10 TB | 10 TB | 0% |
| SQL Phase 1 (Volume) | 10 TB | 500 GB | 95% |
| SQL Phase 2 (Velocity) | 500 GB | 100 GB | 80% |
| Final Suspects | 100 GB | <10 GB | 90% |
| **Total Reduction** | **10 TB** | **<10 GB** | **99.9%+** |

### Classification Output

| Action | Count | Percentage |
| :--- | :--- | :--- |
| BLOCK_IP_IMMEDIATELY | ~60% | High-confidence fraud |
| SEND_TO_MANUAL_REVIEW | ~30% | Requires human analysis |
| MONITOR | ~10% | Low risk, continue tracking |

---

## 🔮 Future Enhancements

- [ ] Real-time streaming ingestion (Snowpipe)
- [ ] ML model deployment (Snowflake ML)
- [ ] Automated alert system (email/Slack)
- [ ] Historical trend analysis
- [ ] Integration with ad platform APIs for automatic blocking

---

## 📝 Spanish Summary (Resumen en Español)

**AdTech Fraud Detection Pipeline** es un proof of concept que demuestra cómo procesar 10TB+ de logs de publicidad digital para detectar fraude. Utiliza un enfoque de "embudo de filtrado" en múltiples etapas: primero SQL para filtrado masivo (reduciendo 99%+ de los datos), luego Snowpark Python para ingeniería de features y scoring de riesgo, y finalmente UDFs de Python desplegadas nativamente en Snowflake para clasificación (BLOQUEAR vs REVISAR MANUAL). El dashboard de Streamlit proporciona visualización interactiva de resultados. Ideal para equipos de AdTech que necesitan escalar detección de fraude con eficiencia de costos.

---

## 📫 Author

**Nicolas Zalazar** | Data Engineer & Microsoft Fabric Specialist  
📧 zalazarn046@gmail.com | 🔗 [LinkedIn](https://www.linkedin.com/in/nicolas-zalazar-63340923a) | 🐙 [GitHub](https://github.com/Nicolenki7)

---

## 📄 License

MIT License — Feel free to fork, modify, and use for personal or commercial projects.

---

*Last Updated: February 2026*
