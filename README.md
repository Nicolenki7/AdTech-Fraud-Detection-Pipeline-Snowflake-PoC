# 🎯 AdTech Fraud Detection Pipeline — Snowflake PoC

**10TB Simulated Log Processing | SQL Filtering | Snowpark Python UDFs | Real-Time Classification**

[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake)](https://www.snowflake.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?logo=python)](https://www.python.org/)
[![SQL](https://img.shields.io/badge/SQL-003B57?logo=postgresql)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 📋 Overview

End-to-end fraud detection workflow demonstrating massive-scale data filtering and real-time classification. Processes simulated 10TB AdTech log data through multi-stage SQL filtering and Snowpark Python UDFs for fraud classification.

This project showcases enterprise-scale data engineering patterns for ad fraud detection with heuristic filtering and ML-based classification.

---

## 💼 Business Impact

- **Data Reduction**: 10TB raw logs → <1% actionable suspects through heuristic filtering
- **Cost Savings**: Reduced storage and compute costs by filtering early in pipeline
- **Real-Time Detection**: Native Snowflake UDFs enable in-database classification
- **Actionable Intelligence**: Filtered dataset ready for investigation workflows

---

## 🛠️ Technical Stack

| Category | Technologies |
| :--- | :--- |
| **Data Platform** | Snowflake Data Cloud |
| **Data Processing** | Snowflake SQL, Window Functions |
| **Machine Learning** | Snowpark Python, UDFs |
| **Data Engineering** | Multi-stage filtering, feature engineering |
| **Deployment** | Native Snowflake deployment |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              ADTECH FRAUD DETECTION PIPELINE                 │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  STAGE 1: RAW DATA INGESTION                                │
│  └─→ 10TB simulated AdTech logs                             │
│      - Click events, impressions, conversions               │
│                                                              │
│  STAGE 2: SQL FILTERING (Multi-Stage)                       │
│  └─→ Heuristic filtering with window functions              │
│      - HAVING clauses for aggregation filters               │
│      - Window functions for pattern detection               │
│      - Result: ~1% of original data                         │
│                                                              │
│  STAGE 3: FEATURE ENGINEERING (Snowpark)                    │
│  └─→ Snowpark DataFrames for feature creation               │
│      - User behavior patterns                               │
│      - Temporal anomalies                                   │
│      - Device/IP correlations                               │
│                                                              │
│  STAGE 4: CLASSIFICATION (Python UDFs)                      │
│  └─→ Native Snowflake Python UDFs                           │
│      - Fraud probability scoring                            │
│      - Risk categorization                                  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Key Features

### Multi-Stage SQL Filtering
- **Window Functions**: Pattern detection across user sessions
- **HAVING Clauses**: Aggregate-level fraud indicators
- **Progressive Filtering**: Reduce data volume at each stage

### Snowpark Feature Engineering
- **User Behavior Patterns**: Click frequency, session duration
- **Temporal Anomalies**: Unusual time-based patterns
- **Device/IP Correlations**: Suspicious device clustering

### Python UDF Classification
- **Native Deployment**: UDFs run inside Snowflake
- **Fraud Scoring**: Probability-based risk assessment
- **Risk Categories**: Low/Medium/High fraud likelihood

---

## 📊 Results & Metrics

| Metric | Value |
| :--- | :--- |
| **Input Data Volume** | 10TB simulated logs |
| **Output Data Volume** | <1% of original (actionable suspects) |
| **Filtering Stages** | 3+ SQL filtering passes |
| **Classification** | Python UDFs (native Snowflake) |

---

## 📁 Project Structure

```
AdTech-Fraud-Detection-Pipeline-Snowflake-PoC/
├── src/                               # Python source code
│   ├── filtering/                     # SQL filtering scripts
│   ├── features/                      # Feature engineering
│   └── udf/                           # Python UDFs
├── dashboard/                         # Visualization components
├── docs/                              # Documentation
├── config.py.example                  # Configuration template
├── requirements.txt                   # Python dependencies
└── README.md                          # Project documentation
```

---

## 🔧 Setup & Installation

### Prerequisites
- Snowflake account with appropriate permissions
- Python 3.8+ with snowflake-connector, snowpark
- Snowpark Python enabled in Snowflake account

### Deployment Steps

```bash
# Clone the repository
git clone https://github.com/Nicolenki7/AdTech-Fraud-Detection-Pipeline-Snowflake-PoC.git
cd AdTech-Fraud-Detection-Pipeline-Snowflake-PoC

# Install dependencies
pip install -r requirements.txt

# Configure Snowflake credentials
cp config.py.example config.py
# Edit config.py with your Snowflake credentials

# Deploy SQL filtering scripts
snowsql -f src/filtering/stage1.sql
snowsql -f src/filtering/stage2.sql

# Deploy Python UDFs
python src/udf/deploy_udf.py
```

---

## 📈 Usage

### Pipeline Execution

```sql
-- Stage 1: Initial filtering
CALL filter_stage1('raw_logs', 'filtered_stage1');

-- Stage 2: Pattern detection
CALL filter_stage2('filtered_stage1', 'filtered_stage2');

-- Stage 3: Feature engineering
CALL create_features('filtered_stage2', 'enriched_data');

-- Stage 4: Fraud classification
CALL classify_fraud('enriched_data', 'fraud_results');
```

### Results Analysis

| Output Table | Content |
| :--- | :--- |
| `fraud_results` | Classified records with fraud scores |
| `suspect_summary` | Aggregated fraud statistics |
| `investigation_queue` | High-risk records for review |

---

## 🎯 Key Learnings

- **Early filtering** dramatically reduces compute costs at scale
- **Window functions** excel at detecting temporal fraud patterns
- **Snowpark UDFs** enable Python ML inside Snowflake without data movement
- **Progressive refinement** (10TB → 1%) makes investigation feasible

---

## 🔮 Future Enhancements

- [ ] Real-time streaming ingestion (Snowpipe)
- [ ] Advanced ML models (XGBoost, Neural Networks)
- [ ] Automated model retraining pipeline
- [ ] Integration with investigation workflow tools
- [ ] A/B testing framework for filter thresholds

---

## 🔗 Links

| Resource | URL |
| :--- | :--- |
| **Repository** | https://github.com/Nicolenki7/AdTech-Fraud-Detection-Pipeline-Snowflake-PoC |

---

## 📝 Resumen en Español

Pipeline end-to-end de detección de fraude AdTech procesando 10TB de logs simulados a través de filtrado SQL multi-etapa y UDFs Python de Snowpark para clasificación de fraude.

El filtrado heurístico reduce los datos a <1% de sospechosos accionables. Las UDFs Python nativas de Snowflake permiten clasificación in-database sin movimiento de datos.

---

## 📄 License

MIT License — Feel free to fork, modify, and use for personal or commercial projects.

---

## 👤 Author

**Nicolás Zalazar** | Senior Data Engineer & Microsoft Fabric Specialist

- GitHub: [@Nicolenki7](https://github.com/Nicolenki7)
- LinkedIn: [nicolas-zalazar-63340923a](https://www.linkedin.com/in/nicolas-zalazar-63340923a)
- Portfolio: [nicolenki7.github.io/Portfolio](https://nicolenki7.github.io/Portfolio/)
- Email: zalazarn046@gmail.com

---

*Last Updated: March 2026*
