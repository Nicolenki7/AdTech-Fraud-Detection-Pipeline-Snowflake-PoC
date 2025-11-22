# 🛡️ AdTech Fraud Detection Pipeline: Snowflake Proof of Concept (PoC)

## Overview

This repository contains a complete, end-to-end simulation of a massive-scale **Ad Fraud detection workflow**. The goal of this Proof of Concept (PoC) is to demonstrate the ability to quickly filter and analyze multi-terabyte log data using **Snowflake's unified platform** (SQL for massive filtering and Snowpark Python for advanced logic deployment).

The pipeline successfully implements heuristic filtering to reduce a simulated 10 TB log dataset down to a small, actionable list of suspect IPs, classifying them with a final action (BLOCK/REVIEW) via a deployed User-Defined Function (UDF).

## 🚀 Key Technologies & Concepts

| Area | Technology | Concept Demonstrated |
| :--- | :--- | :--- |
| **Data Platform** | **Snowflake** | Separation of Compute/Storage, Cost Optimization (X-SMALL WH). |
| **Massive Filtering** | **SQL** | High-Volume (`COUNT/HAVING`) and High-Velocity (`LAG/Window Functions`) heuristics. |
| **Advanced Logic** | **Snowpark (Python)** | Lazy Evaluation, Feature Engineering (`withColumn`, `when/otherwise`). |
| **Deployment** | **UDFs (User-Defined Functions)** | Deploying complex Python business logic (classification/scoring) to run natively and at scale within the Snowflake engine. |
| **Best Practices** | DataOps | Context setting (`USE DATABASE`), Modular code (CTEs, separate files). |

## 🏗️ Pipeline Flow (The Filtering Funnel)

The entire process is structured around a "Filtering Funnel" to ensure cost efficiency and speed, processing only the necessary volume at each stage.

1.  **Ingestion/Simulation:** Raw transactional logs (`BID_LOGS`) are ingested into the `RAW_LOGS` schema.
2.  **SQL Massive Filtering (Steps 2 & 3):**
    * **Goal:** Reduce $10 \text{TB}$ of logs to $<1\%$ suspects.
    * **Heuristics:** Identified IPs exhibiting rapid-fire click patterns (low time between events) and high volume/UA rotation.
    * **Output:** `SUSPECT_IPS` table.
3.  **Snowpark Feature Engineering (Step 4):**
    * A Snowpark DataFrame is created, applying a preliminary `RISK_SCORE` based on the heuristic flag (`HIGH_VELOCITY_BOT` vs. `UA_ROTATION_BOT`).
    * **Output:** `FINAL_RISK_SCORES` table.
4.  **UDF Deployment (Step 5):**
    * A Python function (`determine_action`) is registered as a **Snowflake UDF**.
    * This UDF is applied to the `RISK_SCORE` column, classifying the alert into final business actions (`BLOCK_IP_IMMEDIATELY`, `SEND_TO_MANUAL_REVIEW`).
    * **Final Output:** `ACTIONABLE_ALERTS` table.

## 📁 Repository Structure

The code is organized into sequential files to clearly illustrate the progression from SQL filtering to Python logic deployment.
