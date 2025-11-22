import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, udf
from snowflake.snowpark.types import StringType, FloatType

def main(session: snowpark.Session):
    # 1. SET CONTEXT (CRITICAL FIX)
    # This tells Snowpark where to create the temporary stage for the UDF
    session.use_database("FRAUD_DETECTION_DB")
    session.use_schema("RAW_LOGS")

    # 2. LOAD DATA
    # Now we can just use the table name since we set the schema above
    df_scores = session.table("FINAL_RISK_SCORES")
    
    # 3. DEFINE PYTHON LOGIC (The "Brain")
    def determine_action(score: float) -> str:
        if score is None:
            return "ERROR_MISSING_SCORE"
        if score >= 0.90:
            return "BLOCK_IP_IMMEDIATELY"
        elif score >= 0.70:
            return "SEND_TO_MANUAL_REVIEW"
        else:
            return "MONITOR_PASSIVELY"

    # 4. REGISTER UDF (The "Bridge")
    # Now that the session has a database, this will work!
    decision_udf = udf(
        func=determine_action, 
        return_type=StringType(), 
        input_types=[FloatType()], 
        name="DECISION_LOGIC_UDF", 
        replace=True,
        is_permanent=False # Explicitly make it a temporary function for this session
    )
    
    # 5. APPLY UDF (The Execution)
    df_final = df_scores.withColumn("FINAL_ACTION", decision_udf(col("RISK_SCORE")))
    
    # 6. SAVE RESULTS
    df_final.write.mode("overwrite").save_as_table("ACTIONABLE_ALERTS")
    
    # 7. Return the final table to view in the UI
    return df_final
