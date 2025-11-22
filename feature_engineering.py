import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, when, lit
from snowflake.snowpark.types import FloatType

def main(session: snowpark.Session):
    # 1. LOAD DATA: Create a reference to the table we created in SQL Phase
    # Note: This does not load data into RAM. It's just a pointer.
    df_suspects = session.table("FRAUD_DETECTION_DB.RAW_LOGS.SUSPECT_IPS")
    
    # 2. FEATURE ENGINEERING: Assign a numeric score based on the 'FRAUD_REASON'
    # We use .withColumn() to create a new feature.
    # This is equivalent to SQL's CASE WHEN.
    df_scored = df_suspects.withColumn(
        "RISK_SCORE", 
        when(col("FRAUD_REASON") == "HIGH_VELOCITY_BOT", lit(0.95))
        .when(col("FRAUD_REASON") == "UA_ROTATION_BOT", lit(0.80))
        .otherwise(lit(0.50))
    )
    
    # 3. PERSISTENCE: Save the final scored table
    # .write.save_as_table() triggers the execution on the Warehouse.
    df_scored.write.mode("overwrite").save_as_table("FRAUD_DETECTION_DB.RAW_LOGS.FINAL_RISK_SCORES")
    
    # 4. RETURN: Display the result to verify
    return df_scored
    
