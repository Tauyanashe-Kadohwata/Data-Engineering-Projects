import dlt
from pyspark.sql.functions import col, to_date

@dlt.table(
  name="processed_games_RAW_DIAGNOSTIC", # Note: A new table name for this test
  comment="Diagnostic step: Reading raw data with no transforms"
)
def processed_games_raw():
    # --- STEP 1: DIAGNOSTIC READ ---
    # We are going to do the most basic read possible.
    # If this table is empty, the source file is empty or the path is wrong.
    df = (
        spark.read
        .option("header", "true")
        .csv("/Volumes/workspace/bronze_layer/game_data/Video Games Data.csv")
    )
    
    return df

@dlt.table(
  name="processed_games",
  comment="Cleaned and typed video game sales data from bronze layer"
)
def processed_games():
    # --- STEP 1: Read the data from our new diagnostic table ---
    df = dlt.read("processed_games_RAW_DIAGNOSTIC")

    # --- STEP 2: Rename columns and cast types ---
    # This code is now reading from the raw table we just defined.
    df = (
        df.drop("img") # Drop the image URL column
          .withColumnRenamed("title", "Game_Title")
          .withColumnRenamed("console", "Console")
          .withColumnRenamed("genre", "Genre")
          .withColumnRenamed("publisher", "Publisher")
          .withColumnRenamed("developer", "Developer")
          
          # Cast numeric columns
          .withColumn("Critic_Score", col("critic_score").cast("float"))
          .withColumn("Total_Sales", col("total_sales").cast("float"))
          .withColumn("Sales_North_America", col("na_sales").cast("float"))
          .withColumn("Sales_Japan", col("jp_sales").cast("float"))
          .withColumnRenamed("pal_sales", "Sales_EU_Africa") # Rename this column
          .withColumn("Sales_EU_Africa", col("Sales_EU_Africa").cast("float")) # Cast it
          .withColumn("Sales_Other", col("other_sales").cast("float"))

          # The format in your CSV is "17-09-2013", which is "dd-MM-yyyy"
          .withColumn("Release_Date", to_date(col("release_date"), "dd-MM-yyyy"))
          .withColumn("Last_Update", to_date(col("last_update"), "dd-MM-yyyy"))
          
          # Select only the final columns we want
          .select(
              "Game_Title", "Console", "Genre", "Publisher", "Developer",
              "Critic_Score", "Total_Sales", "Sales_North_America",
              "Sales_Japan", "Sales_EU_Africa", "Sales_Other",
              "Release_Date", "Last_Update"
          )
    )

    # --- STEP 3: Fill any remaining NULLs ---
    
    # Fill NULLs for string-based columns with "Unknown"
    df = df.fillna("Unknown", subset=["Console", "Genre", "Publisher", "Developer", "Last_Update"])
    
    # Fill NULLs for numeric columns with 0.0
    df = df.fillna(0.0, subset=[
        "Critic_Score", "Total_Sales", "Sales_North_America", 
        "Sales_Japan", "Sales_EU_Africa", "Sales_Other"
    ])

    return df