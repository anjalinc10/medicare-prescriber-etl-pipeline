import logging
from pyspark.sql.functions import upper, lit, regexp_extract, col, concat_ws, count, isnan, when, avg, round, coalesce
from pyspark.sql.window import Window

# Load custom logger
logger = logging.getLogger(__name__)


def clean_city_data(df_city):
    # Clean df_city DataFrame:
    #   1 Select only required columns
    #   2 Convert city, state and county fields to upper case
    try:
        logger.info(f"Started executing clean_city_data function..")
        df_city_clean = df_city.select(upper(df_city.city).alias("city"),
                                       df_city.state_id,
                                       upper(df_city.state_name).alias("state_name"),
                                       upper(df_city.county_name).alias("county_name"),
                                       df_city.population,
                                       df_city.zips)
    except Exception as exp:
        logger.error("Error in the method - clean_city_data(). Please check the Stack Trace. ", exc_info=True)
        raise
    else:
        logger.info("Finished executing clean_city_data function..\n\n")
    return df_city_clean


def clean_prescriber_data(df_prescriber):
    try:
        # Clean df_fact DataFrame:
        #   1. Select only required columns
        #   2. Rename the columns
        logger.info(f"Started executing clean_prescriber_data function ..")
        df_prescriber_clean = df_prescriber.select(df_prescriber.npi.alias("presc_id"),
                                                   df_prescriber.nppes_provider_last_org_name.alias("presc_lname"),
                                                   df_prescriber.nppes_provider_first_name.alias("presc_fname"),
                                                   df_prescriber.nppes_provider_city.alias("presc_city"),
                                                   df_prescriber.nppes_provider_state.alias("presc_state"),
                                                   df_prescriber.specialty_description.alias("presc_spclt"),
                                                   df_prescriber.years_of_exp,
                                                   df_prescriber.drug_name,
                                                   df_prescriber.total_claim_count.alias("trx_cnt"),
                                                   df_prescriber.total_day_supply,
                                                   df_prescriber.total_drug_cost)
        #   3. Add a Country Field 'USA'
        df_prescriber_clean = df_prescriber_clean.withColumn("country_name", lit("USA"))

        #   4. Clean years_of_exp field
        pattern = '\d+'
        idx = 0
        df_prescriber_clean = df_prescriber_clean.withColumn("years_of_exp", regexp_extract(col("years_of_exp"), pattern, idx))

        #   5. Convert the yearS_of_exp datatype from string to Number
        df_prescriber_clean = df_prescriber_clean.withColumn("years_of_exp", col("years_of_exp").cast("int"))

        #   6. Combine First Name and Last Name
        df_prescriber_clean = df_prescriber_clean.withColumn("presc_fullname", concat_ws(" ", "presc_fname", "presc_lname"))
        df_prescriber_clean = df_prescriber_clean.drop("presc_fname", "presc_lname")

        #   7. Check all the Null/NaN values
        # df_prescriber_clean.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in df_prescriber_clean.columns]).show()

        #   8. Delete the records where the PRESC_ID is NULL
        df_prescriber_clean = df_prescriber_clean.dropna(subset="presc_id")

        #   9. Delete the records where the DRUG_NAME is NULL
        df_prescriber_clean = df_prescriber_clean.dropna(subset="drug_name")

        #   10. Impute TRX_CNT where it is null as avg of trx_cnt for that prescriber
        spec = Window.partitionBy("presc_id")
        df_prescriber_clean = df_prescriber_clean.withColumn('trx_cnt', coalesce("trx_cnt", round(avg("trx_cnt").over(spec))))
        df_prescriber_clean = df_prescriber_clean.withColumn("trx_cnt", col("trx_cnt").cast('integer'))
        #   Validate that Null/NaN values are removed
        # df_prescriber_clean.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_prescriber_clean.columns]).show()
    except Exception as exp:
        logger.error("Error in the method - clean_prescriber_data(). Please check the Stack Trace. ", exc_info=True)
        raise
    else:
        logger.info("Finished executing clean_prescriber_data function..\n\n")
    return df_prescriber_clean
