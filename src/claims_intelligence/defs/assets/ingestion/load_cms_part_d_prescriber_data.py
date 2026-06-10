import hashlib
import httpx

from dagster import asset, StaticPartitionsDefinition, EnvVar
from dagster_gcp import GCSResource, BigQueryResource
from google.cloud.bigquery import SchemaField, Table, RangePartitioning, PartitionRange, LoadJobConfig, SourceFormat, WriteDisposition

# Set up the dataset ids that we will use to download the files
medicare_part_d_prescriber_datasets = {
    2024: "14d8e8a9-7e9b-4370-a044-bf97c46b4b44",
    2023: "6428fad7-4fb1-454c-93a4-772934d73922",
    2022: "bed99012-c527-4d9d-92ea-67ec2510abea",
    2021: "3f7ab9ce-6fb6-4e6b-9af3-b681f2d3a95e",
    2020: "ed6548c1-c905-4fdf-84cc-1c897ca6210d",
    2019: "007f61da-2c20-4c80-90a5-88be67c8d022",
    2018: "a422bf49-e5b3-429a-862f-723db5e15704",
    2017: "dc524dbb-6115-48c9-a4dc-aa4fb0d21b69",
    2016: "9c3eb417-a310-42d2-8065-9e2129e6a680",
    2015: "2aa87eb6-6555-42fa-a51d-4a332ab7d5e8",
    2014: "8c3806d2-0692-4564-ab38-7bc19206521c",
    2013: "93645ea4-1a3f-4444-95c5-c109e6a4b267",
}

# Partition the asset based on year - one year = one dataset
year_partitions = StaticPartitionsDefinition(
    [str(y) for y in sorted(medicare_part_d_prescriber_datasets.keys())]
)

# Schema for the data
PART_D_STAGING_SCHEMA = [
    SchemaField("PRSCRBR_NPI", "FLOAT64", mode="NULLABLE"),
    SchemaField("Prscrbr_Last_Org_Name", "STRING", mode="NULLABLE"),
    SchemaField("Prscrbr_First_Name", "STRING", mode="NULLABLE"),
    SchemaField("Prscrbr_MI", "STRING", mode="NULLABLE"),
    SchemaField("Prscrbr_Crdntls", "STRING", mode="NULLABLE"),
    SchemaField("Prscrbr_Ent_Cd", "STRING", mode="NULLABLE"),
    SchemaField("Prscrbr_St1", "STRING", mode="NULLABLE"),
    SchemaField("Prscrbr_St2", "STRING", mode="NULLABLE"),
    SchemaField("Prscrbr_City", "STRING", mode="NULLABLE"),
    SchemaField("Prscrbr_State_Abrvtn", "STRING", mode="NULLABLE"),
    SchemaField("Prscrbr_State_FIPS", "STRING", mode="NULLABLE"),
    SchemaField("Prscrbr_zip5", "STRING", mode="NULLABLE"),
    SchemaField("Prscrbr_RUCA", "STRING", mode="NULLABLE"),
    SchemaField("Prscrbr_RUCA_Desc", "STRING", mode="NULLABLE"),
    SchemaField("Prscrbr_Cntry", "STRING", mode="NULLABLE"),
    SchemaField("Prscrbr_Type", "STRING", mode="NULLABLE"),
    SchemaField("Prscrbr_Type_src", "STRING", mode="NULLABLE"),
    SchemaField("Tot_Clms", "FLOAT64", mode="NULLABLE"),
    SchemaField("Tot_30day_Fills", "FLOAT64", mode="NULLABLE"),
    SchemaField("Tot_Drug_Cst", "FLOAT64", mode="NULLABLE"),
    SchemaField("Tot_Day_Suply", "FLOAT64", mode="NULLABLE"),
    SchemaField("Tot_Benes", "FLOAT64", mode="NULLABLE"),
    SchemaField("GE65_Sprsn_Flag", "STRING", mode="NULLABLE"),
    SchemaField("GE65_Tot_Clms", "FLOAT64", mode="NULLABLE"),
    SchemaField("GE65_Tot_30day_Fills", "FLOAT64", mode="NULLABLE"),
    SchemaField("GE65_Tot_Drug_Cst", "FLOAT64", mode="NULLABLE"),
    SchemaField("GE65_Tot_Day_Suply", "FLOAT64", mode="NULLABLE"),
    SchemaField("GE65_Bene_Sprsn_Flag", "STRING", mode="NULLABLE"),
    SchemaField("GE65_Tot_Benes", "FLOAT64", mode="NULLABLE"),
    SchemaField("Brnd_Sprsn_Flag", "STRING", mode="NULLABLE"),
    SchemaField("Brnd_Tot_Clms", "FLOAT64", mode="NULLABLE"),
    SchemaField("Brnd_Tot_Drug_Cst", "FLOAT64", mode="NULLABLE"),
    SchemaField("Gnrc_Sprsn_Flag", "STRING", mode="NULLABLE"),
    SchemaField("Gnrc_Tot_Clms", "FLOAT64", mode="NULLABLE"),
    SchemaField("Gnrc_Tot_Drug_Cst", "FLOAT64", mode="NULLABLE"),
    SchemaField("Othr_Sprsn_Flag", "STRING", mode="NULLABLE"),
    SchemaField("Othr_Tot_Clms", "FLOAT64", mode="NULLABLE"),
    SchemaField("Othr_Tot_Drug_Cst", "FLOAT64", mode="NULLABLE"),
    SchemaField("MAPD_Sprsn_Flag", "STRING", mode="NULLABLE"),
    SchemaField("MAPD_Tot_Clms", "FLOAT64", mode="NULLABLE"),
    SchemaField("MAPD_Tot_Drug_Cst", "FLOAT64", mode="NULLABLE"),
    SchemaField("PDP_Sprsn_Flag", "STRING", mode="NULLABLE"),
    SchemaField("PDP_Tot_Clms", "FLOAT64", mode="NULLABLE"),
    SchemaField("PDP_Tot_Drug_Cst", "FLOAT64", mode="NULLABLE"),
    SchemaField("LIS_Sprsn_Flag", "STRING", mode="NULLABLE"),
    SchemaField("LIS_Tot_Clms", "FLOAT64", mode="NULLABLE"),
    SchemaField("LIS_Drug_Cst", "FLOAT64", mode="NULLABLE"),
    SchemaField("NonLIS_Sprsn_Flag", "STRING", mode="NULLABLE"),
    SchemaField("NonLIS_Tot_Clms", "FLOAT64", mode="NULLABLE"),
    SchemaField("NonLIS_Drug_Cst", "FLOAT64", mode="NULLABLE"),
    SchemaField("Opioid_Tot_Clms", "FLOAT64", mode="NULLABLE"),
    SchemaField("Opioid_Tot_Drug_Cst", "FLOAT64", mode="NULLABLE"),
    SchemaField("Opioid_Tot_Suply", "FLOAT64", mode="NULLABLE"),
    SchemaField("Opioid_Tot_Benes", "FLOAT64", mode="NULLABLE"),
    SchemaField("Opioid_Prscrbr_Rate", "FLOAT64", mode="NULLABLE"),
    SchemaField("Opioid_LA_Tot_Clms", "FLOAT64", mode="NULLABLE"),
    SchemaField("Opioid_LA_Tot_Drug_Cst", "FLOAT64", mode="NULLABLE"),
    SchemaField("Opioid_LA_Tot_Suply", "FLOAT64", mode="NULLABLE"),
    SchemaField("Opioid_LA_Tot_Benes", "FLOAT64", mode="NULLABLE"),
    SchemaField("Opioid_LA_Prscrbr_Rate", "FLOAT64", mode="NULLABLE"),
    SchemaField("Antbtc_Tot_Clms", "FLOAT64", mode="NULLABLE"),
    SchemaField("Antbtc_Tot_Drug_Cst", "FLOAT64", mode="NULLABLE"),
    SchemaField("Antbtc_Tot_Benes", "FLOAT64", mode="NULLABLE"),
    SchemaField("Antpsyct_GE65_Sprsn_Flag", "STRING", mode="NULLABLE"),
    SchemaField("Antpsyct_GE65_Tot_Clms", "FLOAT64", mode="NULLABLE"),
    SchemaField("Antpsyct_GE65_Tot_Drug_Cst", "FLOAT64", mode="NULLABLE"),
    SchemaField("Antpsyct_GE65_Bene_Suprsn_Flag", "STRING", mode="NULLABLE"),
    SchemaField("Antpsyct_GE65_Tot_Benes", "FLOAT64", mode="NULLABLE"),
    SchemaField("Bene_Avg_Age", "FLOAT64", mode="NULLABLE"),
    SchemaField("Bene_Age_LT_65_Cnt", "FLOAT64", mode="NULLABLE"),
    SchemaField("Bene_Age_65_74_Cnt", "FLOAT64", mode="NULLABLE"),
    SchemaField("Bene_Age_75_84_Cnt", "FLOAT64", mode="NULLABLE"),
    SchemaField("Bene_Age_GT_84_Cnt", "FLOAT64", mode="NULLABLE"),
    SchemaField("Bene_Feml_Cnt", "FLOAT64", mode="NULLABLE"),
    SchemaField("Bene_Male_Cnt", "FLOAT64", mode="NULLABLE"),
    SchemaField("Bene_Race_Wht_Cnt", "FLOAT64", mode="NULLABLE"),
    SchemaField("Bene_Race_Black_Cnt", "FLOAT64", mode="NULLABLE"),
    SchemaField("Bene_Race_Api_Cnt", "FLOAT64", mode="NULLABLE"),
    SchemaField("Bene_Race_Hspnc_Cnt", "FLOAT64", mode="NULLABLE"),
    SchemaField("Bene_Race_Natind_Cnt", "FLOAT64", mode="NULLABLE"),
    SchemaField("Bene_Race_Othr_Cnt", "FLOAT64", mode="NULLABLE"),
    SchemaField("Bene_Dual_Cnt", "FLOAT64", mode="NULLABLE"),
    SchemaField("Bene_Ndual_Cnt", "FLOAT64", mode="NULLABLE"),
    SchemaField("Bene_Avg_Risk_Scre", "FLOAT64", mode="NULLABLE"),
]


@asset(partitions_def=year_partitions, group_name="ingestion")
def load_cms_part_d_prescriber_data(context, gcs: GCSResource, bigquery: BigQueryResource):
    """
    """

    # Get the Partition Year and the dataset id
    year = int(context.partition_key)
    dataset_id = medicare_part_d_prescriber_datasets[year]

    # Get the metadata for the dataset we are looking for so we know which file we are downloading
    meta_url = (
        f"https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data-viewer?size=0"
    )
    meta_json = httpx.get(meta_url).raise_for_status().json()['meta']

    # Create an asset metadata dictionary to save relevant meta data to dagster
    file_url = "https://data.cms.gov" + meta_json["data_file_url"]
    file_name = meta_json["data_file_name"]
    file_sha1 = meta_json["data_file_meta_data"]["csvFileSHA1"]
    file_size = meta_json["data_file_meta_data"]["csvFileSize"]
    file_row_cnt = meta_json["total_rows"]
    asset_metadata = {
        "file_size_bytes": file_size,
        "file_sha1": file_sha1,
        "total_rows": file_row_cnt,
        "file_url": file_url,
        "file_name": file_name,
    }

    # Google Storage Bucket Config to save CSV into bucket
    google_bucket_name = "cms-prescriber-data"
    destination_blob_name = file_name
    storage_client = gcs.get_client()
    bucket = storage_client.bucket(google_bucket_name)
    blob = bucket.blob(destination_blob_name)

    gcs_uri = f"gs://{google_bucket_name}/{destination_blob_name}"

    if blob.exists():
        context.log.info(f"File {destination_blob_name} already exists in GCS.")
        asset_metadata["skipped_file_download"] = True
        context.add_output_metadata(asset_metadata)
    else:
        context.log.info(f"File {destination_blob_name} does not exist in GCS.")
        hasher = hashlib.sha1()
        bytes_written = 0

        with httpx.stream("GET", file_url) as r:
            r.raise_for_status()
            with blob.open("wb", chunk_size=8 * 1024 * 1024) as f:
                for chunk in r.iter_bytes(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
                    hasher.update(chunk)
                    bytes_written += len(chunk)
                    pct = bytes_written / file_size * 100
                    context.log.info(
                        f"{pct:.1f}% ({bytes_written / 1024 ** 2:.1f} MB)",
                    )

        if file_sha1 != hasher.hexdigest():
            raise ValueError("HASH DOES NOT MATCH!")

        if file_size != bytes_written:
            raise ValueError("SIZE DOES NOT MATCH!")

        asset_metadata["skipped_file_download"] = False

    gcp_project_id = EnvVar("GCP_PROJECT")
    dataset_id = EnvVar("GCP_RAW_DATASET")
    staging_table_name_base = EnvVar("GCP_RAW_STAGING_TABLE_BASE")
    final_raw_table = EnvVar("GCP_RAW_TABLE")

    # Set parameters
    staging_table_id = f"{staging_table_name_base}_{year}"


    with bigquery.get_client() as bq_client:
        final_schema = PART_D_STAGING_SCHEMA + [
            SchemaField("data_year", "INT64", mode="REQUIRED"),
            SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        table = Table(
            f"{gcp_project_id}.{dataset_id}.{final_raw_table}", schema=final_schema
        )
        table.range_partitioning = RangePartitioning(
            field="data_year", range_=PartitionRange(start=2013, end=2030, interval=1)
        )
        bq_client.create_table(table, exists_ok=True)

        job_config = LoadJobConfig(
            schema=PART_D_STAGING_SCHEMA,
            skip_leading_rows=1,
            source_format=SourceFormat.CSV,
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = bq_client.load_table_from_uri(
            gcs_uri,
            f"{gcp_project_id}.{dataset_id}.{staging_table_id}",
            job_config=job_config,
        )

        load_job.result()  # Waits for the job to complete.

        #   4. DELETE FROM final WHERE data_year = year
        delete_query = f"""
           DELETE FROM `{gcp_project_id}`.`{dataset_id}`.`{final_raw_table}` 
           WHERE data_year = {year}
        """

        bq_client.query(delete_query).result()

        #   5. INSERT INTO final SELECT *, year AS data_year FROM staging
        insert_query = f"""
           INSERT INTO `{gcp_project_id}`.`{dataset_id}`.`{final_raw_table}`
           SELECT *, {year} as data_year, CURRENT_TIMESTAMP as loaded_at
           FROM `{gcp_project_id}`.`{dataset_id}`.`{staging_table_id}`
       """

        bq_client.query(insert_query).result()

        #   6. delete staging table
        bq_client.delete_table(f"{gcp_project_id}.{dataset_id}.{staging_table_id}")
