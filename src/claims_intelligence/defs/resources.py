from dagster_gcp import BigQueryResource, GCSResource

from dagster import Definitions, EnvVar

defs = Definitions(
    assets=[],
    resources={
        "bigquery": BigQueryResource(
            project=EnvVar("GCP_PROJECT")),
        "gcs": GCSResource(project=EnvVar("GCP_PROJECT")),
    },
)