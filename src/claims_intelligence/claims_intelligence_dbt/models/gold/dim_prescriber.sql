{{
    config(
        materialized='table',
        meta={"dagster": {"group": "gold"}},
    )
}}

select
    {{  dbt_utils.generate_surrogate_key(['data_year', 'prescriber_npi']) }} as prescriber_sk,
    data_year,
    prescriber_npi,
    prescriber_last_name,
    prescriber_first_name,
    prescriber_middle_initial,
    prescriber_credentials,
    prescriber_entity_code,
    prescriber_specialty_type,
    prescriber_specialty_type_source
from
    {{ ref('part_d_silver') }}
