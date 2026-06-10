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
    prescriber_address_line1,
    prescriber_address_line2,
    prescriber_city,
    prescriber_state,
    prescriber_state_FIPS_code,
    prescriber_zip_code,
    prescriber_rural_urban_commuting_area_code,
    prescriber_rural_urban_commuting_area_desc,
    prescriber_country
from
    {{ ref('part_d_silver') }}
