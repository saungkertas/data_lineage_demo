{{ config(materialized='ephemeral') }}

select * from `id-bi-staging.playground_dev.public_dataset_financial_account`