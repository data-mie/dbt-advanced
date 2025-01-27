{{
    config(
        materialized='table'
    )
}}

select
    1 as id,
    'blue' as color
