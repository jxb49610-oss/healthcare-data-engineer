{{ config(materialized='table') }}

SELECT DISTINCT
    doctor,
    room,
    diagnosa
FROM {{ ref('stg_healthcare') }}

