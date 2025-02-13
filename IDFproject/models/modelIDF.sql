WITH event_counts AS (
    SELECT
        craft
        , COUNT(*) AS craft_count
        , MAX(_inserted_at) AS last_inserted_at
    FROM {{ source('idf','PARSED_TABLE') }}
    GROUP BY craft
)
SELECT
    craft,
    craft_count,
    last_inserted_at
FROM event_counts