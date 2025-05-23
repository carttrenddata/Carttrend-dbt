WITH canal as(
    SELECT canal_social
    FROM {{source("carttrend_brut",'Carttrend_Posts')}}
    UNION DISTINCT
    SELECT canal
    FROM {{source('carttrend_brut', 'Carttrend_Campaigns')}}
)
SELECT
    ROW_NUMBER() OVER() as id_canal,
    canal_social as canal
FROM canal
ORDER BY canal ASC
