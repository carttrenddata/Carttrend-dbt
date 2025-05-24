SELECT
    id_campagne,
    date,
    c.id_canal as id_canal,
    CAST(
        CASE 
            WHEN budget < 0 THEN 0
            ELSE budget
        END
    AS FLOAT64 ) as budget,
    CASE 
        WHEN impressions < 0 THEN 0
        ELSE impressions
    END as impressions,
    CASE 
        WHEN clics < 0 THEN 0
        ELSE clics
    END as clics,
    CASE 
        WHEN conversions < 0 THEN 0
        ELSE conversions
    END as conversions,
    CASE
        WHEN CTR > 0 AND CTR < 1 THEN CTR
        ELSE NULL
    END as ctr
from {{source('carttrend_brut','Carttrend_Campaigns')}}
    join {{ref('Canaux')}} c using(canal)
WHERE REGEXP_CONTAINS(id_campagne, r'^CAMP\d{3}$')
