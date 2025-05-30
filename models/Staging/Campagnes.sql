SELECT
    id_campagne,
    date,
    c.id_canal as id_canal,
    CAST(
        CASE 
            WHEN budget < 0 THEN 0
            ELSE budget
        END
    AS FLOAT64 ) as budget, -- transformer budget en float64, tous les données budget doivent >=0
    CASE 
        WHEN impressions < 0 THEN 0
        ELSE impressions
    END as impressions, -- tous les données impressions doivent >=0
    CASE 
        WHEN clics < 0 THEN 0
        ELSE clics
    END as clics, -- tous les données clics doivent >=0
    CASE 
        WHEN conversions < 0 THEN 0
        ELSE conversions
    END as conversions, -- tous les données conversions doivent >=0
    CASE
        WHEN CTR > 0 AND CTR < 1 THEN CTR
        ELSE NULL
    END as ctr  -- tous les données ctr doivent entre 0 et 1, sinon transformées en null
from {{source('google_drive','carttrend_campaigns_campagnes')}}
    join {{ref('Canaux')}} c using(canal)
