WITH canal as(
    SELECT canal_social
    FROM {{source("google_drive",'carttrend_posts_posts')}}
    UNION DISTINCT --combine les données en éliminant les doublons
    SELECT canal
    FROM {{source('google_drive', 'carttrend_campaigns_campagnes')}}
)                   -- déclarer une CTE (common table expression, une vue temporaire) appelée canal
SELECT
    ROW_NUMBER() OVER() as id_canal, -- attribuer un numéro unique à chaque ligne
    canal_social as canal
FROM canal
ORDER BY canal ASC
