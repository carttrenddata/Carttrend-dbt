WITH posts_stats AS (
    SELECT
        date_post,
        id_canal,
        SUM(volume_mentions) as volume_mentions,
        AVG(
            CASE
                WHEN sentiment_global = 'Positif' THEN 3 
                WHEN sentiment_global = 'Neutre' THEN 2
                WHEN sentiment_global = 'Negatif' THEN 1
                ELSE 2
            END
        ) AS sentiment
    FROM {{ref('Posts')}}
    GROUP BY date_post, id_canal
)
SELECT 
    c.id_campagne,
    c.date,
    can.canal,
    ps.volume_mentions,
    ps.sentiment,
    c.budget,
    c.impressions,
    c.clics,
    c.conversions,
    c.ctr
FROM {{ref('Campagnes')}} c
    JOIN {{ref('Canaux')}} can on can.id_canal = c.id_canal
    LEFT JOIN posts_stats ps on c.date = ps.date_post AND c.id_canal = ps.id_canal
