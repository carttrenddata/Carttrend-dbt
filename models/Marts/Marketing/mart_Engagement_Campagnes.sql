 /* WITH posts_stats AS (
    SELECT
        date_post as date,
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
    CASE
        id_cam,
    COALESCE(c.date, ps.date) AS date,
    COALESCE(can.canal, can2.canal) AS canal,
    ps.volume_mentions,
    ps.sentiment,
    c.budget,
    c.impressions,
    c.clics,
    c.conversions,
    c.ctr
FROM {{ref('Campagnes')}} c
    FULL JOIN posts_stats ps on c.date = ps.date AND c.id_canal = ps.id_canal
    LEFT JOIN {{ref('Canaux')}} can on can.id_canal = c.id_canal
    LEFT JOIN {{ref('Canaux')}} can2 on can2.id_canal = ps.id_canal */


WITH 
  date_range AS (
    SELECT day
    FROM UNNEST(GENERATE_DATE_ARRAY(
      (SELECT MIN(date_post) FROM {{ref('Posts')}}),
      (SELECT MAX(date) FROM {{ref('Campagnes')}})
    )) AS day
  ),
  
  canals AS (
    SELECT id_canal, canal FROM {{ref('Canaux')}}
  ),
  
  posts_stats AS (
    SELECT
      TRUE as is_post,
      date_post AS date,
      id_canal,
      SUM(volume_mentions) AS volume_mentions,
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
  ),
  
  campagnes_stats AS (
    SELECT
      TRUE as is_campagne,
      date,
      id_canal,
      SUM(budget) AS budget,
      SUM(impressions) AS impressions,
      SUM(clics) AS clics,
      SUM(conversions) AS conversions,
      AVG(ctr) as ctr
    FROM {{ref('Campagnes')}}
    GROUP BY date, id_canal
  )

SELECT 
  CASE 
    WHEN c.is_campagne IS NOT NULL AND p.is_post IS NULL THEN 'Campagne'
    WHEN p.is_post IS NOT NULL AND c.is_campagne IS NULL THEN 'Post'
    WHEN c.is_campagne IS NOT NULL AND p.is_post IS NOT NULL THEN 'Campagne et Post'
    ELSE 'Aucun'
  END AS source_type,
  d.day AS date,
  can.canal,
  COALESCE(c.budget, 0) AS budget,
  COALESCE(c.impressions, 0) AS impressions,
  COALESCE(c.clics, 0) AS clics,
  COALESCE(c.conversions, 0) AS conversions,
  COALESCE(c.ctr, 0) AS ctr,
  COALESCE(p.volume_mentions, 0) AS volume_mentions,
  COALESCE(p.sentiment, 0) AS sentiment
FROM date_range d
CROSS JOIN canals can
LEFT JOIN campagnes_stats c ON c.date = d.day AND c.id_canal = can.id_canal
LEFT JOIN posts_stats p ON p.date = d.day AND p.id_canal = can.id_canal
ORDER BY date, can.canal
