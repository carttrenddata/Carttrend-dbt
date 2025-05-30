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
      SUM(CASE WHEN sentiment_global = 'Positif' THEN 1 ELSE 0 END) AS sentiment_positif,
      SUM(CASE WHEN sentiment_global = 'Neutre' THEN 1 ELSE 0 END) AS sentiment_neutre,
      SUM(CASE WHEN sentiment_global = 'Négatif' THEN 1 ELSE 0 END) AS sentiment_negatif,
      SUM(CASE WHEN sentiment_global IS NULL THEN 1 ELSE 0 END) AS sentiment_absent
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
  COALESCE(p.sentiment_positif, 0) AS sentiment_positif,
  COALESCE(p.sentiment_neutre, 0) AS sentiment_neutre,
  COALESCE(p.sentiment_negatif, 0) AS sentiment_negatif,
  COALESCE(p.sentiment_absent, 0) AS sentiment_absent
FROM date_range d
CROSS JOIN canals can
LEFT JOIN campagnes_stats c ON c.date = d.day AND c.id_canal = can.id_canal
LEFT JOIN posts_stats p ON p.date = d.day AND p.id_canal = can.id_canal
ORDER BY date, can.canal

