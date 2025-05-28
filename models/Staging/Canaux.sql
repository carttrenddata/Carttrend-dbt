WITH canal as(
    SELECT canal_social
    FROM {{source("google_drive",'carttrend_posts_posts')}}
    UNION DISTINCT
    SELECT canal
    FROM {{source('google_drive', 'carttrend_campaigns_campagnes')}}
)
SELECT
    ROW_NUMBER() OVER() as id_canal,
    canal_social as canal
FROM canal
ORDER BY canal ASC
