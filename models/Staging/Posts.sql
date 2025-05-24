SELECT 
    id_post, 
    id_canal, 
    CASE 
        WHEN volume_mentions < 0 THEN 0 
        ELSE volume_mentions 
    END AS volume_mentions,
    CASE 
        WHEN sentiment_global IN ('Positif', 'Négatif', 'Neutre') THEN sentiment_global
        ELSE NULL 
    END AS sentiment_global,
    CASE 
        WHEN  date_post <= CURRENT_DATE() THEN date_post
        ELSE CURRENT_DATE()
    END AS date_post,
    contenu_post

FROM {{source("carttrend_brut",'Carttrend_Posts')}} p
JOIN {{ref('Canaux')}} c ON c.canal = p.canal_social