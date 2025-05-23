SELECT
    id_satisfaction,
    id_commande,
    CASE
        WHEN note_client > 5 THEN 5
        WHEN note_client < 1 THEN 1
        ELSE note_client
    END AS note_client,

    CASE
        WHEN temps_r__ponse_support < 0 THEN 0
        ELSE temps_r__ponse_support
    END AS delai_traitement,

    commentaire,
    plainte,
    type_plainte, 
    employ___support AS employe_support

FROM {{source("carttrend_brut",'Carttrend_Satisfaction')}}
