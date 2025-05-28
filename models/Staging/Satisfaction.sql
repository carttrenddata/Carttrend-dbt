SELECT
    id_satisfaction,
    id_commande,
    CASE
        WHEN note_client > 5 THEN 5
        WHEN note_client < 1 THEN 1
        ELSE note_client
    END AS note_client,

    CASE
        WHEN temps_reponse_support < 0 THEN 0
        ELSE temps_reponse_support
    END AS delai_traitement,

    commentaire,
    plainte,
    type_plainte, 
    employe_support

FROM {{source("google_drive",'carttrend_satisfaction_satisfaction')}}
