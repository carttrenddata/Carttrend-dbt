SELECT 
    c.date_commande AS date,
    e.id_entrepot,
    e.localisation,
    COUNT(c.id_commande) AS nb_commandes_traitees,
    CAST(AVG(DATE_DIFF(c.date_livraison_estimee, c.date_commande, DAY)) AS INT64) AS moyenne_delai_livraison_et_traitement
FROM {{ref('Entrepots')}} e
    JOIN {{ref('Commandes')}} c USING(id_entrepot)
GROUP BY c.date_commande, e.id_entrepot, e.localisation, e.temperature