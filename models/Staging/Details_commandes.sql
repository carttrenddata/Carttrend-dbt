SELECT
    id_commande,
    id_produit,
    quantite as quantite,
cast(
    CASE
    WHEN emballage_special = 'Oui' THEN TRUE
    WHEN emballage_special = 'Non' THEN FALSE
    ELSE FALSE
END AS BOOL) as emballage_special
FROM {{source("google_drive",'carttrend_details_commandes_details_commandes_')}}
