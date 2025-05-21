SELECT
    id_commande,
    id_produit,
    quantit__ as quantite,
cast(
    CASE
    WHEN emballage_sp__cial = 'Oui' THEN TRUE
    WHEN emballage_sp__cial = 'Non' THEN FALSE
    ELSE FALSE
END AS BOOL) as emballage_special
FROM {{source("carttrend_brut",'Carttrend_Details_Commandes')}}
