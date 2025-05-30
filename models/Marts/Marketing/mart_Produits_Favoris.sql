SELECT
    DISTINCT(id_produit) AS id_produit,
    (
        SELECT COUNT(id_produit)
        FROM {{ref('Produits_Favoris')}} 
        WHERE id_produit = pf.id_produit
    ) AS nb_clients,
    (
        SELECT COUNT(id_produit)
        FROM {{ref('mart_Commandes')}} co
        WHERE co.id_produit = pf.id_produit AND co.id_client = pf.id_client
    ) AS nb_fois_achetes
FROM {{ref('Produits_Favoris')}} pf
    JOIN {{ref('Produits')}} p USING(id_produit)