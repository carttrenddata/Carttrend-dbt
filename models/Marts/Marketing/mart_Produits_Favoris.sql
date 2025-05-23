SELECT
    DISTINCT(id_produit) AS id_produit,
    p.nom,
    p.categorie,
    p.sous_categorie,
    p.variation,
    p.marque,
    (
        SELECT COUNT(id_produit)
        FROM {{ref('Produits_Favoris')}} 
        WHERE id_produit = pf.id_produit
    ) AS nb_clients
FROM {{ref('Produits_Favoris')}} pf
    JOIN {{ref('Produits')}} p USING(id_produit)