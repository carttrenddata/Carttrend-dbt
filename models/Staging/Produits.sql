select 
    ID as id_produit,
    CASE 
        WHEN Categorie IN ('Livres','Alimentation','Mode','Maison','Électronique','Jouets','Beauté','Sports') THEN Categorie
        ELSE 'Autre'
    END as categorie,
    Sous_categorie as sous_categorie,
    initcap(Marque) as marque,
    CASE 
        WHEN produit LIKE CONCAT('%', variation, '%') OR variation IS NULL THEN produit
        ELSE CONCAT(produit, variation)
    END AS nom,
    ROUND(CASE
        WHEN Prix < 0 Then 0
        ELSE Prix
    END, 2) as prix,
from 
    {{source("google_drive", "carttrend_produits_produits")}}


