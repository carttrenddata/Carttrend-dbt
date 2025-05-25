select 
    ID as id_produit,
    CASE 
        WHEN Categorie IN ('Livres','Alimentation','Mode','Maison','Électronique','Jouets','Beauté','Sports') THEN Categorie
        ELSE 'Autre'
    END as categorie,
    initcap(Marque) as marque,
    initcap(Produit) as nom,
    ROUND(CASE
        WHEN Prix < 0 Then 0
        ELSE Prix
    END, 2) as prix,
    Sous_categorie as sous_categorie,
    Variation as variation
from 
    {{source("google_drive", "carttrend_produits_produits")}}


