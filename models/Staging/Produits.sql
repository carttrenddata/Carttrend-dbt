select 
    ID as id_produit,
    CASE 
        WHEN Cat__gorie IN ('Livres','Alimentation','Mode','Maison','Électronique','Jouets','Beauté','Sports') THEN Cat__gorie
        ELSE 'Autre'
    END as categorie,
    initcap(Marque) as marque,
    initcap(Produit) as nom,
    ROUND(CASE
        WHEN Prix < 0 Then 0
        ELSE Prix
    END, 2) as prix,
    Sous_cat__gorie as sous_categorie,
    Variation as variation
from 
    {{source("carttrend_brut", "Carttrend_Produits")}}


