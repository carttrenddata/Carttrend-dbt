select 
    ID as id_produit,
    Cat__gorie as categorie,
    Marque as marque,
    Produit as nom,
    Prix as prix,
    Sous_cat__gorie as sous_categorie,
    Variation as variation
from 
    {{source("carttrend_brut", "Carttrend_Produits")}}