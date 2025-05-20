select 
    p.id_promotion as id_promotion, 
    p.id_produit as id_produit, 
    p.type_promotion as type_promotion,
    case 
        when p.type_promotion = 'Pourcentage' then round(pr.prix * p.valeur_promotion,2)
        else p.valeur_promotion
    end as valeur_promotion, 
    p.date_d__but as date_debut, 
    p.date_fin as date_fin, 
    p.responsable_promotion as responsable_promotion
from {{source('carttrend_brut','Carttrend_Promotions')}} p
left join {{ref('Produits')}} pr
on p.id_produit = pr.id_produit
