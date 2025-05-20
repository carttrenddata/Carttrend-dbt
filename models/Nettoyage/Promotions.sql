select 
    p.id_promotion as id_promotion, # Hugues : Regex pour verifier le format 'PROMXXXX'
    p.id_produit as id_produit,
    p.type_promotion as type_promotion, # Hugues : Supprimer la colone? Plus vraiment utile...
    case 
        when p.type_promotion = 'Pourcentage' then round(pr.prix * p.valeur_promotion,2)
        else p.valeur_promotion
    end as valeur_promotion, # Hugues : Ajouter une colone % promotion supplementaire? Sans doute des calculs evités plus tard...
    p.date_d__but as date_debut,
    p.date_fin as date_fin,  # Hugues : Verifier si apres date debut
    p.responsable_promotion as responsable_promotion # Hugues : Supprimer cette colone? Le responsable est toujours 'Responsable Marketing'...
from {{source('carttrend_brut','Carttrend_Promotions')}} p
left join {{ref('Produits')}} pr # Hugues : Right Join plutot non? Comme ca on supprime la ligne si le produit n'existe pas. Sinon ce join ne fait rien je crois...
on p.id_produit = pr.id_produit
