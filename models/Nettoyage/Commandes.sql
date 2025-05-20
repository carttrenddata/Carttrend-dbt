select 
c.id_commande as id_commande, 
c.id_client as id_client, 
c.id_entrep__t_d__part as id_entrepot_depart, 
c.date_commande as date_commande, 
c.statut_commande as statut_commande, 
case 
    when p.id_promotion is not null then c.id_promotion_appliqu__e
    else null
end as id_promotion_appliquee,
c.mode_de_paiement as mode_de_paiement,
c.num__ro_tracking as numero_tracking,
c.date_livraison_estim__e as date_livraison_estimee
from {{source('carttrend_brut','Carttrend_Commandes')}} c
left join {{ref('Promotions')}} p
on c.id_promotion_appliqu__e = p.id_promotion
