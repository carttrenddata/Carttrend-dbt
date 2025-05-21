select 
c.id_commande as id_commande, 
c.id_client as id_client, 
/*case 
    when REGEXP_CONTAINS(c.id_entrep__t_d__part, r'^E\d{3}$') then c.id_entrep__t_d__part 
    else null
end as id_entrepot_depart,*/ -- Retrait du test de validation de l'id entrepot, redondant avec le test de cle etrangere dans schema.yaml
c.id_entrep__t_d__part as id_entrepot_depart,
c.date_commande as date_commande, 
case 
    when LOWER(c.statut_commande) in ('en transit', 'annulée', 'validée', 'livrée') then c.statut_commande
    else null
end as statut_commande,
/*case 
    when pr.id_promotion is not null then c.id_promotion_appliqu__e
    else null
end as id_promotion_appliquee,*/ -- Retrait du test de l'existence de la cle etrangere dans la table promotion, redondant avec le test dans schema.yaml
id_promotion_appliqu__e as id_promotion,
case 
    when LOWER(c.mode_de_paiement) in ('paypal', 'virement bancaire', 'carte de crédit') then c.mode_de_paiement
    else null
end as mode_de_paiement,
c.num__ro_tracking as numero_tracking,
case 
    when DATE_DIFF(c.date_livraison_estim__e, c.date_commande, day)>=0 then c.date_livraison_estim__e
    else null
end as date_livraison_estimee
from {{source('carttrend_brut','Carttrend_Commandes')}} c
/*
left join (
    select id_promotion from {{ref('Promotions')}}
    ) as pr
on c.id_promotion_appliqu__e = pr.id_promotion */ -- Join plus necessaire du coup !
where c.id_commande is not null and c.id_client is not null