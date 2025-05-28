select 
c.id_commande as id_commande, 
c.id_client as id_client, 
c.id_entrepot_depart as id_entrepot,
c.date_commande as date_commande, 
case 
    when LOWER(c.statut_commande) in ('en transit', 'annulée', 'validée', 'livrée') then c.statut_commande
    else null
end as statut_commande,
id_promotion_appliquee as id_promotion,
case 
    when LOWER(c.mode_de_paiement) in ('paypal', 'virement bancaire', 'carte de crédit') then c.mode_de_paiement
    else null
end as mode_de_paiement,
c.numero_tracking as numero_tracking,
case 
    when DATE_DIFF(c.date_livraison_estimee, c.date_commande, day)>=0 then c.date_livraison_estimee
    else null
end as date_livraison_estimee
from {{source('google_drive','carttrend_commandes_commandes')}} c
where c.id_commande is not null and c.id_client is not null