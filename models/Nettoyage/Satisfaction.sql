select 
id_satisfaction,
id_commande,
CASE 
    WHEN note_client > 5 THEN 5
    ELSE note_client < 1 THEN 1
    ELSE note_client
END AS note_client,
commentaire,
plainte,
temps_r__ponse_support as temps_reponse_support,
type_plainte, 
employ___support as employe_support

from {{source("carttrend_brut",'Carttrend_Satisfaction')}}
