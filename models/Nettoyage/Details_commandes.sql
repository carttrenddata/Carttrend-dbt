select
id_commande,
id_produit,
quantit__ as quantite,
emballage_sp__cial as emballage_special
from {{source("carttrend_brut",'Carttrend_Details_Commandes')}}
