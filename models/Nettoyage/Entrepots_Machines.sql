select 
id_entrep__t as id_entrepot,
__tat_machine as etat_machine,
temps_d___arr__t as temps_d_arret,
volume_trait__ as volume_traite,
PARSE_DATE('%Y-%m', mois) AS date
from {{source("carttrend_brut", "Carttrend_Entrepots_Machines")}}
