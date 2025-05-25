select 
id_machine,
id_entrepot,
case
    when LOWER(type_machine) in ('tri','maintenance','transport','stockage','conditionnement') then type_machine
    else 'Autre'
end as type_machine,
case
    when LOWER(etat_machine) in ('en service','en panne','en maintenance') then INITCAP(REPLACE(etat_machine, 'En ', ''))
    else 'Autre'
end as etat_machine,
case
   when temps_d_arret >=0 then temps_d_arret
   else 0
end as temps_d_arret,
case
   when volume_traite >=0 then volume_traite
   else 0
end as volume_traite,

PARSE_DATE('%Y-%m', mois) as date
from {{source("google_drive", "carttrend_entrepots_machines_machines_entrepots")}}
