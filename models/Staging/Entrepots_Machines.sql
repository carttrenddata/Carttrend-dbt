select 
id_machine,
id_entrep__t as id_entrepot,
case
    when LOWER(type_machine) in ('tri','maintenance','transport','stockage','conditionnement') then type_machine
    else 'Autre'
end as type_machine,
case
    when LOWER(__tat_machine) in ('en service','en panne','en maintenance') then INITCAP(REPLACE(__tat_machine, 'En ', ''))
    else 'Autre'
end as etat_machine,
case
   when temps_d___arr__t >=0 then temps_d___arr__t
   else 0
end as temps_d_arret,
case
   when volume_trait__ >=0 then volume_trait__
   else 0
end as volume_traite,

PARSE_DATE('%Y-%m', mois) as date
from {{source("carttrend_brut", "Carttrend_Entrepots_Machines")}}
