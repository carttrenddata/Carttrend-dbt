SELECT 
  EXTRACT(MONTH FROM m.date) AS month,
  e.localisation,
  e.temperature,
  m.volume_traite,
  m.type_machine,
  1 - m.temps_d_arret / (DATE_DIFF(
    DATE_ADD(m.date, INTERVAL 1 MONTH),
    m.date,
    DAY
  ) * 24) as pourcentage_uptime,
FROM {{ref("Entrepots_Machines")}} m
  JOIN {{ref('Entrepots')}} e using(id_entrepot)