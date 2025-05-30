WITH commandes_mois AS (
    SELECT 
        id_entrepot,
        DATE_TRUNC(date_commande, MONTH) AS mois, -- transformer format 2024-05-17 en 2024-05-01
        CAST(AVG(DATE_DIFF(date_livraison_estimee, date_commande, DAY)) AS INT64) AS delai_livraison_et_traitement_entrepot,
        COUNT(id_commande) as nombres_commande_traitees_entrepot
    FROM {{ref("Commandes")}}
    GROUP BY id_entrepot, mois
)
SELECT 
  m.date,
  m.id_machine,
  m.id_entrepot,
  e.localisation,
  m.volume_traite,
  m.type_machine,
  1 - m.temps_d_arret / (DATE_DIFF(
    DATE_ADD(m.date, INTERVAL 1 MONTH),
    m.date,
    DAY
  ) * 24) as pourcentage_uptime,
  cm.delai_livraison_et_traitement_entrepot,
  cm.nombres_commande_traitees_entrepot
FROM {{ref('Entrepots_Machines')}} m
  JOIN {{ref('Entrepots')}} e using(id_entrepot)
  JOIN commandes_mois cm on e.id_entrepot = cm.id_entrepot and m.date = cm.mois