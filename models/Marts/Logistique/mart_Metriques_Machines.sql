/*select
em.id_entrepot,
co.date_commande,
date_livraison_estimee,
type_machine,
Sum(temps_d_arret) as temps_d_arret_total
from {{ref("Entrepots_Machines")}} em
left join {{ref("Entrepots")}} en on em.id_entrepot = en.id_entrepot
left join {{ref("Commandes")}} co on en.id_entrepot = co.id_entrepot
group by em.type_machine, co.date_commande, em.id_entrepot, date_livraison_estimee */

WITH commandes_mois AS (
    SELECT 
        id_entrepot,
        DATE_TRUNC(date_commande, MONTH) AS mois,
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
    m.etat_machine,
    m.temps_d_arret,
    cm.delai_livraison_et_traitement_entrepot,
    cm.nombres_commande_traitees_entrepot
FROM {{ref("Entrepots_Machines")}} m
    JOIN {{ref("Entrepots")}} e USING(id_entrepot)
    JOIN commandes_mois cm ON m.id_entrepot = cm.id_entrepot AND m.date = cm.mois