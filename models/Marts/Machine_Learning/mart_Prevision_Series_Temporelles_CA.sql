WITH 
    cal AS (
        SELECT date
        FROM UNNEST(GENERATE_DATE_ARRAY(
            (SELECT MIN(date_commande) FROM {{ref('mart_Commandes')}}),
            (SELECT MAX(date_commande) FROM {{ref('mart_Commandes')}})
        )) AS date
    ),
    ca AS (
        SELECT 
            date_commande AS date,
            SUM(prix_apres_remise) AS ca
        FROM {{ref('mart_Commandes')}}
        WHERE statut_commande != 'Annulée'
        GROUP BY date_commande
    )
SELECT
    cal.date,
    ROUND(IFNULL(ca.ca, 0), 2) as chiffre_affaire
FROM cal
    LEFT JOIN ca USING(date)
ORDER BY date