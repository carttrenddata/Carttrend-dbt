-- https://rasbt.github.io/mlxtend/user_guide/frequent_patterns/apriori/

SELECT
  ARRAY_AGG(DISTINCT dc.id_produit) AS produits
FROM {{ref('Commandes')}} c
    JOIN {{ref('Details_commandes')}} dc USING(id_commande)
GROUP BY c.id_client