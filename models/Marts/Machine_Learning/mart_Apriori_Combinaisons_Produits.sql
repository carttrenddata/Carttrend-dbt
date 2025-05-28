-- https://rasbt.github.io/mlxtend/user_guide/frequent_patterns/apriori/

SELECT
  ARRAY_AGG(DISTINCT p.categorie) AS categorie
FROM {{ref('Commandes')}} c
    JOIN {{ref('Details_commandes')}} dc USING(id_commande)
    JOIN {{ref('Produits')}} p USING(id_produit)
GROUP BY c.id_client