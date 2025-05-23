WITH split_favoris AS (
  SELECT
    id_client,
    SPLIT(favoris, ',') AS favoris_array
  FROM
    {{source("carttrend_brut", "Carttrend_Clients")}}
)
SELECT
  id_client,
  REGEXP_REPLACE(favori, r'^P(\d{3})$', r'P00\1') as id_produit
FROM
  split_favoris,
  UNNEST(favoris_array) AS favori
WHERE
    id_client in (SELECT id_client from {{ref('Clients')}})
    AND REGEXP_REPLACE(favori, r'^P(\d{3})$', r'P00\1') in (SELECT id_produit from {{ref('Produits')}})

