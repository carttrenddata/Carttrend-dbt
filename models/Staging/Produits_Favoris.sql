WITH split_favoris AS (
  SELECT
    id_client,
    SPLIT(favoris, ',') AS favoris_array
  FROM
    {{source("google_drive", "carttrend_clients_clients")}}
)
SELECT
  id_client,
  REGEXP_REPLACE(favori, r'^P(\d{3})$', r'P00\1') as id_produit -- ne transformer que pour le format comme P123 => P00123
FROM
  split_favoris,
  UNNEST(favoris_array) AS favori
WHERE
    id_client in (SELECT id_client from {{ref('Clients')}})
    AND REGEXP_REPLACE(favori, r'^P(\d{3})$', r'P00\1') in (SELECT id_produit from {{ref('Produits')}})