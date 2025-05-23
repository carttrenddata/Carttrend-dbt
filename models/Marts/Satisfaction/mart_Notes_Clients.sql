SELECT 
    id_produit, 
    nom, 
    categorie, 
    date_commande,
    date_livraison_estimee, 
    type_plainte,
    AVG(note_client) AS avg_note_client
FROM {{ ref("Produits") }}
JOIN {{ ref("Details_commandes")}} USING(id_produit)
JOIN {{ ref("Commandes") }} USING(id_commande)
JOIN {{ ref("Satisfaction") }} USING(id_commande)
GROUP BY 
    id_produit, 
    nom, 
    categorie, 
    date_commande,
    date_livraison_estimee, 
    type_plainte
ORDER BY avg_note_client ASC