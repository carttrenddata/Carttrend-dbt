WITH 
    id_client,
    COUNT()

SELECT 
    cl.id_client,
    c.id_produit,
    (
        SELECT 
            COUNT(c.quantite)
        FROM {{ref('Commandes')}} co
        WHERE cl.id_clent = co.id_clent AND 
    )
    COUNT(c.quantite),


par categorie de produit, pourcentage de client qui ont achete au moins une fois un produit mis en favoris
