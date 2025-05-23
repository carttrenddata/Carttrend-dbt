/* select p.nom, p.prix, p.categorie, p.sous_categorie, 
case
    when pf.id_produit is not null then true
    else false
end as produit_favoris,
pr.valeur_promotion, pr.pourcentage_promotion,
dc.quantite,
co.date_commande,
cl.age, cl.tranche_age, cl.genre, cl.frequence_visites, cl.date_inscription, cl.anciennete_jours
from {{ref('Commandes')}} co
left join {{ref('Details_commandes')}} dc using (id_commande)
left join {{ref('Produits')}} p using (id_produit)
left join {{ref('Promotions')}} pr using (id_promotion)
left join {{ref('Clients')}} cl using (id_client)
left join {{ref('Produits_Favoris')}} pf on p.id_produit = pf.id_produit */

select 
    co.date_commande,
    p.nom,
    ROUND(p.prix * dc.quantite, 2) AS prix_initial, -- Ajouté la multiplication pour avoir le prix total
    ROUND(case 
        when pourcentage_promotion is not NULL then
            p.prix * dc.quantite * (1 - pourcentage_promotion)
        else p.prix * dc.quantite 
    end, 2) as prix_apres_remise, -- Ajouté le calcul de la remise
    p.categorie, 
    p.sous_categorie, 
    case 
        when exists (
            select *
            from {{ref('Produits_Favoris')}} pf
            where pf.id_client = cl.id_client and pf.id_produit = p.id_produit )
        then true
        else false
    end as produit_favori, -- Corigé le test du produit favori qui ne marchait pas
    pr.valeur_promotion,
    pr.pourcentage_promotion,
    dc.quantite,
    cl.age,
    cl.tranche_age,
    cl.genre,
    cl.frequence_visites,
    cl.date_inscription,
    cl.anciennete_jours
from {{ref('Commandes')}} co
left join {{ref('Details_commandes')}} dc using (id_commande)
left join {{ref('Produits')}} p using (id_produit)
left join {{ref('Promotions')}} pr using (id_promotion)
left join {{ref('Clients')}} cl using (id_client)