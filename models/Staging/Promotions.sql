select * from
(
    select 
        REGEXP_REPLACE(p.id_promotion, r'^PROM(\d{3})$', r'P\1') as id_promotion, -- transformer format ex. 'PROM123' => 'P123'
        p.id_produit as id_produit,
        pr.Prix,
        p.type_promotion,
        case 
            when (p.type_promotion = 'Pourcentage') then round(pr.prix * p.valeur_promotion, 2) --calcul remise fixe avec la donnée 'valeur_promotion' qui est en pourcentage
            when (p.type_promotion = 'Remise fixe') then p.valeur_promotion
        end as valeur_promotion, -- uniformiser 'valeur_promotion' en remise fixe
        case 
            when (p.type_promotion = 'Pourcentage') then p.valeur_promotion
            when (p.type_promotion = 'Remise fixe') then p.valeur_promotion / pr.prix  --calcul % promotion avec la donnée remise fixe 
        end as pourcentage_promotion, -- ajouter une nouvelle colonne pour % promotion
        p.date_debut as date_debut,
        p.date_fin as date_fin
    from {{source('google_drive','carttrend_promotions_promotions')}} p
    join (
        select id_produit, prix from {{ref('Produits')}}
        ) as pr
        on p.id_produit = pr.id_produit     -- sous request pour avoir la colonne 'prix', 'id_produit'
    where DATE_DIFF(p.date_fin, p.date_debut, day) >= 0 -- vérifier date_debut de promotion < date_fin de promotion, sinon on prend pas la ligne
)
where pourcentage_promotion < 1  -- très util, dans les données brutes, bcp de % promo >1 => pas logique. on prend pas les lignes dont % promo > 1
