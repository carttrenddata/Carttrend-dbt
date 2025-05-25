select * from
(
    select 
        REGEXP_REPLACE(p.id_promotion, r'^PROM(\d{3})$', r'P\1') as id_promotion,
        p.id_produit as id_produit,
        pr.Prix,
        p.type_promotion,
        case 
            when (p.type_promotion = 'Pourcentage') then round(pr.prix * p.valeur_promotion, 2)
            when (p.type_promotion = 'Remise fixe') then p.valeur_promotion
        end as valeur_promotion,
        case 
            when (p.type_promotion = 'Pourcentage') then p.valeur_promotion
            when (p.type_promotion = 'Remise fixe') then p.valeur_promotion / pr.prix 
        end as pourcentage_promotion,
        p.date_debut as date_debut,
        p.date_fin as date_fin
    from {{source('google_drive','carttrend_promotions_promotions')}} p
    join (
        select id_produit, prix from {{ref('Produits')}}
        ) as pr
        on p.id_produit = pr.id_produit
    where DATE_DIFF(p.date_fin, p.date_debut, day) >= 0
)
where pourcentage_promotion < 1
