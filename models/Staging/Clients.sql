with tel as (
    select
        id_client,
        REGEXP_REPLACE(num__ro_t__l__phone, r'^.*?(\d{3})\D?(\d{3})\D?(\d{4}).*$', r'\1-\2-\3') AS numero_telephone,
        CASE 
            WHEN LENGTH(ac.area_code) = 3 THEN CONCAT('+', ac.area_code)
            WHEN LENGTH(ac.area_code) = 2 THEN CONCAT('+0', ac.area_code)
            WHEN LENGTH(ac.area_code) = 1 THEN CONCAT('+00', ac.area_code)
            ELSE '+033'
        END as area_code,
    from {{source("carttrend_brut",'Carttrend_Clients')}}
        join ( 
            SELECT 
                id_client,
                REGEXP_EXTRACT(num__ro_t__l__phone, r'^\+?([1-9]{0,3}).*$') AS area_code
            FROM {{source("carttrend_brut",'Carttrend_Clients')}}
        ) ac using(id_client)
)
select 
    id_client,
    sha256(CONCAT(prenom_client, nom_client)) as hash_prenom_nom,
    CASE 
        WHEN REGEXP_CONTAINS(email, r'^[\w\.-]+@[\w-]+.[A-Za-z]+$') THEN email
        ELSE NULL
    END as email,
    CASE
        WHEN __ge > 0 THEN __ge
        ELSE NULL
    END as age,
    CASE
        WHEN __ge > 0  AND __ge <= 25 THEN '<=25'
        WHEN __ge > 25  AND __ge <= 40 THEN '25-40'
        WHEN __ge > 40  AND __ge <= 55 THEN '40-55'
        WHEN __ge > 55  AND __ge <= 70 THEN '55-70'
        WHEN __ge > 70 THEN '>70'
        ELSE NULL
    END as tranche_age,
    CASE
        WHEN lower(genre) = 'homme' THEN 'Homme'
        WHEN lower(genre) = 'femme' THEN 'Femme'
        ELSE NULL
    END as genre, # Probablement à changer de nos jours...
    CASE
        WHEN fr__quence_visites > 0 THEN fr__quence_visites
        ELSE 0
    END as frequence_visites,
    CONCAT(tel.area_code, '-', tel.numero_telephone) as telephone,
    CASE 
        WHEN DATE_DIFF(date_inscription, CURRENT_DATE(), DAY) <= 0 THEN date_inscription
        ELSE NULL
    END as date_inscription,
    CASE
        WHEN DATE_DIFF(date_inscription, CURRENT_DATE(), DAY) <= 0 THEN DATE_DIFF(CURRENT_DATE(), date_inscription, DAY)
        ELSE NULL
    END as anciennete_jours,
    CASE
        WHEN REGEXP_CONTAINS(adresse_ip, r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$') THEN adresse_ip
        ELSE NULL
    END as ip    # IPv4 seulement
from {{source("carttrend_brut",'Carttrend_Clients')}}
    join tel using(id_client)
where id_client is not NULL
