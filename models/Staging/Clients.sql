with tel as (
    select
        id_client,
        REGEXP_REPLACE(numero_telephone, r'^.*?(\d{3})\D?(\d{3})\D?(\d{4}).*$', r'\1-\2-\3') AS numero_telephone,
        CASE 
            WHEN LENGTH(ac.area_code) = 3 THEN CONCAT('+', ac.area_code)
            WHEN LENGTH(ac.area_code) = 2 THEN CONCAT('+0', ac.area_code)
            WHEN LENGTH(ac.area_code) = 1 THEN CONCAT('+00', ac.area_code)
            ELSE '+033'
        END as area_code,
    from {{source("google_drive",'carttrend_clients_clients')}}
        join ( 
            SELECT 
                id_client,
                REGEXP_EXTRACT(numero_telephone, r'^\+?([1-9]{0,3}).*$') AS area_code
            FROM {{source("google_drive",'carttrend_clients_clients')}}
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
        WHEN age > 0 THEN age
        ELSE NULL
    END as age,
    CASE
        WHEN age > 0  AND age <= 25 THEN '<=25'
        WHEN age > 25  AND age <= 40 THEN '25-40'
        WHEN age > 40  AND age <= 55 THEN '40-55'
        WHEN age > 55  AND age <= 70 THEN '55-70'
        WHEN age > 70 THEN '>70'
        ELSE NULL
    END as tranche_age,
    CASE
        WHEN lower(genre) = 'homme' THEN 'Homme'
        WHEN lower(genre) = 'femme' THEN 'Femme'
        ELSE NULL
    END as genre, # Probablement à changer de nos jours...
    CASE
        WHEN frequence_visites > 0 THEN frequence_visites
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
from {{source("google_drive",'carttrend_clients_clients')}}
    join tel using(id_client)
where id_client is not NULL
