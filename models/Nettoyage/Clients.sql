select 
    id_client,
    prenom_client as prenom,
    nom_client as nom,
    email as email,
    __ge as age,
    genre as genre,
    fr__quence_visites as frequence_visites,
    REGEXP_REPLACE(
        REGEXP_EXTRACT(REGEXP_REPLACE(num__ro_t__l__phone, r'\D', ''), r'(\d{1,3})(\d{3})(\d{3})(\d{4})'),
        r'(\d{1,3})(\d{3})(\d{3})(\d{4})',
        r'+\1-\2-\3-\4'
        ) AS numero_telephone,
    date_inscription,
    favoris as favoris,
    adresse_ip as adresse_ip
from {{source("carttrend_brut",'Carttrend_Clients')}}
where id_client is not NULL
