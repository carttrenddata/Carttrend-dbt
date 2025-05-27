SELECT
    delai_traitement,
    type_plainte,
    employe_support
FROM
    {{ref('Satisfaction')}}
WHERE employe_support IS NOT NULL
    AND type_plainte IS NOT NULL
    AND delai_traitement IS NOT NULL