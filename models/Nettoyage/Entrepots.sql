select
    id_entrep__t as id_entrepot,
    initcap(localisation) as localisation,
    CASE 
        WHEN capacit___max < 0 THEN 0
        ELSE capacit___max
    END AS capacite_max,
    CASE 
        WHEN volume_stock__ < 0 THEN 0
        ELSE volume_stock__
    END AS volume_stocke,
    volume_stock__ / capacit___max AS taux_remplissage, -- Hugues : Calculé, evite les erreurs et les arrondis...
    temp__rature_moyenne_entrep__t as temperature
from {{source("carttrend_brut", "Carttrend_Entrepots")}}