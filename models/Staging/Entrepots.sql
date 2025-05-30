select
    id_entrepot,
    initcap(localisation) as localisation, -- ex. paris => Paris
    CASE 
        WHEN capacite_max < 0 THEN 0
        ELSE capacite_max
    END AS capacite_max,
    CASE 
        WHEN volume_stocke < 0 THEN 0
        ELSE volume_stocke
    END AS volume_stocke,
    volume_stocke / capacite_max AS taux_remplissage,
    temperature_moyenne_entrepot as temperature
from {{source("google_drive", "carttrend_entrepots_entrepots")}}