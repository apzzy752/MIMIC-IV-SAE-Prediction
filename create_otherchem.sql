CREATE MATERIALIZED VIEW public.otherchem AS
SELECT
    le.subject_id,
    le.hadm_id,
    le.charttime,
    le.specimen_id,
    
    MAX(CASE WHEN le.itemid = 50970 THEN le.valuenum ELSE NULL END) AS Phosphate,
    MAX(CASE WHEN le.itemid = 50920 THEN le.valuenum ELSE NULL END) AS eGFR,  
    MAX(CASE WHEN le.itemid = 51213 THEN le.valuenum ELSE NULL END) AS FDP,
    MAX(CASE WHEN le.itemid = 50960 THEN le.valuenum ELSE NULL END) AS Magnesium  
FROM mimic_hosp.labevents le
WHERE le.itemid = ANY (ARRAY[50970, 50920, 51213, 50960])  
GROUP BY le.subject_id, le.hadm_id, le.charttime, le.specimen_id;
