WITH unique_specimens AS (
    SELECT 
        ak.stay_id,
        ak.subject_id,
        ak.hadm_id,
        si.specimen,
        DENSE_RANK() OVER (PARTITION BY ak.stay_id ORDER BY si.specimen) AS rn
    FROM 
        public.aki_sepsis ak
    LEFT JOIN 
        public.suspicion_of_infection si ON ak.stay_id = si.stay_id
    WHERE 
        si.positive_culture = 1
)
SELECT 
    ak.subject_id,
    ak.hadm_id,
    ak.stay_id,
    MAX(CASE WHEN us.rn = 1 THEN us.specimen END) AS specimen_1,
    MAX(CASE WHEN us.rn = 2 THEN us.specimen END) AS specimen_2,
    MAX(CASE WHEN us.rn = 3 THEN us.specimen END) AS specimen_3,
    MAX(CASE WHEN us.rn = 4 THEN us.specimen END) AS specimen_4
FROM 
    public.aki_sepsis ak
LEFT JOIN 
    unique_specimens us ON ak.stay_id = us.stay_id
GROUP BY 
    ak.subject_id, ak.hadm_id, ak.stay_id
ORDER BY 
    ak.hadm_id, ak.stay_id;
