WITH ranked_sofa AS (
    SELECT
        s.stay_id,
        s.starttime AS sofa_starttime,
        s.sofa_24hours,
        ROW_NUMBER() OVER (PARTITION BY s.stay_id ORDER BY s.starttime ASC) AS rn
    FROM
        public.sofa s
)
SELECT
    a.subject_id,
    a.hadm_id,
    a.stay_id,
    rs.sofa_24hours,
    sp.sapsii
FROM
    public.aki_sepsis a
LEFT JOIN 
    ranked_sofa rs ON a.stay_id = rs.stay_id 
                    AND rs.rn = 1 
                    AND rs.sofa_starttime BETWEEN a.icu_intime AND a.icu_intime + INTERVAL '24 hours' -- 提取ICU24h内sofa
LEFT JOIN 
    public.sapsii sp ON a.stay_id = sp.stay_id
ORDER BY
    a.hadm_id, a.stay_id;
