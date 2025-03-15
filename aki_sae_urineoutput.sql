SELECT 
    ak.subject_id,
    ak.hadm_id,
    ak.stay_id,
    fuo.urineoutput
FROM 
    public.aki_sepsis ak
LEFT JOIN 
    public.first_day_urine_output fuo ON ak.stay_id = fuo.stay_id
ORDER BY hadm_id, stay_id;
