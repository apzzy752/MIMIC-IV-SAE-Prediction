WITH input_sum AS (
    SELECT 
        ak.stay_id,
        SUM(ie.totalamount) AS input_total
    FROM 
        mimic_icu.inputevents ie
    JOIN 
        public.aki_sepsis ak ON ie.stay_id = ak.stay_id
    WHERE 
        ie.starttime BETWEEN ak.icu_intime AND ak.icu_intime + INTERVAL '24 hours'
    GROUP BY 
        ak.stay_id
),
output_sum AS (
    SELECT 
        ak.stay_id,
        SUM(oe.value) AS output_total
    FROM 
        mimic_icu.outputevents oe
    JOIN 
        public.aki_sepsis ak ON oe.stay_id = ak.stay_id
    WHERE 
        oe.charttime BETWEEN ak.icu_intime AND ak.icu_intime + INTERVAL '24 hours'
    GROUP BY 
        ak.stay_id
)
SELECT 
    ak.subject_id,
    ak.hadm_id,
    ak.stay_id,
    COALESCE(i.input_total, 0) AS total_input,
    COALESCE(o.output_total, 0) AS total_output,
    COALESCE(i.input_total, 0) - COALESCE(o.output_total, 0) AS balance
FROM 
    public.aki_sepsis ak
LEFT JOIN 
    input_sum i ON ak.stay_id = i.stay_id
LEFT JOIN 
    output_sum o ON ak.stay_id = o.stay_id
ORDER BY 
    ak.hadm_id, ak.stay_id;
