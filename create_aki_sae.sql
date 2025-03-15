CREATE TABLE public.aki_sepsis AS
WITH aki_patients AS (
    SELECT 
        icu.*
    FROM 
        public.sepsis3 s
    JOIN 
        public.kdigo_stages k ON s.stay_id = k.stay_id
    JOIN 
        public.icustay_detail icu ON s.stay_id = icu.stay_id
    WHERE 
        k.aki_stage != 0 AND k.charttime BETWEEN icu.icu_intime AND icu.icu_intime + INTERVAL '24 hours'
),

max_aki_stage AS (
    SELECT 
        k.stay_id,
        MAX(k.aki_stage) AS max_aki_stage
    FROM 
        public.kdigo_stages k
    JOIN 
        public.icustay_detail icu ON k.stay_id = icu.stay_id
    WHERE 
        k.charttime BETWEEN icu.icu_intime AND icu.icu_intime + INTERVAL '24 hours'
    GROUP BY 
        k.stay_id
),

delirium_gcs AS (
    SELECT 
        stay_id
    FROM 
        public.gcs
    WHERE 
        gcs < 15
    GROUP BY 
        stay_id
),

delirium_diagnosis AS (
    SELECT 
        d.hadm_id
    FROM 
        mimic_hosp.diagnoses_icd d
    WHERE 
        d.icd_code IN ('2930', '2931', 'F05')
    GROUP BY 
        d.hadm_id
),

excluded_major_conditions AS (
    SELECT DISTINCT hadm_id
    FROM mimic_hosp.diagnoses_icd
    WHERE icd_code IN (
        '85400', 'S069X0A', '431', 'I639', 'I619', 'G409', 'G060'
    )
    OR icd_code LIKE '433%1'   
    OR icd_code LIKE '345%'    
    OR icd_code LIKE '324%'    
    OR icd_code BETWEEN '941' AND '948'  
    OR icd_code BETWEEN 'T20' AND 'T3299'  
    OR icd_code BETWEEN '800' AND '898'  
    OR icd_code BETWEEN 'S00' AND 'T1491XS'  
    OR icd_code IN (
        '29634', '29635', '29636', '29640', '29641', '29642', '29643', '29644', '29650', 
        '29651', '29652', '2971', '2972', '29500', '29501', '29502', '29510', '29520', '29530', 
        '29410', '29411', '3310', '33182', '33119', '2980', '2981', '2982', '2989',
        '30303', '30390', '30391', '30392', '30393', '30400', '30401', '30402', '30403', 
        '30410', '30411', '30412', '30413', '30420', '30421', '30422', '30423', '30430', 
        '30431', '30432', '30433', '30440', '30441', '30442', '30443', '30460',
        '34831', '5722', '0700', '07020', '07021', '07022', '07023', '07041', '07042', '07043', 
        '07044', '07049', '07052', '0706', '07071', '2706', '2510', '4372', 
        '2761', 'E871', 
        '25080', '25000', 'E1165', 'E1065', '2512', '2510', 'E162', 'E161', 
        '51881', '51884', 
        '2768', 'J9600', 'J9690', 'E875',
        '9960', '5A12012', '5A1200Z', '3481', 'G931', '4275', 'I469', '7991', 'R092'
    )
),

sedative_excluded_patients AS (
    SELECT DISTINCT 
        p.hadm_id
    FROM 
        mimic_hosp.prescriptions p
    JOIN 
        aki_patients a ON p.hadm_id = a.hadm_id
    WHERE 
        p.drug ILIKE ANY (ARRAY[
            '%Midazolam%', 
            '%Propofol%', 
            '%Fentanyl%', 
            '%Morphine%', 
            '%Oxycodone%', 
            '%Etomidate%'
        ])
        AND p.starttime BETWEEN a.admittime AND a.icu_intime
)

SELECT DISTINCT
    a.*,
    CASE 
        WHEN g.stay_id IS NOT NULL OR d.hadm_id IS NOT NULL THEN 1
        ELSE 0
    END AS sae,
    m.max_aki_stage  -- 从 max_aki_stage 子查询中获取最大值
FROM 
    aki_patients a
LEFT JOIN 
    delirium_gcs g ON a.stay_id = g.stay_id
LEFT JOIN 
    delirium_diagnosis d ON a.hadm_id = d.hadm_id
LEFT JOIN 
    max_aki_stage m ON a.stay_id = m.stay_id
WHERE 
    a.hadm_id NOT IN (SELECT hadm_id FROM excluded_major_conditions)
    AND a.hadm_id NOT IN (SELECT hadm_id FROM sedative_excluded_patients)
    AND a.los_icu >= 1
ORDER BY 
    a.hadm_id, a.stay_id;
