WITH earliest_wbc AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        bd.wbc,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY bd.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.blood_differential bd 
        ON np.hadm_id = bd.hadm_id 
        AND bd.charttime >= np.icu_intime
        AND bd.charttime < np.icu_intime + INTERVAL '24 hours'
        AND bd.wbc IS NOT NULL
),
earliest_lymphocytes_abs AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        bd.lymphocytes_abs,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY bd.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.blood_differential bd 
        ON np.hadm_id = bd.hadm_id 
        AND bd.charttime >= np.icu_intime
        AND bd.charttime < np.icu_intime + INTERVAL '24 hours'
        AND bd.lymphocytes_abs IS NOT NULL
),
earliest_neutrophils_abs AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        bd.neutrophils_abs,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY bd.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.blood_differential bd 
        ON np.hadm_id = bd.hadm_id 
        AND bd.charttime >= np.icu_intime
        AND bd.charttime < np.icu_intime + INTERVAL '24 hours'
        AND bd.neutrophils_abs IS NOT NULL
),
earliest_lymphocytes AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        bd.lymphocytes,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY bd.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.blood_differential bd 
        ON np.hadm_id = bd.hadm_id 
        AND bd.charttime >= np.icu_intime
        AND bd.charttime < np.icu_intime + INTERVAL '24 hours'
        AND bd.lymphocytes IS NOT NULL
),
earliest_neutrophils AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        bd.neutrophils,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY bd.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.blood_differential bd 
        ON np.hadm_id = bd.hadm_id 
        AND bd.charttime >= np.icu_intime
        AND bd.charttime < np.icu_intime + INTERVAL '24 hours'
        AND bd.neutrophils IS NOT NULL
),
earliest_hemoglobin AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        cbc.hemoglobin,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY cbc.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.complete_blood_count cbc 
        ON np.hadm_id = cbc.hadm_id 
        AND cbc.charttime >= np.icu_intime
        AND cbc.charttime < np.icu_intime + INTERVAL '24 hours'
        AND cbc.hemoglobin IS NOT NULL
),
earliest_platelet AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        cbc.platelet,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY cbc.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.complete_blood_count cbc 
        ON np.hadm_id = cbc.hadm_id 
        AND cbc.charttime >= np.icu_intime
        AND cbc.charttime < np.icu_intime + INTERVAL '24 hours'
        AND cbc.platelet IS NOT NULL
),
earliest_crp AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        inf.crp,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY inf.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.inflammation inf 
        ON np.hadm_id = inf.hadm_id 
        AND inf.charttime >= np.icu_intime
        AND inf.charttime < np.icu_intime + INTERVAL '24 hours'
        AND inf.crp IS NOT NULL
),
earliest_egfr AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        oc.egfr,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY oc.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.otherchem oc 
        ON np.hadm_id = oc.hadm_id 
        AND oc.charttime >= np.icu_intime
        AND oc.charttime < np.icu_intime + INTERVAL '24 hours'
        AND oc.egfr IS NOT NULL
),
earliest_fdp AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        oc.fdp,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY oc.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.otherchem oc 
        ON np.hadm_id = oc.hadm_id 
        AND oc.charttime >= np.icu_intime
        AND oc.charttime < np.icu_intime + INTERVAL '24 hours'
        AND oc.fdp IS NOT NULL
),
earliest_phosphate AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        oc.phosphate,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY oc.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.otherchem oc 
        ON np.hadm_id = oc.hadm_id 
        AND oc.charttime >= np.icu_intime
        AND oc.charttime < np.icu_intime + INTERVAL '24 hours'
        AND oc.phosphate IS NOT NULL
),
earliest_magnesium AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        oc.magnesium,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY oc.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.otherchem oc 
        ON np.hadm_id = oc.hadm_id 
        AND oc.charttime >= np.icu_intime
        AND oc.charttime < np.icu_intime + INTERVAL '24 hours'
        AND oc.magnesium IS NOT NULL
)
SELECT 
    np.subject_id,
    np.hadm_id,
    np.stay_id,
    ewbc.wbc AS wbc_day1,
    ely_abs.lymphocytes_abs AS lymphocytes_abs_day1,
    enu_abs.neutrophils_abs AS neutrophils_abs_day1,
    ely.lymphocytes AS lymphocytes_day1,
    enu.neutrophils AS neutrophils_day1,
    eplt.platelet AS platelet_day1,
    ehgb.hemoglobin AS hemoglobin_day1,
    ecrp.crp AS crp_day1,
    eegfr.egfr AS egfr_day1,
    efdp.fdp AS fdp_day1,
    ephosphate.phosphate AS phosphate_day1,
    emagnesium.magnesium AS magnesium_day1
FROM 
    public.aki_sepsis np
LEFT JOIN 
    earliest_wbc ewbc ON np.hadm_id = ewbc.hadm_id AND np.stay_id = ewbc.stay_id AND ewbc.rn = 1
LEFT JOIN 
    earliest_lymphocytes_abs ely_abs ON np.hadm_id = ely_abs.hadm_id AND np.stay_id = ely_abs.stay_id AND ely_abs.rn = 1
LEFT JOIN 
    earliest_neutrophils_abs enu_abs ON np.hadm_id = enu_abs.hadm_id AND np.stay_id = enu_abs.stay_id AND enu_abs.rn = 1
LEFT JOIN 
    earliest_lymphocytes ely ON np.hadm_id = ely.hadm_id AND np.stay_id = ely.stay_id AND ely.rn = 1
LEFT JOIN 
    earliest_neutrophils enu ON np.hadm_id = enu.hadm_id AND np.stay_id = enu.stay_id AND enu.rn = 1
LEFT JOIN 
    earliest_platelet eplt ON np.hadm_id = eplt.hadm_id AND np.stay_id = eplt.stay_id AND eplt.rn = 1
LEFT JOIN 
    earliest_hemoglobin ehgb ON np.hadm_id = ehgb.hadm_id AND np.stay_id = ehgb.stay_id AND ehgb.rn = 1
LEFT JOIN 
    earliest_crp ecrp ON np.hadm_id = ecrp.hadm_id AND np.stay_id = ecrp.stay_id AND ecrp.rn = 1
LEFT JOIN 
    earliest_egfr eegfr ON np.hadm_id = eegfr.hadm_id AND np.stay_id = eegfr.stay_id AND eegfr.rn = 1
LEFT JOIN 
    earliest_fdp efdp ON np.hadm_id = efdp.hadm_id AND np.stay_id = efdp.stay_id AND efdp.rn = 1
LEFT JOIN 
    earliest_phosphate ephosphate ON np.hadm_id = ephosphate.hadm_id AND np.stay_id = ephosphate.stay_id AND ephosphate.rn = 1
LEFT JOIN 
    earliest_magnesium emagnesium ON np.hadm_id = emagnesium.hadm_id AND np.stay_id = emagnesium.stay_id AND emagnesium.rn = 1
ORDER BY 
    np.hadm_id, np.stay_id;
