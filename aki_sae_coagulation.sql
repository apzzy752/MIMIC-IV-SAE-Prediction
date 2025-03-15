WITH earliest_d_dimer AS (
    SELECT 
        np.hadm_id,
        co.d_dimer,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id ORDER BY co.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.coagulation co 
        ON np.hadm_id = co.hadm_id 
        AND co.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND d_dimer IS NOT NULL
),
earliest_fibrinogen AS (
    SELECT 
        np.hadm_id,
        co.fibrinogen,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id ORDER BY co.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.coagulation co 
        ON np.hadm_id = co.hadm_id 
        AND co.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND fibrinogen IS NOT NULL
),
earliest_thrombin AS (
    SELECT 
        np.hadm_id,
        co.thrombin,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id ORDER BY co.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.coagulation co 
        ON np.hadm_id = co.hadm_id 
        AND co.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND thrombin IS NOT NULL
),
earliest_inr AS (
    SELECT 
        np.hadm_id,
        co.inr,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id ORDER BY co.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.coagulation co 
        ON np.hadm_id = co.hadm_id 
        AND co.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND inr IS NOT NULL
),
earliest_pt AS (
    SELECT 
        np.hadm_id,
        co.pt,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id ORDER BY co.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.coagulation co 
        ON np.hadm_id = co.hadm_id 
        AND co.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND pt IS NOT NULL
),
earliest_ptt AS (
    SELECT 
        np.hadm_id,
        co.ptt,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id ORDER BY co.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.coagulation co 
        ON np.hadm_id = co.hadm_id 
        AND co.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND ptt IS NOT NULL
)
SELECT 
    np.subject_id,
    np.hadm_id,
    np.stay_id,
    ed_dimer.d_dimer,
    efib.fibrinogen,
    ethromb.thrombin,
    einr.inr,
    ept.pt,
    eptt.ptt
FROM 
    public.aki_sepsis np
LEFT JOIN 
    earliest_d_dimer ed_dimer ON np.hadm_id = ed_dimer.hadm_id AND ed_dimer.rn = 1
LEFT JOIN 
    earliest_fibrinogen efib ON np.hadm_id = efib.hadm_id AND efib.rn = 1
LEFT JOIN 
    earliest_thrombin ethromb ON np.hadm_id = ethromb.hadm_id AND ethromb.rn = 1
LEFT JOIN 
    earliest_inr einr ON np.hadm_id = einr.hadm_id AND einr.rn = 1
LEFT JOIN 
    earliest_pt ept ON np.hadm_id = ept.hadm_id AND ept.rn = 1
LEFT JOIN 
    earliest_ptt eptt ON np.hadm_id = eptt.hadm_id AND eptt.rn = 1
ORDER BY 
    np.hadm_id;
