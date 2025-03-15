WITH earliest_albumin AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        ch.albumin,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY ch.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.chemistry ch 
        ON np.hadm_id = ch.hadm_id 
        AND ch.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND ch.albumin IS NOT NULL
),
earliest_bun AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        ch.bun,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY ch.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.chemistry ch 
        ON np.hadm_id = ch.hadm_id 
        AND ch.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND ch.bun IS NOT NULL
),
earliest_calcium AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        ch.calcium,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY ch.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.chemistry ch 
        ON np.hadm_id = ch.hadm_id 
        AND ch.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND ch.calcium IS NOT NULL
),
earliest_chloride AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        ch.chloride,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY ch.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.chemistry ch 
        ON np.hadm_id = ch.hadm_id 
        AND ch.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND ch.chloride IS NOT NULL
),
earliest_creatinine AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        ch.creatinine,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY ch.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.chemistry ch 
        ON np.hadm_id = ch.hadm_id 
        AND ch.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND ch.creatinine IS NOT NULL
),
earliest_sodium AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        ch.sodium,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY ch.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.chemistry ch 
        ON np.hadm_id = ch.hadm_id 
        AND ch.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND ch.sodium IS NOT NULL
),
earliest_potassium AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        ch.potassium,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY ch.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.chemistry ch 
        ON np.hadm_id = ch.hadm_id 
        AND ch.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND ch.potassium IS NOT NULL
)
SELECT 
    np.subject_id,
    np.hadm_id,
    np.stay_id,
    ea.albumin,
    ebun.bun,
    ecal.calcium,
    echl.chloride,
    ecrea.creatinine,
    esod.sodium,
    epot.potassium
FROM 
    public.aki_sepsis np
LEFT JOIN 
    earliest_albumin ea ON np.hadm_id = ea.hadm_id AND np.stay_id = ea.stay_id AND ea.rn = 1
LEFT JOIN 
    earliest_bun ebun ON np.hadm_id = ebun.hadm_id AND np.stay_id = ebun.stay_id AND ebun.rn = 1
LEFT JOIN 
    earliest_calcium ecal ON np.hadm_id = ecal.hadm_id AND np.stay_id = ecal.stay_id AND ecal.rn = 1
LEFT JOIN 
    earliest_chloride echl ON np.hadm_id = echl.hadm_id AND np.stay_id = echl.stay_id AND echl.rn = 1
LEFT JOIN 
    earliest_creatinine ecrea ON np.hadm_id = ecrea.hadm_id AND np.stay_id = ecrea.stay_id AND ecrea.rn = 1
LEFT JOIN 
    earliest_sodium esod ON np.hadm_id = esod.hadm_id AND np.stay_id = esod.stay_id AND esod.rn = 1
LEFT JOIN 
    earliest_potassium epot ON np.hadm_id = epot.hadm_id AND np.stay_id = epot.stay_id AND epot.rn = 1
ORDER BY 
    np.hadm_id, np.stay_id;
