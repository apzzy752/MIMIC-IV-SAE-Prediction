WITH earliest_heart_rate AS (
    SELECT 
        np.stay_id,
        vs.heart_rate,
        ROW_NUMBER() OVER (PARTITION BY np.stay_id ORDER BY vs.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.vitalsign vs 
        ON np.stay_id = vs.stay_id 
        AND vs.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND heart_rate IS NOT NULL
),
earliest_sbp AS (
    SELECT 
        np.stay_id,
        vs.sbp,
        ROW_NUMBER() OVER (PARTITION BY np.stay_id ORDER BY vs.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.vitalsign vs 
        ON np.stay_id = vs.stay_id 
        AND vs.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND sbp IS NOT NULL
),
earliest_dbp AS (
    SELECT 
        np.stay_id,
        vs.dbp,
        ROW_NUMBER() OVER (PARTITION BY np.stay_id ORDER BY vs.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.vitalsign vs 
        ON np.stay_id = vs.stay_id 
        AND vs.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND dbp IS NOT NULL
),
earliest_mbp AS (
    SELECT 
        np.stay_id,
        vs.mbp,
        ROW_NUMBER() OVER (PARTITION BY np.stay_id ORDER BY vs.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.vitalsign vs 
        ON np.stay_id = vs.stay_id 
        AND vs.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND mbp IS NOT NULL
),

earliest_resp_rate AS (
    SELECT 
        np.stay_id,
        vs.resp_rate,
        ROW_NUMBER() OVER (PARTITION BY np.stay_id ORDER BY vs.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.vitalsign vs 
        ON np.stay_id = vs.stay_id 
        AND vs.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND resp_rate IS NOT NULL
),
earliest_temperature AS (
    SELECT 
        np.stay_id,
        vs.temperature,
				vs.temperature_site,
        ROW_NUMBER() OVER (PARTITION BY np.stay_id ORDER BY vs.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.vitalsign vs 
        ON np.stay_id = vs.stay_id 
        AND vs.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND temperature IS NOT NULL
),
earliest_spo2 AS (
    SELECT 
        np.stay_id,
        vs.spo2,
        ROW_NUMBER() OVER (PARTITION BY np.stay_id ORDER BY vs.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.vitalsign vs 
        ON np.stay_id = vs.stay_id 
        AND vs.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND spo2 IS NOT NULL
)
SELECT 
    np.subject_id,
    np.hadm_id,
    np.stay_id,
    ehr.heart_rate,
    esbp.sbp,
    edbp.dbp,
    embp.mbp,
    eresp.resp_rate,
    etemp.temperature,
		etemp.temperature_site,
    espo2.spo2
FROM 
    public.aki_sepsis np
LEFT JOIN 
    earliest_heart_rate ehr ON np.stay_id = ehr.stay_id AND ehr.rn = 1
LEFT JOIN 
    earliest_sbp esbp ON np.stay_id = esbp.stay_id AND esbp.rn = 1
LEFT JOIN 
    earliest_dbp edbp ON np.stay_id = edbp.stay_id AND edbp.rn = 1
LEFT JOIN 
    earliest_mbp embp ON np.stay_id = embp.stay_id AND embp.rn = 1
LEFT JOIN 
    earliest_resp_rate eresp ON np.stay_id = eresp.stay_id AND eresp.rn = 1
LEFT JOIN 
    earliest_temperature etemp ON np.stay_id = etemp.stay_id AND etemp.rn = 1
LEFT JOIN 
    earliest_spo2 espo2 ON np.stay_id = espo2.stay_id AND espo2.rn = 1
ORDER BY 
    np.hadm_id, np.stay_id;
