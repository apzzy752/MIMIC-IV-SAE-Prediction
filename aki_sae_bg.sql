WITH earliest_ph AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        bg.ph,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY bg.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.bg bg 
        ON np.hadm_id = bg.hadm_id 
        AND bg.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND bg.ph IS NOT NULL
        AND bg.specimen_pred = 'ART.'
),
earliest_pco2 AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        bg.pco2,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY bg.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.bg bg 
        ON np.hadm_id = bg.hadm_id 
        AND bg.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND bg.pco2 IS NOT NULL
        AND bg.specimen_pred = 'ART.'
),
earliest_bicarbonate AS (
    SELECT 
        np.hadm_id,
        np.stay_id,
        bg.bicarbonate,
        ROW_NUMBER() OVER (PARTITION BY np.hadm_id, np.stay_id ORDER BY bg.charttime ASC) AS rn
    FROM 
        public.aki_sepsis np
    LEFT JOIN 
        public.bg bg 
        ON np.hadm_id = bg.hadm_id 
        AND bg.charttime BETWEEN np.icu_intime AND np.icu_intime + INTERVAL '24 hours'
        AND bg.bicarbonate IS NOT NULL
        AND bg.specimen_pred = 'ART.'
)
SELECT 
    np.subject_id,
    np.hadm_id,
    np.stay_id,
    eph.ph AS ph,
    epco2.pco2 AS pco2,
    ebicar.bicarbonate AS bicarbonate
FROM 
    public.aki_sepsis np
LEFT JOIN 
    earliest_ph eph ON np.hadm_id = eph.hadm_id AND np.stay_id = eph.stay_id AND eph.rn = 1
LEFT JOIN 
    earliest_pco2 epco2 ON np.hadm_id = epco2.hadm_id AND np.stay_id = epco2.stay_id AND epco2.rn = 1
LEFT JOIN 
    earliest_bicarbonate ebicar ON np.hadm_id = ebicar.hadm_id AND np.stay_id = ebicar.stay_id AND ebicar.rn = 1
ORDER BY 
    np.hadm_id, np.stay_id;
