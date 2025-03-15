WITH ventilation_data AS (
    SELECT 
        ak.stay_id,
        CASE 
            WHEN v.ventilation_status IN ('Trach', 'InvasiveVent', 'NonInvasiveVent') THEN 1
            ELSE 0
        END AS vent,
        EXTRACT(EPOCH FROM (v.endtime - v.starttime)) / 3600 AS ventilation_hours -- 将时间差转换为小时
    FROM 
        public.aki_sepsis ak
    LEFT JOIN 
        public.ventilation v ON ak.stay_id = v.stay_id
    WHERE 
        v.ventilation_status IN ('Trach', 'InvasiveVent', 'NonInvasiveVent')
),
rrt_data AS (
    SELECT 
        ak.stay_id,
        CASE 
            WHEN pe.itemid IN (224270, 225436) THEN 1
            ELSE 0
        END AS rrt,
        EXTRACT(EPOCH FROM (pe.endtime - pe.starttime)) / 3600 AS rrt_hours -- 将时间差转换为小时
    FROM 
        public.aki_sepsis ak
    LEFT JOIN 
        mimic_icu.procedureevents pe ON ak.stay_id = pe.stay_id
    WHERE 
        pe.itemid IN (224270, 225436)
)
SELECT 
    ak.subject_id,
    ak.hadm_id,
    ak.stay_id,
    CASE WHEN SUM(vd.vent) > 0 THEN 1 ELSE 0 END AS vent, -- 通气标记：至少有一条符合条件记录则输出1，否则0
    COALESCE(SUM(vd.ventilation_hours), 0) AS total_ventilation_hours, -- 通气总时长（小时）
    CASE WHEN SUM(rd.rrt) > 0 THEN 1 ELSE 0 END AS rrt, -- RRT标记：至少有一条符合条件记录则输出1，否则0
    COALESCE(SUM(rd.rrt_hours), 0) AS total_rrt_hours -- RRT总时长（小时）
FROM 
    public.aki_sepsis ak
LEFT JOIN 
    ventilation_data vd ON ak.stay_id = vd.stay_id
LEFT JOIN 
    rrt_data rd ON ak.stay_id = rd.stay_id
GROUP BY 
    ak.subject_id, ak.hadm_id, ak.stay_id
ORDER BY 
    ak.hadm_id, ak.stay_id;
