/*
--查询各类药物的itemid
SELECT 
    itemid,
    label
FROM 
    d_items
WHERE 
    LOWER(label) LIKE '%propofol%'               -- 丙泊酚
    OR LOWER(label) LIKE '%midazolam%'           -- 咪达唑仑
    OR LOWER(label) LIKE '%fentanyl%'            -- 芬太尼
    OR LOWER(label) LIKE '%methylprednisolone%'  -- 甲泼尼龙
    OR LOWER(label) LIKE '%prednisone%'          -- 泼尼松
    OR LOWER(label) LIKE '%dexamethasone%'       -- 地塞米松
    OR LOWER(label) LIKE '%hydrocortisone%'      -- 氢化可的松
    OR LOWER(label) LIKE '%betamethasone%'       -- 倍他米松
    OR LOWER(label) LIKE '%triamcinolone%'       -- 曲安奈德
    OR LOWER(label) LIKE '%ranitidine%'          -- 雷尼替丁
    OR LOWER(label) LIKE '%famotidine%'          -- 法莫替丁
    OR LOWER(label) LIKE '%cimetidine%'          -- 西咪替丁
    OR LOWER(label) LIKE '%nizatidine%'          -- 尼扎替丁
    OR LOWER(label) LIKE '%heparin%'             -- 肝素钠注射液
    OR LOWER(label) LIKE '%furosemide%'          -- 呋塞米
    OR LOWER(label) LIKE '%hydrochlorothiazide%' -- 氢氯噻嗪
    OR LOWER(label) LIKE '%torsemide%'           -- 替加利尿素
    OR LOWER(label) LIKE '%bumetanide%'          -- 布美他尼
    OR LOWER(label) LIKE '%spironolactone%'      -- 螺内酯
    OR LOWER(label) LIKE '%acetazolamide%'       -- 乙酰唑胺
    OR LOWER(label) LIKE '%amiloride%'           -- 氨苯蝶啶
ORDER BY 
    itemid;
以下是按您的要求将 `itemid` 和 `label` 按镇静剂、类固醇、H2拮抗剂、肝素、利尿剂分类后的归纳结果：

### 镇静剂
| itemid | label                       |
|--------|------------------------------|
| 221668 | Midazolam (Versed)           |
| 222168 | Propofol                     |
| 226224 | Propofol Ingredient          |
| 227210 | Propofol (Intubation)        |

### 镇痛剂
| itemid | label                       |
|--------|------------------------------|
| 221744 | Fentanyl                    |
| 225942 | Fentanyl (Concentrate)      |
| 225972 | Fentanyl (Push)             |

### H2拮抗剂
| itemid | label                        |
|--------|-------------------------------|
| 222190 | Ranitidine                   |
| 225907 | Famotidine (Pepcid)          |
| 225911 | Ranitidine (Prophylaxis)     |

### 肝素
| itemid | label                           |
|--------|----------------------------------|
| 224145 | Heparin Dose (per hour)          |
| 225152 | Heparin Sodium                   |
| 225958 | Heparin Concentration (units/mL) |
| 225975 | Heparin Sodium (Prophylaxis)     |
| 229373 | UF Heparin (Anti-Xa)_U_1         |
| 229375 | UF Heparin (Anti-Xa)             |
| 229538 | PICC - Heparin Dependent         |
| 229597 | Heparin Sodium (Impella)         |
| 230044 | Heparin Sodium (CRRT-Prefilter)  |

### 利尿剂
| itemid | label                       |
|--------|------------------------------|
| 221794 | Furosemide (Lasix)           |
| 228340 | Furosemide (Lasix) 250/50    |
| 229639 | Bumetanide (Bumex)           |
*/

WITH medication_flags AS (
    SELECT 
        ak.stay_id,
        MAX(CASE WHEN ie.itemid IN (221668, 222168, 226224, 227210) THEN 1 ELSE 0 END) AS sedative,   -- 镇静剂
        MAX(CASE WHEN ie.itemid IN (221744, 225942, 225972) THEN 1 ELSE 0 END) AS analgesic,            -- 镇痛剂
        MAX(CASE WHEN ie.itemid IN (222190, 225907, 225911) THEN 1 ELSE 0 END) AS h2_antagonist,       -- H2拮抗剂
        MAX(CASE WHEN ie.itemid IN (224145, 225152, 225958, 225975, 229373, 229375, 229538, 229597, 230044) THEN 1 ELSE 0 END) AS heparin, -- 肝素
        MAX(CASE WHEN ie.itemid IN (221794, 228340, 229639) THEN 1 ELSE 0 END) AS diuretic             -- 利尿剂
    FROM 
        public.aki_sepsis ak
    LEFT JOIN 
        mimic_icu.inputevents ie ON ak.stay_id = ie.stay_id
    GROUP BY 
        ak.stay_id
),
vasoactive_flags AS (
    SELECT 
        stay_id,
        1 AS vasoactive_agent
    FROM 
        public.vasoactive_agent
    GROUP BY 
        stay_id
),
antibiotic_flags AS (
    SELECT 
        stay_id,
        1 AS antibiotic
    FROM 
        public.antibiotic
    GROUP BY 
        stay_id
)
SELECT 
    ak.subject_id,
    ak.hadm_id,
    ak.stay_id,
    COALESCE(mf.sedative, 0) AS sedative,            -- 镇静剂列
    COALESCE(mf.analgesic, 0) AS analgesic,           -- 镇痛剂列
    COALESCE(mf.h2_antagonist, 0) AS h2_antagonist,   -- H2拮抗剂列
    COALESCE(mf.heparin, 0) AS heparin,               -- 肝素列
    COALESCE(mf.diuretic, 0) AS diuretic,             -- 利尿剂列
    COALESCE(vf.vasoactive_agent, 0) AS vasoactive_agent,  -- 血管活性剂列
    COALESCE(af.antibiotic, 0) AS antibiotic               -- 抗生素列
FROM 
    public.aki_sepsis ak
LEFT JOIN 
    medication_flags mf ON ak.stay_id = mf.stay_id
LEFT JOIN 
    vasoactive_flags vf ON ak.stay_id = vf.stay_id
LEFT JOIN 
    antibiotic_flags af ON ak.stay_id = af.stay_id
ORDER BY 
    ak.hadm_id, ak.stay_id;
