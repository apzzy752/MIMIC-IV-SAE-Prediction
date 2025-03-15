WITH comorbidities AS (
    SELECT 
        d.hadm_id,
				-- 根据icd编码查询同一次住院是否诊断并发症
        MAX(CASE WHEN d.icd_code LIKE 'I10%' OR d.icd_code LIKE 'I11%' OR d.icd_code LIKE 'I12%' OR d.icd_code LIKE 'I13%' OR d.icd_code LIKE 'I15%' OR d.icd_code LIKE '401%' OR d.icd_code LIKE '402%' OR d.icd_code LIKE '403%' OR d.icd_code LIKE '404%' OR d.icd_code LIKE '405%' THEN 1 ELSE 0 END) AS hypertension,
				MAX(CASE 
        WHEN d.icd_code LIKE '250%'  
          OR d.icd_code LIKE 'E10%'   
          OR d.icd_code LIKE 'E11%'   
          OR d.icd_code LIKE 'E13%'   
					OR d.icd_code LIKE 'E14%'  
        THEN 1 
        ELSE 0 
    END) AS diabetes,
				MAX(CASE WHEN d.icd_code IN ('4660', '490', '4910', '4911', '49120', '49121', '4918', '4919', '4920', '4928', '494', '4940', '4941', '496') OR d.icd_code LIKE 'J44%' THEN 1 ELSE 0 END) AS copd,
				MAX(CASE WHEN d.icd_code IN ('5851', '5852', '5853', '5854', '5855', '5856', '5859') 
         OR d.icd_code IN ('N18', 'N181', 'N182', 'N183', 'N184', 'N185', 'N186', 'N189') 
    THEN 1 ELSE 0 END) AS ckd,
				
				MAX(CASE WHEN 
        (d.icd_code >= '390' AND d.icd_code <= '459') OR 
        (d.icd_code >= 'I00' AND d.icd_code <= 'I99') 
    THEN 1
    ELSE 0 
END) AS cardiovascular_disease


    FROM 
        mimic_hosp.diagnoses_icd d
    GROUP BY 
        d.hadm_id
)

SELECT
  p.subject_id, p.hadm_id, p.stay_id,
	c.hypertension,
	c.diabetes,
	c.copd,
	c.ckd,
	c.cardiovascular_disease
	
	FROM
  public.aki_sepsis p
	LEFT JOIN comorbidities c ON p.hadm_id = c.hadm_id
	ORDER BY p.hadm_id, p.stay_id;