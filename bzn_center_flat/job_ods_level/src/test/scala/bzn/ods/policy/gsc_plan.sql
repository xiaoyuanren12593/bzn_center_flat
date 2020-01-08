-- gsc
SELECT c.insurance_policy_no,c.policy_no,c.holder_name,a.plan_name,
case d.profession_type when '1' then '1-3类'
	when '2' then '1-4类'
	when '3' then '5类'
	else '未知'
	end as profession_type,
case d.join_social
	when '1' then 'Y'
	when '0' then 'N'
	end as join_social,
cast(d.plan_type as CHAR) as plan_type,
d.policy_category as deadline_type,
cast(d.status as char) as status,
now() as create_time
from bzn_central.b_policy_product_plan a
LEFT JOIN bzn_business.dict_china_life_plan b on b.plan_code=SUBSTRING_INDEX(a.plan_detail_code,',',-1)
LEFT JOIN bzn_business.dict_china_life_plan_group d on d.group_code=b.group_code
LEFT JOIN bzn_central.b_policy c on c.policy_no=a.policy_no
where d.profession_type is not null or d.join_social is NOT NULL;

-- zh
SELECT c.insurance_policy_no,c.policy_no,c.holder_name,a.plan_name,
case b.profession_type  when 'K1' then '1-2类'
when 'K2' then '1-3类' when 'K3' then '1-4类' when 'K4' then '5类' end as profession_type,
b.society_scale,
b.deadline_type,
b.service_charge,
b.dead_amount,
b.is_medical,
b.medical_amount,
b.medical_percent,
b.is_delay,
b.delay_amount,
b.delay_percent,
b.delay_days,
b.is_hospital,
b.hospital_amount,
b.hospital_days,
b.hospital_total_days,
b.disability_scale,
b.society_num_scale,
b.compensate_scale,
b.extend_24hour,
b.extend_hospital,
b.extend_job_Injury,
b.extend_overseas,
b.extend_self_charge_medicine,
b.extend_three,
b.extend_three_item,
b.sales_model,
b.sales_model_percent,
b.commission_percent,
b.min_price,
b.premium,
b.status,
b.create_time,
b.create_user_id,
b.create_user_name,
b.update_time
from bzn_central.b_policy_product_plan a
LEFT JOIN bzn_business.dict_zh_plan b on b.plan_code=SUBSTRING_INDEX(a.plan_detail_code,',',-1)
LEFT JOIN bzn_central.b_policy c on c.policy_no=a.policy_no
where b.profession_type is NOT NULL OR b.society_scale is NOT NULL;