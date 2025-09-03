show databases;

use health_risk;

select *
from health_risk_dataset
where systolic_BP>=120;

create table health_risk_dataset2
select *
from health_risk_dataset;

select *
from health_risk_dataset2;


	
    select distinct Patient_id, respiratory_rate, oxygen_saturation, consciousness
   from  health_risk_dataset2;
    
    select *
     from health_risk_dataset2
     order by patient_id desc
     limit 3;
     
      select *
     from health_risk_dataset2
     order by systolic_BP desc
     limit 3;
     
     select systolic_BP, Avg(systolic_BP) AS avg_sys
     from   health_risk_dataset2
     group by systolic_BP;
	
	 select *
     from health_risk_dataset2
     where oxygen_saturation<92;
     -- count number of the risk level--
     select risk_level, count(risk_level) AS count
     from  health_risk_dataset2
     Group by risk_level
	order by count DESC;
    
    -- Comparing Temperature_level of various patient_id with risk_level --
    with temp_risk_factor AS
	 (select patient_id, risk_level, temperature
      from health_risk_dataset2
	   group by patient_id, risk_level, temperature
      order by temperature desc)
      select *
      from temp_risk_factor;
     
    -- classifying patients vitals--
		select patient_id, oxygen_saturation, o2_scale, consciousness, risk_level, Heart_Rate,
       case 
		when oxygen_saturation<96 then 1 else 0 
		End, 
		case 
        when o2_scale>1 then 1 else 0 
		End,
		case 
        when systolic_BP<90 then 1 else 0 
		End, 
		case 
        when consciousness!= 'Alert' then 1 else 0 
		End as risk_score
        from health_risk_dataset2;
        
        -- classifying Patients_id by comparing to systolic_BP --
			
            select patient_id, systolic_BP, 
			case when systolic_BP >= 96 then 0 End,
			case when systolic_BP = 95 or 94 then 1 End,
			case when systolic_BP <94 then 2 End AS new_score
	       from health_risk_dataset2; 

            -- min(avg) of systolic_bp
            select systolic_BP, avg(systolic_BP), min(systolic_BP), max(systolic_BP), count(systolic_BP)
           from health_risk_dataset2
           group by Systolic_BP
           order by systolic_BP desc;

        
		        
	
	 
     
















