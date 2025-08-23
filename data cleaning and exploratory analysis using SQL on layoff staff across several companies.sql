select *
from layoffs;

-- 1 Remove Duplicates
-- 2 standardize the Data
-- 3 Null values 0r BanK values
-- 4 Remove any columns


create table layoffs_staging
select *
from layoffs;

select *,
Row_Number() over(partition by company, location, total_laid_off, 'date') AS row_num
from layoffs_staging;

with duplicate_CTE AS
(
select *,
Row_Number() over(partition by company, location, total_laid_off, 'date', stage, funds_raised, country, date_added) AS row_num
from layoffs_staging)
select *
from duplicate_CTE
where row_num>1;

with duplicate_CTE AS
(
select *,
Row_Number() over(partition by company, location, total_laid_off, 'date', stage, funds_raised, country, date_added) AS row_num
from layoffs_staging)
select *
from duplicate_CTE
where row_num>1;



create table layoffs_staging2
select *
from layoffs;



with duplicate_CTE2 AS
( select *,
Row_Number() over(partition by company, location, total_laid_off, 'date', stage, funds_raised, country, date_added) AS row_num
from layoffs_staging2)
select company, trim(company) 
from duplicate_CTE2
;




CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `total_laid_off` text,
  `date` text,
  `percentage_laid_off` text,
  `industry` text,
  `source` text,
  `stage` text,
  `funds_raised` text,
  `country` text,
  `date_added` text,
  `Row_num` Int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;




insert into layoffs_staging2
 select *,
Row_Number() over(partition by company, location, total_laid_off, 'date', stage, funds_raised, country, date_added) AS row_num
from layoffs_staging;

select *
from layoffs_staging2
where Row_num>1;



-- Standization


select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select *
from layoffs_staging2
where industry like'crypto%';

select Distinct industry
from layoffs_staging2;

update layoffs_staging2
set industry = 'crypto'
where industry like 'crypro%';

select distinct country, trim(country)
from layoffs_staging2;

Select `date`
from layoffs_staging2;

update layoffs_staging2
set `date`= str_to_date(`date`,'%m/%d/%Y');

Alter table layoffs_staging2
Modify column `date` DATE;

select *  
from layoffs_staging2
where total_laid_off = ''
AND percentage_laid_off = '';

select *
from layoffs_staging2
where industry is Null
or industry= '';

select *
from layoffs_staging2
where company = 'Balleys';

select T1.industry, T2.industry
from layoffs_staging2 as T1
Join layoffs_staging2 as T2
on T1.company = T2.company
where (T1.industry is Null or T1.industry = '')
And T2.industry is not Null or T2.industry != '';


update layoffs_staging2 T1
Join layoffs_staging2 as T2
on T1.company = T2.company
set T1.industry = T2.industry
where (T1.industry is Null)
And T2.industry is not Null;


select *  
from layoffs_staging2
where total_laid_off = ''
And percentage_laid_off = '';

update layoffs_staging2
set percentage_laid_off = Null
where percentage_laid_off = '';

Alter table layoffs_staging2
drop column Row_num;







-- Exploratory data analysis


SELECT *  
FROM  layoffs_staging2
;

SELECT max(total_laid_off), max(percentage_laid_off)
FROM  layoffs_staging2;

SELECT *  
FROM  layoffs_staging2
where percentage_laid_off <= 98
order by total_laid_off DESC;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 DESC;

select min(`date`), max(`date`) 
from layoffs_staging2;

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 DESC;

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 DESC;


select substring(`date`,6,7) AS `month`, sum(total_laid_off) 
from layoffs_staging2
group by `month`
;

select substring(`date`,1,7) AS `month`, sum(total_laid_off) 
from layoffs_staging2
group by `month`
order by 1 ASc;

with rolling_total AS
( select substring(`date`,1,7) AS `month`, sum(total_laid_off) AS total_off
from layoffs_staging2
group by `month`
order by 1 ASc
)
select `month`, total_off, sum(total_off)  over(order by `month`) AS rolling_total
from rolling_total;


select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
order by 3 DESC;

with company_year (company, years, total_laid_off) AS
( select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
order by 3 DESC
), company_year_rank AS
(select*, dense_rank() over(partition by years order by total_laid_off desc) AS ranking
from company_year
where years is not null
)
select *
from company_year_rank
where ranking<=5;

