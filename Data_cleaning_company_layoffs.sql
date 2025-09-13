/*Open Table*/

SELECT * FROM world_layoffs.company_layoffs;

/*end*/
/*Dupicate table for staging*/

Create table layoffs_staging
like world_layoffs.company_layoffs;

insert layoffs_staging
select *
from company_layoffs;

select *
from layoffs_staging;

/*end*/
/*check duplicated data on the layoffs_staging table using CTE*/

with duplicate_CTE as ( select *, row_number() over(partition by
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) as row_num
from layoffs_staging)

select *
from duplicate_CTE
where row_num > 1;

/*end*/
/*create layoffs_staging2 table and add row_num collumn to delete the duplicated data.*/

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


insert layoffs_staging2
select *, row_number() over(partition by
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
)
from layoffs_staging;


select *
from layoffs_staging2
where row_num > 1;

delete
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2;

/*end*/
/*Standardize table, check collumns and rows*/
/*checking company column*/

/*removing spaces*/
select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select company
from layoffs_staging2;

select location, trim(location)
from layoffs_staging2;

update layoffs_staging2
set location = trim(location);

select *
from layoffs_staging2;

select industry, trim(industry)
from layoffs_staging2;

update layoffs_staging2
set industry = trim(industry);

select *
from layoffs_staging2;

select total_laid_off, trim(total_laid_off)
from layoffs_staging2;

update layoffs_staging2
set total_laid_off = trim(total_laid_off);

select *
from layoffs_staging2;

select percentage_laid_off, trim(percentage_laid_off)
from layoffs_staging2;

update layoffs_staging2
set percentage_laid_off = trim(percentage_laid_off);

select *
from layoffs_staging2;

select `date`, trim(`date`)
from layoffs_staging2;

update layoffs_staging2
set `date` = trim(`date`);

select *
from layoffs_staging2;

select stage, trim(stage)
from layoffs_staging2;

update layoffs_staging2
set stage = trim(stage);

select *
from layoffs_staging2;

select country, trim(country)
from layoffs_staging2;

update layoffs_staging2
set country = trim(country);

select *
from layoffs_staging2;

select funds_raised_millions, trim(funds_raised_millions)
from layoffs_staging2;

update layoffs_staging2
set funds_raised_millions = trim(funds_raised_millions);

select *
from layoffs_staging2;
/*end*/
/*checking location row*/

select distinct location
from layoffs_staging2
order by 1;

/*end*/
/*checking industry column*/

select distinct industry
from layoffs_staging2
order by 1;

select distinct industry
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'Crypto Currency'
where industry like 'crypto%';

select distinct industry
from layoffs_staging2
order by 1;

/*end*/
/*change date format and variable type*/

select *
from layoffs_staging2;

select `date`, str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date`
from layoffs_staging2;

alter table layoffs_staging2
modify column `date` date;

/*end*/
/*check country column*/

select *
from layoffs_staging2;

select distinct country
from layoffs_staging2
order by 1;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country);

/*end*/
/*populate industry*/

select *
from layoffs_staging2
where industry is null or industry = ''
order by 1;

select *
from layoffs_staging2
where company = 'Airbnb'
order by 3;

select *
from layoffs_staging2 as l1
join layoffs_staging2 as l2
on l1.company = l1.company
where (l1.industry is null or l1.industry = '')
and l2.industry is not null
order by 3;

update layoffs_staging2
set industry = null
where industry = '';


update layoffs_staging2 as l1
join layoffs_staging2 as l2
on l1.company = l1.company
set l1.industry = l2.industry
where (l1.industry is null or l1.industry = '')
and l2.industry is not null;

select distinct industry
from layoffs_staging2
order by 1;

/**/
/*remove row_num column*/

select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;

/*Now this file is ready for exploration*/
/*end*/

