-- OPEN THE TABLE --
SELECT * FROM world_layoff.company_layoffs;
-- END --

/*Create layoff staging table*/
create table layoff_staging
like world_layoff.company_layoffs;

insert into layoff_staging
select *
from world_layoff.company_layoffs;
/*end*/

-- start the cleaning by checking duplicated columns --
/**create cte for checking duplicated columns**/
with duplicate_cte as (
select *, row_number() over(partition by
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) as row_num
from layoof_staging
)

select *
from duplicate_cte
where row_num >1;
/**end**/
/**duplicate table layoff_staging, add row_num column. Name it layoff_staging2**/
create table layoff_staging2
like layoof_staging;

alter table layoff_staging2
add column row_num int;

insert into layoff_staging2
select *, row_number() over(partition by
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) as row_num
from layoff_staging;
/**end**/
/**delete duplicated column in layoff_staging2**/
select *
from layoff_staging2
where row_num > 1;

delete
from layoff_staging2
where row_num > 1;
/**end**/
/**Remove unnecessary blank spaces at the beginning of the data**/
select *
from layoff_staging2;

update layoff_staging2
set company = trim(company), location = trim(location), industry = trim(industry), total_laid_off = trim(total_laid_off),
percentage_laid_off = trim(percentage_laid_off), `date` = trim(`date`), stage = trim(stage),
country = trim(country), funds_raised_millions = trim(funds_raised_millions);
/**end**/
/**check and update for other typo errors in the table**/
select distinct industry
from layoff_staging2
order by 1;

update layoff_staging2
set industry = "Crypto"
where industry like "crypto%";

select distinct country
from layoff_staging2
order by 1;

update layoff_staging2
set country = trim(trailing "." from country);
/**end**/
/**convert for the date format and modify the datetype**/
update layoff_staging2
set `date` = str_to_date(`date`, "%m-%d-%Y");

alter table layoff_staging2
modify column `date` date;
/**end**/
/**populating necessary blank cells**/
select *
from layoff_staging2
order by 3;

select *
from layoff_staging2 as t1
join layoff_staging2 as t2
on t1.company = t2.company
where t1.industry is null or t1.industry = ""
and t2.industry is not null
;

update layoff_staging2
set industry = null
where industry = "";

update layoff_staging2 as t1
join layoff_staging2 as t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null
;

select *
from layoff_staging2
where industry is null or industry ="";

select *
from layoff_staging2
where company = "airbnb";
/**end**/
/**optional- delete other columns with no data on total laid off and percentage laid off **/
select *
from layoff_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoff_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoff_staging2
order by 4,5;
/**end**/
/**drop column row_num**/
alter table layoff_staging2
drop column row_num;
/**end**/

-- THE TABLE IS READY!! --
