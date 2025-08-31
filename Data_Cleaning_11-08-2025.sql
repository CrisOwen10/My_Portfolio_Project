
-- select table layoffs
select * from layoffs;

-- create table layoffs_staging
create table layoffs_staging
like layoffs;

insert layoffs_staging
select * from layoffs;


-- select table layoffs_staging
select * from layoffs_staging;

-- check duplicates
select *, row_number() over(partition by
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

with duplicate_cte as (
select *, row_number() over(partition by
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging)

select * from duplicate_cte
where row_num >1;


-- create table layoffs_staging2 and add row_num column
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

-- select table layoffs_staging2
select *
from layoffs_staging2;

-- populate layoffs_staging2 table
insert layoffs_staging2
select *,  row_number() over(partition by
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

-- check duplicates
select *
from layoffs_staging2
where row_num > 1;

-- delete duplicates
delete
from layoffs_staging2
where row_num > 1;

-- select table layoffs_staging2
select *
from layoffs_staging2;

-- CLEANING THE TABLE
-- trim blank spaces before the chaaracters on each column
select company , trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select *
from layoffs_staging2;

select location , trim(location)
from layoffs_staging2;

update layoffs_staging2
set location = trim(location);

select *
from layoffs_staging2;

select industry , trim(industry)
from layoffs_staging2;

update layoffs_staging2
set industry = trim(industry);

select *
from layoffs_staging2;

select total_laid_off , trim(total_laid_off)
from layoffs_staging2;

update layoffs_staging2
set total_laid_off = trim(total_laid_off);

select *
from layoffs_staging2;

select percentage_laid_off , trim(percentage_laid_off)
from layoffs_staging2;

update layoffs_staging2
set percentage_laid_off = trim(percentage_laid_off);

select *
from layoffs_staging2;

select `date` , trim(`date`)
from layoffs_staging2;

update layoffs_staging2
set `date` = trim(`date`);

select *
from layoffs_staging2;

select stage , trim(stage)
from layoffs_staging2;

update layoffs_staging2
set stage = trim(stage);

select *
from layoffs_staging2;

select country , trim(country)
from layoffs_staging2;

update layoffs_staging2
set country = trim(country);

select *
from layoffs_staging2;

select funds_raised_millions , trim(funds_raised_millions)
from layoffs_staging2;

update layoffs_staging2
set funds_raised_millions = trim(funds_raised_millions);

-- check and update industry column
select distinct industry
from layoffs_staging2
order by 1;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'crypto%';

-- check country column
select distinct country
from layoffs_staging
order by 1;

-- remove excess character from the column country
update layoffs_staging
set country = trim(trailing '.' from country);

-- format and update the date
select `date` , str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2
order by 1;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select *
from layoffs_staging2
order by 1;

-- change date value
alter table layoffs_staging2
modify column `date` date;


-- populate the blank value in industry column
select *
from layoffs_staging2
where company = 'Airbnb'
order by 3;

select *
from layoffs_staging2 as t1
join layoffs_staging2 as t2
 on t1.company = t2.company
 where (t1.industry is null or t1.industry = '')
 and t2.industry is not null;
 
 update layoffs_staging2
 set industry = null
 where industry = '';

 update layoffs_staging2 as t1
join layoffs_staging2 as t2
 on t1.company = t2.company
 set t1.industry = t2.industry
 where (t1.industry is null or t1.industry = '')
 and t2.industry is not null;
 
 
 -- removing rows with null value in total_laid_off and percentage_laid_off
 select *
 from layoffs_staging2
 order by 1;
 
select *
 from layoffs_staging2
 where total_laid_off is null
 and percentage_laid_off is null
 ;
 
delete
 from layoffs_staging2
 where total_laid_off is null
 and percentage_laid_off is null
 ;
 
 -- drop the row_num column
 alter table layoffs_staging2
 drop column row_num;
 
 -- DONE CLEANING
 select *
 from layoffs_staging2;
 
     -- Export data for "Tab 1"
    SELECT *
    FROM your_table_1
    INTO OUTFILE '/path/to/your/file1.csv'
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n';

    -- Export data for "Tab 2"
    SELECT *
    FROM layoffs
    INTO OUTFILE 'C:\Users\THEIA_TIMOTHY\Documents\MySQL Roadmap to Data Analyst/layoffs_stagging2.csv'
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n';
    
    show variables like "secure_file_priv"