Use project_layoff;

SELECT * FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging
select * FROM layoffs;

-- Let's Remove Duplicates

# First let's check for duplicates

SELECT company, location, COUNT(*)
FROM layoffs_staging
GROUP BY company, location
HAVING count(*) > 1;

SELECT * FROM layoffs_staging
WHERE company = 'evernote';

SELECT *, 
	ROW_NUMBER() OVER(partition by company, industry, total_laid_off,`date`
			) AS row_num
FROM layoffs_staging;

SELECT * 
FROM (
	SELECT *, 
	ROW_NUMBER() OVER(partition by company, industry, total_laid_off,`date`) AS row_num
	FROM layoffs_staging
    ) duplicates
WHERE row_num > 1;

-- it looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

-- these are our real duplicates 

WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging
	) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE;

-- from previous query we found out delete is treated as updation command and can't update CTE
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging;
        
DELETE FROM layoffs_staging2
WHERE row_num > 1;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Standardizing the data

SELECT * 
FROM layoffs_staging2;

-- removing whitespaces from company names
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- if we look at industry, crypto, cryto currency and cyptocurrency are the same thing
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT t1.industry, t2.industry 
FROM layoffs_staging2 t1
JOIN layoffs_staging t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Final outcome after data cleaning
SELECT * 
FROM layoffs_staging2;

-- Now lets do Exploratory Data Analysis - explore the data and find trends or patterns

SELECT MAX(total_laid_off)
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

-- 1 means 100% of the conmpany is laid off
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- companies with the biggest single Layoff
SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging
ORDER BY 2 DESC;

-- compnaies with most total layoffs
SELECT company, SUM(total_laid_off) AS sum_layoffs
FROM layoffs_staging2
GROUP BY company
ORDER BY sum_layoffs DESC;

-- industries with most total layoffs
SELECT industry, SUM(total_laid_off) AS sum_layoffs
FROM layoffs_staging2
GROUP BY industry
ORDER BY sum_layoffs DESC;

-- countries with most total layoffs
SELECT country, SUM(total_laid_off) AS sum_layoffs
FROM layoffs_staging2
GROUP BY country
ORDER BY sum_layoffs DESC;

SELECT location, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY location
ORDER BY 2 DESC;

-- this it total in the past 3 years or in the dataset
SELECT YEAR(`date`) as years, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as month_dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY month_dates
ORDER BY month_dates;

-- now use it in a CTE 
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as month_dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
WHERE SUBSTRING(date,1,7) IS NOT NULL
GROUP BY month_dates
ORDER BY month_dates
)
SELECT month_dates, SUM(total_laid_off) OVER (ORDER BY month_dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY month_dates;


