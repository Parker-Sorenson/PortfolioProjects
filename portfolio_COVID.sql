/*
COVID 19 Data Exploration

Skills used: Joins, CTE, Temp Table, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types
*/

# Ensure raw data imported correctly
	
    SELECT *
	FROM coviddeaths
	WHERE continent <> ''
	ORDER BY 3, 4;


#Create staging data sets
	
    CREATE TABLE coviddeaths_staging
	LIKE coviddeaths;

	INSERT coviddeaths_staging
	SELECT *
	FROM coviddeaths;

	CREATE TABLE covidvaccinations_staging
	LIKE covidvaccinations;

	INSERT covidvaccinations_staging
	SELECT *
	FROM covidvaccinations;


#Modify 'date' column and update type to simplify analysis

	UPDATE coviddeaths_staging
	SET date = STR_TO_DATE(date, '%m/%d/%Y');

	UPDATE covidvaccinations_staging
	SET date = STR_TO_DATE(date, '%m/%d/%Y');

	ALTER TABLE coviddeaths_staging
	MODIFY COLUMN date DATE;

	ALTER TABLE covidvaccinations_staging
	MODIFY COLUMN date DATE;


# Select starting data

	SELECT location, date, total_cases, new_cases, total_deaths, population
	FROM coviddeaths_staging
	WHERE continent <> ''
	ORDER BY 1, 2;


# Total cases vs. total deaths
## Shows likelihood of dying from COVID if contracted in USA

	SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS deathPercentage
	FROM coviddeaths_staging
	WHERE location LIKE '%states%'
	AND continent <> ''
	ORDER BY 1, 2;


#Total cases vs. population
##Shows percent of the population with COVID

	SELECT location, date, total_cases, population, (total_cases/population)*100 AS infectionPercent
	FROM coviddeaths_staging
	#WHERE location LIKE '%states%'
	ORDER BY 1, 2;


#Countries with highest infection rate compared to population

	SELECT location, MAX(total_cases) AS highest_infectionCount, population, 
		MAX((total_cases/population))*100 AS maxInfectionPercent
	FROM coviddeaths_staging
	GROUP BY location, population
	ORDER BY maxInfectionPercent DESC;


# Countries with highest death rate compared to population

	SELECT location, max(CAST(total_deaths AS UNSIGNED)) AS totalDeathCount
	FROM coviddeaths_staging
	WHERE continent = ''
	GROUP BY location
	ORDER BY totalDeathCount DESC;


#Breakdown by Continent

	SELECT continent, MAX(CAST(total_deaths AS UNSIGNED)) AS TotalDeathCount
	FROM coviddeaths_staging
	#Where location like '%states%'
	WHERE continent <> ''
	GROUP BY continent
	ORDER BY TotalDeathCount DESC


#GLOBAL NUMBERS
##Number of new cases and deaths globally	

	SELECT SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, 
		(SUM(new_deaths)/SUM(new_cases))*100 AS deathPercent 
		#total_cases, total_deaths, (total_deaths/total_cases)*100 AS deathPercent
	FROM coviddeaths_staging
	WHERE continent <> ''
	GROUP BY date
	ORDER BY 1, 2;


#vaccinations data missing a HUGE number of cells (like 60k), causing NULL's in data where there shouldn't be
#Total population vs. vaccinations
##Shows percent of population receiving at least 1 vaccine

	SELECT d.continent, d.location, d.date, d.population, v.new_vaccinations, 
		SUM(v.new_vaccinations) OVER(PARTITION BY d.location ORDER BY d.location, d.date) AS rolling_peopleVaccinated
	FROM coviddeaths_staging AS d
	LEFT JOIN covidvaccinations_staging AS v
		ON d.location = v.location
		AND d.date = v.date
	WHERE d.continent <> ''
	ORDER BY 2, 3;


# Use CTE to perform calculations on 'PARTITION BY' in previous query

	WITH popVsVac (continent, location, date, population, new_vaccinations, rolling_peopleVaccinated)
	AS (
	SELECT d.continent, d.location, d.date, d.population, v.new_vaccinations, 
		SUM(v.new_vaccinations) OVER(PARTITION BY d.location ORDER BY d.location, d.date) AS rolling_peopleVaccinated
	FROM coviddeaths_staging AS d
	LEFT JOIN covidvaccinations_staging AS v
		ON d.location = v.location
		AND d.date = v.date
	WHERE d.continent <> ''
	#ORDER BY 2, 3;
	)
	SELECT *, (rolling_peopleVaccinated/population)*100
	FROM popVsVac;


#Create View to Store Data for Later Visulization

CREATE VIEW rolling_peoplevaccinated AS
SELECT d.continent, d.location, d.date, d.population, v.new_vaccinations, 
	SUM(v.new_vaccinations) OVER(PARTITION BY d.location ORDER BY d.location, d.date) AS rolling_peopleVaccinated
FROM coviddeaths_staging AS d
LEFT JOIN covidvaccinations_staging AS v
	ON d.location = v.location
    AND d.date = v.date
WHERE d.continent <> '';