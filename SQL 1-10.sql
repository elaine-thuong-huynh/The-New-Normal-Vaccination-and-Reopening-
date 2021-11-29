#Questions 1-10 for group project SQL 
#Seminar Class: 9
#Team: 3
use group_project;

#1.	What is the total population in Asia?
select population as total_population_in_Asia
From country_data 
Where location = "Asia";

#2.	
Select sum(distinct(population)) as total_population_in_ASEAN 
From country_data 
Where location in ("Singapore", "Malaysia", "Vietnam", "Laos", "Brunei", "Thailand",
	"Myanmar", "Philippines", "Indonesia", "Cambodia");


#3
Select distinct(source_name) as "Source Name" 
From info_source;

#4
SELECT date, total_vaccinations FROM country_vaccinations
	WHERE STR_TO_DATE(date,'%m/%d/%Y') >= "2021-03-01"
		AND STR_TO_DATE(date,'%m/%d/%Y') <= "2021-05-31"
        AND country = "Singapore"
	GROUP BY date;

#5
SELECT min(date) as First_batch_in_sg from country_vaccinations where country = "Singapore";
    
#6
select sum(new_cases_smoothed) as total_case from covid19data
where str_to_date(date, '%Y-%m-%d') >  (select  str_to_date(min(date),'%m/%d/%Y')  from country_vaccinations
where country = 'Singapore') and location = 'Singapore';


#7
SELECT date, total_cases AS Cases FROM covid19data
	WHERE location = "Singapore"
	AND date = DATE_ADD(STR_TO_DATE(
		(SELECT min(date) FROM country_vaccinations
			WHERE country = "Singapore"),'%m/%d/%Y')
    , INTERVAL -1 DAY);

 
#8
SELECT A.date AS Date, A.percentage_new_cases AS "Percentage of new cases/ %", 
IFNULL(B.vaccine, "NIL") AS Vaccines, 
IFNULL(B.percentage_total_vaccinations, 0) AS "Percentage of total vaccinations on each 
available vaccine in relation to its population/ %" 
FROM  (SELECT date, round(100*((new_cases_smoothed_per_million*(population/1000000))/population),7) 
AS percentage_new_cases FROM covid19data
JOIN country_data
ON covid19data.location = country_data.location
WHERE covid19data.location = "GERMANY"
GROUP BY date) AS A LEFT OUTER JOIN 
(SELECT date, vaccine, round(100*(C.total_vaccinations)/population,2) 
AS percentage_total_vaccinations FROM country_vaccinations_by_manufacturer AS C 
RIGHT OUTER JOIN country_data AS D 
ON C.location = D.location
WHERE D.location = "Germany") AS B ON A.date=B.date;


#9
SELECT A.location AS Location, A.date AS Date, A.new_cases_smoothed AS "New cases smoothed", IFNULL(B.vaccine, "null") AS Vaccines, 
IFNULL(D.total_vaccinations,0) AS "Total vaccinations after 20 days", 
IFNULL(C.total_vaccinations,0) AS "Total vaccinations after 30 days", 
IFNULL(B.total_vaccinations, 0) AS "Total vaccinations after 40 days" 
FROM (SELECT new_cases_smoothed, location, date FROM covid19data
WHERE location = "Germany") AS A LEFT OUTER JOIN (SELECT * FROM country_vaccinations_by_manufacturer
WHERE location = "Germany") AS B ON A.date = DATE_SUB(B.date, INTERVAL 40 DAY) 
LEFT OUTER JOIN (SELECT * FROM country_vaccinations_by_manufacturer
WHERE location = "Germany") AS C ON A.date = DATE_SUB(C.date, INTERVAL 30 DAY) AND C.vaccine = B. vaccine
LEFT OUTER JOIN (SELECT * FROM country_vaccinations_by_manufacturer
WHERE location = "Germany") AS D ON A.date = DATE_SUB(D.date, INTERVAL 20 DAY) AND D.vaccine = B.vaccine;


#10
select total_accumulated_vaccinations, A.date as date_0, new_cases_smoothed_0,
DATE_ADD(A.date, INTERVAL 21 DAY) as date_21, new_cases_smoothed_21, 
round(((new_cases_smoothed_21 - new_cases_smoothed_0)*100/new_cases_smoothed_0), 3) as rate_of_change_21,
DATE_ADD(A.date, INTERVAL 60 DAY) as date_60, new_cases_smoothed_60, 
round(((new_cases_smoothed_60 - new_cases_smoothed_0)*100/new_cases_smoothed_0), 3) as rate_of_change_60,
DATE_ADD(A.date, INTERVAL 120 DAY) as date_120, new_cases_smoothed_120, 
round(((new_cases_smoothed_120 - new_cases_smoothed_0)*100/new_cases_smoothed_0), 3) as rate_of_change_120 from 
(select date, sum(total_vaccinations) as total_accumulated_vaccinations from country_vaccinations_by_manufacturer
where location = "Germany"
group by date) as A
left outer join 
(select date, new_cases_smoothed as new_cases_smoothed_0 from covid19data
where location = "Germany") as B
on B.date = A.date 
left outer join
(select date, new_cases_smoothed as new_cases_smoothed_21 from covid19data
where location = "Germany") as C
on C.date = DATE_ADD(A.date, INTERVAL 21 DAY) 
left outer join
(select date, new_cases_smoothed as new_cases_smoothed_60 from covid19data
where location = "Germany") as D
on D.date = DATE_ADD(A.date, INTERVAL 60 DAY)
left outer join 
(select date, new_cases_smoothed as new_cases_smoothed_120 from covid19data
where location = "Germany") as E
on E.date = DATE_ADD(A.date, INTERVAL 120 DAY);