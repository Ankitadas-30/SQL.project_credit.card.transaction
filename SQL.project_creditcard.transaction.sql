/* CREDIT CARD TRANSACTION DATA EXPLORATION

Credit Card Transactions Dataset: https://www.kaggle.com/datasets/thedevastator/analyzing-credit-card-spending-habits-in-india

About the Data:
This dataset contains insights into a collection of credit card transactions made in India, offering a comprehensive look at the spending
habits of Indians across the nation. From the Gender and Card type used to carry out each transaction, to which city saw the highest 
amount of spending and even what kind of expenses were made, this dataset paints an overall picture about how money is being spent in 
India today.

Data Source :https://data.world/ash018

Column name	          Description
City	    The city in which the transaction took place. (String)
Date	    The date of the transaction. (Date)
Card Type	The type of credit card used for the transaction. (String)
Exp Type	The type of expense associated with the transaction. (String)
Gender	    The gender of the cardholder. (String)
Amount	    The amount of the transaction. (Number)

Skills used: Joins, CTE's, Subqueries, Windows Functions, Aggregate Functions, Filters, group by statement, Date Functions,
		     null handling functions, numeric functions, case expression, converting data types.

Changed the column names to lower case before importing data to sql server.Also replaced space within column names with underscore.
Changed the data types where neccessary.
*/

select * from credit_card_transactions;

--EXPLORING THE DATA

--1-different types of expense associated with the transactions
select distinct exp_type from credit_card_transactions;
--2-different cities where transactions took place
select distinct city from credit_card_transactions;
--3-different card types used in the transaction 
select distinct card_type from credit_card_transactions;

--4-time period between which the data ranges
select min(transaction_date) as min_date, max(transaction_date) as max_date from credit_card_transactions

--5-city wise total transation amount in descending order
select city,sum(amount) as total_amount from credit_card_transactions
group by city
order by total_amount desc

--6-total transaction amount for each gender and card type combination
select gender, card_type,sum(amount) as total_amount from credit_card_transactions
group by gender , card_type
order by total_amount desc

--7-year wise total transaction amount
with year_cte as(
select *, datepart(year,transaction_date) as transaction_year from credit_card_transactions)
select transaction_year,sum(cast(amount as BIGINT)) as total_amount from year_cte
group by transaction_year
order by total_amount

--8-top 5 cities with highest spends and their percentage contribution of total credit card spends 
with cte_spends as (
select city,sum(amount) as total_spend
from credit_card_transactions
group by city)
,total_spend as 
(select sum(cast(amount as bigint)) as total_amount from credit_card_transactions)
select top 5 cte_spends.*, round(total_spend*1.0/total_amount,4) * 100 as percent_contribution from 
cte_spends inner join total_spend on 1=1
order by total_spend desc

--9-highest spend month and amount spent in that month for each card type
with cte as
(select card_type, datepart(year,transaction_date) as transaction_year,datepart(month,transaction_date) as transaction_month,
sum(amount) as total_spend from credit_card_transactions
group by card_type, datepart(year,transaction_date),datepart(month,transaction_date)), cte_rn as
(select *,rank() over(partition by card_type order by total_spend) as rn from cte)
select card_type,transaction_year, transaction_month,total_spend from cte_rn 
where rn=1
order by total_spend;

--10-the transaction details(all columns from the table) for each card type when it reaches a cumulative of 1,000,000 total spends
with cte as(
select *, sum(amount) over(partition by card_type order by transaction_date,transaction_id) as running_total
from credit_card_transactions)
select * from (select *, rank() over (partition by card_type order by running_total) as rn from cte
where running_total>=1000000) a
where rn=1;

--11-city which had lowest percentage spend for gold card type
with cte as (
select city,card_type,sum(amount) as amount,sum(case when card_type='Gold' then amount end) as gold_amount
from credit_card_transactions
group by city,card_type)
select top 1 city,sum(gold_amount)*1.0/sum(amount)*100 as gold_percent
from cte
group by city
having sum(gold_amount) is not null 
order by gold_percent;

--12-print 3 columns:  city, highest_expense_type , lowest_expense_type (example format : Delhi , bills, Fuel)
with cte as (
select city,exp_type, sum(amount) as total_amount from credit_card_transactions
group by city,exp_type)
select city , max(case when rn_asc=1 then exp_type end) as lowest_exp_type,
min(case when rn_desc=1 then exp_type end) as highest_exp_type
from (select *,rank() over(partition by city order by total_amount desc) rn_desc
,rank() over(partition by city order by total_amount asc) rn_asc
from cte) A
group by city;

--13-percentage contribution of spends by females for each expense type
select exp_type,sum(case when gender='F' then amount else 0 end )*1.0/sum(amount) *100 as percent_female_contributn
from credit_card_transactions
group by exp_type
order by percent_female_contributn desc

--14-card and expense type combination that shows highest month over month growth in Jan-2014
with cte as 
(select card_type,exp_type, datepart(month,transaction_date) as mt,datepart(year, transaction_date) as yt,sum(amount) as total_spend
from credit_card_transactions
group by card_type,exp_type, datepart(month,transaction_date) ,datepart(year, transaction_date)
), cte_lag as
(select *, lag(total_spend,1) over (partition by card_type,exp_type order by yt, mt) as previous_month_spend from cte)
select top 1 card_type, exp_type,yt,mt,total_spend-previous_month_spend as month_growth from cte_lag
where previous_month_spend is not null and yt =2014 and mt=1
order by month_growth desc;

--15-during weekends the city that has highest total spend to total no of transcations ratio 
select top 1 city, sum(amount)/count(transaction_id) as ratio 
from credit_card_transactions
where DATENAME(weekday,transaction_date) in ('Saturday','Sunday') 
group by city
order by ratio desc

--16-city that took least number of days to reach its 500th transaction after the first transaction in that city
with cte as 
(select *, ROW_NUMBER() over (partition by city order by transaction_date,transaction_id) as rn
from credit_card_transactions)
select top 1 city,min(transaction_date) as min_date, max(transaction_date) as max_date,
datediff(day,min(transaction_date), max(transaction_date)) as day_diff
from cte
where rn=1 or rn=500
group by city
having count(1)=2
order by day_diff


