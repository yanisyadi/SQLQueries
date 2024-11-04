/*
   ** Exploitation of a csv dataset in order to answer some statistical questions by using a total of 12 queries
   ** Students: Yadi, Yanis Atmane && Gedeon, Ronald
*/

--dataset link: https://www.kaggle.com/datasets/zafarali27/car-price-prediction

-- Table [car_price_prediction_] auto-generated code
/****** Object:  Table [dbo].[car_price_prediction_]    Script Date: 9/30/2024 6:14:56 PM ******/
/*
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[car_price_prediction_](
	[Car ID] [varchar](50) NULL,
	[Brand] [varchar](50) NULL,
	[Year] [varchar](50) NULL,
	[Engine Size] [varchar](50) NULL,
	[Fuel Type] [varchar](50) NULL,
	[Transmission] [varchar](50) NULL,
	[Mileage] [varchar](50) NULL,
	[Condition] [varchar](50) NULL,
	[Price] [varchar](50) NULL,
	[Model] [varchar](50) NULL
) ON [PRIMARY]
GO
*/

-- PRE-REQUISITS
	-- Create database named datasets;
	-- Import car_price_prediction.csv file into DB datasets
	-- select *  from CAR_PRICE_PREDICTION_;

-- Query 1: Migration to new table CARS with an evolved schema
	-- add an identity as PK & a dateStamp generated randomly simulating the date each record was registered in the DB
	-- change the metadata of the rest of the attributes
-- Query 1 should be executed from begin transaction until the dash line right after SELECT * FROM  CARS;

use datasets;
go

begin transaction t1

DROP TABLE IF EXISTS CARS

-- DDL
CREATE TABLE CARS(
	ID int identity primary key,
	DATESTAMP DATE,
	BRAND NVARCHAR(25),
	MODEL NVARCHAR(25),
	[YEAR] INT, 
	ENGINE_SIZE DECIMAL(2,1), 
	FUEL_TYPE NVARCHAR(10), 
	TRANSMISSION NVARCHAR(10), 
	MILEAGE INT, 
	CONDITION NVARCHAR(10), 
	PRICE DECIMAL(9,2)	
)

	-- Helper variables to generae a Random date this year
DECLARE @StartDate DATE = CAST(CONCAT(YEAR(GETDATE()), '-01-01') AS DATE);
DECLARE @EndDate DATE = GETDATE();

	-- DML: migrate data from table dbo.car_price_prediction_ to new talbe dbo.CARS
INSERT INTO CARS(DATESTAMP, BRAND, MODEL, [YEAR], ENGINE_SIZE, FUEL_TYPE, TRANSMISSION, MILEAGE, CONDITION, PRICE)
	SELECT DATEADD(DAY, ABS(CHECKSUM(NEWID())) % DATEDIFF(DAY, @StartDate, @EndDate) + 1, @StartDate), 
		BRAND, MODEL, [YEAR], [ENGINE SIZE], [FUEL TYPE], TRANSMISSION, MILEAGE, CONDITION, PRICE
	FROM CAR_PRICE_PREDICTION_;

	-- Transactions Control
		-- Commit or rollback Query 1: to persist or undo the changes in datasets
		-- Notice in the case of a rollback here, we won't be able to execute the rest of the queries
commit;
-- rollback

SELECT * FROM  CARS;
-----------------------------------------------

-- Query 2: DML -Aggregates
	-- Inventary: Count of Cars by Brand & Model AND DISPLAY THE MIN, AVERAGE & MAX PRICES BY MODEL
SELECT BRAND, MODEL, COUNT(*) [TOTAL QTY], MIN(PRICE) [MIN PRICE], round(AVG(PRICE), 2) [AVG PRICE BY MODEL], MAX(PRICE) [MAX PRICE]
FROM CARS 
GROUP BY BRAND, MODEL
ORDER BY BRAND, [TOTAL QTY] DESC;
-----------------------------------------------

-- Query 3: DML -Aggregates 
	-- Inventary: Total used & new cars
SELECT CONDITION, COUNT(*) [TOTAL QTY], MIN(PRICE) [MIN PRICE], round(AVG(PRICE), 2) [AVG PRICE BY MODEL], MAX(PRICE) [MAX PRICE]
FROM CARS
GROUP BY CONDITION
ORDER BY [TOTAL QTY] DESC;
-----------------------------------------------

-- Query 4: DML-Aggregates & correlated Sub-Query with the main query 
	-- Cars whose prices are greater than the Average price of all the cars OF its sub-category -MODEL
SELECT ID, BRAND, MODEL, YEAR, MILEAGE, CONDITION, PRICE
FROM CARS as [OUTER] -- we use an alias as the externe & sub-query use the same table
WHERE PRICE > (
	SELECT AVG(PRICE) 
	FROM CARS
	WHERE MODEL = [OUTER].MODEL
   )
ORDER BY PRICE;
-----------------------------------------------

--Query 5: Aggregates
	-- Count the number of Cars registered each month of this year
SELECT 
    YEAR(DATESTAMP) AS Year, 
    MONTH(DATESTAMP) AS Month, 
    COUNT(*) AS [Qty By Month]
FROM CARS
GROUP BY YEAR(DATESTAMP), MONTH(DATESTAMP)
ORDER BY Year, Month;
-----------------------------------------------

-- Query 6: DDL
	-- Apply a 10% discount on used cars
-- SELECT * FROM CARS WHERE CONDITION = 'Used';

begin transaction t2

UPDATE CARS
SET PRICE = PRICE * 0.9
WHERE CONDITION = 'Used';

-- Transactions Control
		-- Commit or rollback Query 7: to persist or undo the changes in datasets
commit;
-- rollback
-- SELECT * FROM CARS WHERE CONDITION = 'Used';
-----------------------------------------------

-- Query 7: DDL
	-- Set FUEL_TYPE to Electric for all Tesla as there is no Petrol, Diesel & Hybrid in Tesla
-- SELECT * FROM CARS WHERE BRAND = 'Tesla' AND FUEL_TYPE <> 'Electric';

begin transaction t3
UPDATE CARS
SET FUEL_TYPE = 'Electric'
WHERE BRAND = 'Tesla' AND FUEL_TYPE <> 'Electric';

-- Transactions Control
		-- Commit or rollback Query 8: to persist or undo the changes in datasets
commit;
-- rollback
-- SELECT * FROM CARS WHERE BRAND = 'Tesla' AND FUEL_TYPE <> 'Electric';
-----------------------------------------------

-- Query 8: DML -Aggregates with sub-queries 
	-- Most common Fuel Type
SELECT FUEL_TYPE
FROM CARS
GROUP BY FUEL_TYPE
HAVING COUNT(*) = (
    SELECT MAX(FuelCount) 
    FROM (
        SELECT FUEL_TYPE, COUNT(*) AS FuelCount
        FROM CARS
        GROUP BY FUEL_TYPE
    ) AS FuelTypeCounts
);
-----------------------------------------------
-- Query 9: Average Price for all Electric Cars
select Brand, Model, round(AVG(Price), 2) as avgPrice
from CARS
where Fuel_Type = 'Electric'
group by Brand, Model
-----------------------------------------------

-- Query 10: Aggregates
	-- Group Cars having a price < 5000
select Brand, Model, Condition, Year, MIN(Price) as minPrice
from CARS
group by Brand, Condition, Year, Model
having MIN(Price) < 5000;
-----------------------------------------------

-- Query 11: Sub-Query with any
	-- Cars which price > max Price of any Honda
Select Brand, Model, Max(Price) as maxPrice
from CARS
Group by Model, Brand
Having Max(Price) > (
		select Max(Price)
		from CARS
		Where Brand = 'Honda'
);
-----------------------------------------------
-- Query 12: Views
	-- Create a view showcasing the price of any Audi < price of A4 mode in Audi
Create or alter view audiView
as select brand, model, price
	from CARS
	where brand = 'Audi' AND
	Price < 
		(select MAX(price)
		from CARS
		where model = 'A4');

-- Bonnus: show content of audiView & clean up Queries
SELECT * FROM audiView;
DROP VIEW audiView;
DROP TABLE CARS;
DROP DATABASE datasets;
-----------------------------------------------