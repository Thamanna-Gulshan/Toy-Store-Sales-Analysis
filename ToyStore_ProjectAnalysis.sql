/* DATABASE CREATION */

CREATE DATABASE toyStore;
USE toyStore;

/* TABLE SCHEMA CREATION */

-- Products table
CREATE TABLE products (
    product_id VARCHAR(100),
    product_name VARCHAR(100),
    product_category VARCHAR(100),
    product_cost VARCHAR(100),
    product_price VARCHAR(100)
);

-- Stores table
CREATE TABLE stores (
    store_id VARCHAR(100),
    store_name VARCHAR(100),
    store_city VARCHAR(100),
    store_location VARCHAR(100),
    store_open_date VARCHAR(100)
);

-- Inventory table
CREATE TABLE inventory (
    store_id VARCHAR(100),
    product_id VARCHAR(100),
    stock_on_hand VARCHAR(100)
);

-- Sales table
CREATE TABLE sales (
    sale_id VARCHAR(100),
    sale_date VARCHAR(100),
    store_id VARCHAR(100),
    product_id VARCHAR(100),
    units VARCHAR(100)
);

/* IMPORTING DATA */

-- Enable the local_infile parameter to permit local data loading 
SHOW VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;

-- Products data
LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\products.csv'
INTO TABLE products
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Stores data
LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\stores.csv'
INTO TABLE stores
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Inventory data
LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\inventory.csv'
INTO TABLE inventory
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
    
-- Sales data
LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\sales.csv'
INTO TABLE sales
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

/* DATA CLEANING AND PREPARATION */

-- Products table
SELECT Column_name, Data_type 
FROM Information_schema.columns
WHERE Table_name='products';

SELECT * FROM products;

-- Identify missing values on key columns
SELECT COUNT(*) AS missingValue_Count 
FROM products WHERE
Product_ID IS NULL OR
Product_Name IS NULL OR
Product_Category IS NULL OR
Product_Cost IS NULL OR
Product_Price IS NULL;
-- No missing values in the data

-- Identify duplicate values
WITH Duplicate as (SELECT Product_ID, Product_Name,Product_Category,Product_Cost,Product_Price, 
row_number() over(partition by Product_ID ORDER BY Product_ID) as ranking
FROM products)
select count(*) from duplicate where ranking>1;
-- No duplicate values in the table

-- Correcting datatype of columns
ALTER TABLE products
MODIFY COLUMN product_id int;

-- Product_Cost and Product_Price columns have a “$” symbol with them due to which their data type would not be double
-- Need to change the “Product_Cost” and “Product_Price” columns such that they only have double values

-- Removing the '$' sign
UPDATE Products 
SET Product_Cost=substr(Product_Cost,2,length(Product_Cost)),
Product_Price=substr(Product_Price,2,length(Product_Price));

-- Coverting data type to double
 ALTER TABLE Products 
 MODIFY COLUMN Product_Cost DOUBLE,
 MODIFY COLUMN Product_Price DOUBLE;
 
-- Stores table
SELECT Column_name, Data_type 
FROM Information_schema.columns
WHERE Table_name='stores';

SELECT * FROM stores;

-- Identify missing values on key columns
SELECT COUNT(*) AS missingValue_Count 
FROM stores WHERE
Store_ID IS NULL OR
Store_Name IS NULL OR
Store_City IS NULL OR
Store_Location IS NULL OR
Store_Open_Date IS NULL;
-- No missing values in the data

-- Identify duplicate values
WITH Duplicate AS 
(SELECT Store_ID, Store_Name,Store_City,Store_Location, Store_Open_Date,
row_number() over(partition by Store_ID ORDER BY Store_ID) as ranking
FROM stores)
select count(*) from duplicate where ranking>1;
-- No duplicate values in the table

-- Correcting datatype
ALTER TABLE stores
MODIFY COLUMN store_id INT;

-- Formatting the column values to YYYY-MM-DD
UPDATE stores SET Store_Open_Date=STR_TO_DATE(Store_Open_Date, '%d-%m-%Y');

ALTER TABLE stores 
MODIFY COLUMN Store_Open_Date DATE;

-- Inventory table
SELECT Column_name, Data_type 
FROM Information_schema.columns
WHERE Table_name='inventory';

SELECT * FROM inventory;

-- Identify missing values on key columns
SELECT COUNT(*) AS missingValue_Count 
FROM inventory WHERE
Store_ID IS NULL OR
Product_ID IS NULL OR
Stock_On_Hand IS NULL;
-- No missing values in the data

-- Identify duplicate values
WITH Duplicate AS 
(SELECT Store_ID, Product_ID,Stock_On_Hand,
row_number() over(partition by Store_ID,Product_ID,Stock_On_Hand ORDER BY Store_ID) as ranking
FROM inventory)
select count(*) from duplicate where ranking>1;
-- No duplicate values in the table

-- Correcting datatype
ALTER TABLE inventory
MODIFY COLUMN store_id INT, 
MODIFY COLUMN product_id INT,
MODIFY COLUMN stock_on_hand INT;

-- Sales table
SELECT Column_name, Data_type 
FROM Information_schema.columns
WHERE Table_name='sales';

SELECT * FROM sales;

-- Identify missing values on key columns
SELECT COUNT(*) AS missingValue_Count 
FROM sales WHERE
Sale_ID IS NULL OR 
Date IS NULL OR
Store_ID IS NULL OR
Product_ID  IS NULL;
-- No missing values in the data

-- Identify duplicate values
WITH Duplicate AS 
(SELECT Sale_ID, Date,Store_ID,Product_ID,Units, 
row_number() over(partition by Sale_ID ORDER BY Sale_ID) as ranking
FROM sales)
select count(*) from duplicates where ranking>1;
-- No duplicate values in the table

-- Correcting datatype
ALTER TABLE sales
MODIFY COLUMN sale_id int,
MODIFY COLUMN sale_date date,
MODIFY COLUMN store_id int,
MODIFY COLUMN product_id int,
MODIFY COLUMN units int;

/* ESTABLISHING RELATIONSHIP BETWEEN TABLES */

-- Adding primary key on product_id on products table
ALTER TABLE products
ADD CONSTRAINT PK_Product_ID
PRIMARY KEY(product_id);

-- Adding primary key on store_id on stores table
ALTER TABLE stores
ADD CONSTRAINT PK_store_id
PRIMARY KEY(store_id);

-- Adding primary key on sale_id on sales table
ALTER TABLE sales
ADD CONSTRAINT PK_sale_id
PRIMARY KEY(sale_id);

-- Establishing relationship between sales and stores table 
ALTER TABLE sales
ADD CONSTRAINT fk_store_id_s
FOREIGN KEY(store_id) REFERENCES stores(store_id);

-- Establishing relationship between sales and products table 
ALTER TABLE sales
ADD CONSTRAINT fk_product_id_s
FOREIGN KEY(product_id) REFERENCES products(product_id);

-- Establishing relationship between inventory and stores table 
ALTER TABLE inventory
ADD CONSTRAINT fk_store_id_i
FOREIGN KEY(store_id) REFERENCES stores(store_id);

-- Establishing relationship between inventory and products table 
ALTER TABLE inventory
ADD CONSTRAINT fk_product_id_i
FOREIGN KEY(product_id) REFERENCES products(product_id);

/* ADDING NEW COLUMNS */

-- Adding new column 'profit' to the sales table calculating the profit for each sale transaction. 
-- Profit = units*(product_price-product_cost)


ALTER TABLE sales ADD COLUMN profit DOUBLE;

-- Updating the column with values
UPDATE sales s
JOIN products p on p.product_id=s.product_id 
SET profit = round(units*(Product_Price - Product_Cost),2);

-- Adding new columns to sales table that are useful to identify sales trends
ALTER TABLE sales 
ADD COLUMN week_day VARCHAR(20),
ADD COLUMN month_number INT,
ADD COLUMN month_name varchar(20),
ADD COLUMN quarter char(2),
ADD COLUMN year int;

-- Updating the columns with values
UPDATE sales SET
week_day=DAYNAME(sale_date),
month_number=MONTH(sale_date),
month_name=MONTHNAME(sale_date),
quarter=CONCAT('Q',QUARTER(sale_date)),
year=YEAR(sale_date);

select * from sales;

-- Adding a column revenue in sales table to understand the revenue generated
ALTER TABLE sales ADD COLUMN revenue double;

-- Updating the column with values
-- Revenue = Units*Product_Price
UPDATE sales s
join products p on s.Product_ID=p.Product_ID
SET revenue=s.units*p.Product_Price;

/* ABOUT DATA */

Select COUNT(DISTINCT sale_id) from sales;
-- 8,29,262 records of sales transactions

SELECT MIN(sale_date) as start_date, MAX(sale_date) as end_date  from sales;
-- For the period of JAN 2022 to SEP 2023.

SELECT COUNT(DISTINCT product_id) from products;
SELECT COUNT(DISTINCT product_category) from products;
-- 35 different types of products are available divided into 5 categories

SELECT COUNT(DISTINCT store_id) from stores;
SELECT COUNT(DISTINCT store_city) from stores;
SELECT COUNT(DISTINCT store_location) from stores;
-- 50 no of stores spread across 29 different cities in 4 different locations

/* SUMMARY: 
The dataset contains:
- Over 800,000 records of sales transactions 
- By 50 Maven Toys store locations across 29 different cities in 4 different locations
- Selling 35 products across 5 categories
- From JAN 2022 to SEP 2023.

/* ANALYSIS */

/* Sales Trend Analysis*/

-- Monthly wise sales trend over the stores, location for both year(2022 & 2023)
With monthlySales as (SELECT DATE_FORMAT(sale_date, '%Y-%m') as Month, Store_Location, Store_Name, FORMAT(round(sum(revenue),2),2) as Total_Sales
FROM sales s
join stores st on s.Store_ID=st.Store_ID
where year IN ('2022','2023')
group by DATE_FORMAT(sale_date, '%Y-%m'), Store_Location,Store_Name 
ORDER BY Store_Location, Store_Name, Month)
Select *,
ROUND(100.0*(Total_Sales-LAG(Total_Sales) OVER(PARTITION BY Store_Location,Store_Name))/
LAG(Total_Sales) OVER(PARTITION BY Store_Location,Store_Name),2) as Prct_Change
from monthlySales;


-- Create a comparison of Monthly sales, Quarterly sales between 2022 and 2023 sales
-- Monthly sales comparision
WITH monthly_sales as 
(select month_name , 
CONCAT(FORMAT(SUM(CASE WHEN year='2022' then revenue else 0 end)/ 1000, 2), 'k') as Sales_2022,
CONCAT(FORMAT(SUM(case when year='2023' then revenue else 0 end)/ 1000, 2), 'k') as Sales_2023
from sales
group by month_name)
SELECT *, 
ROUND((Sales_2023 - Sales_2022) / Sales_2022 * 100,2) AS '% change in revenue'
FROM monthly_sales;

-- Quarterly sales comparision
WITH quarterly_sales as 
(select quarter, 
CONCAT(FORMAT(sum(case when year='2022' then revenue else 0 end)/ 1000000, 2), 'M') as Sales_2022,
CONCAT(FORMAT(sum(case when year='2023' then revenue else 0 end)/ 1000000, 2), 'M') as Sales_2023
from sales
group by quarter)
SELECT *, 
ceil((Sales_2023 - Sales_2022) / Sales_2022 * 100) AS 'prct_chnage'
FROM quarterly_sales;
  
/* Stores performance Analysis */

-- Find the sales trend over the different Stores and find the best and least five stores as per the performance in one query.
with store_ranking as (select Store_Name , CONCAT(FORMAT(sum(revenue)/ 1000, 2), 'k') as total_sales, 
row_number() over(order by sum(revenue) desc) as top_rank, 
row_number() over(order by sum(revenue)) as least_rank
from stores st
join sales s on st.Store_ID=s.Store_ID
group by store_name)

SELECT
    CASE 
        WHEN top_rank <= 5 THEN 'Best Performing'
        WHEN least_rank <= 5 THEN 'Least Performing'
        ELSE 'Other'
    END AS store_performance, Store_Name, total_sales
FROM store_ranking
WHERE top_rank <= 5 OR least_rank <= 5
ORDER BY total_sales DESC;

-- Which stores performs well than the last year 
WITH store_sales as 
(SELECT Store_Name, store_location,
CONCAT(FORMAT(SUM(CASE WHEN year='2022' then revenue end) / 1000, 2), 'k') as sales_2022,
CONCAT(FORMAT(SUM(case when year='2023' then revenue end) / 1000, 2), 'k') as sales_2023
from sales s
join stores st on s.store_id=st.store_id
group by store_name,store_city, store_location)
select *,  round((sales_2023 - sales_2022) / sales_2022 * 100,2)AS 'Prct_Increase'
from store_sales 
where sales_2022<sales_2023
order by Prct_Increase desc;

/* Product performance analysis */

-- Find out the report of Product that which product performs well and contributing most part of sales 
CREATE VIEW Product_Sales AS
SELECT product_name,product_category, CONCAT(FORMAT(sum(revenue)/ 1000000, 2), 'M') AS total_sales_revenue,
FORMAT(sum(units),0) AS total_units_sold,
ROUND(100.0*SUM(revenue)/(SELECT sum(revenue) FROM sales),1) AS Prct_contribution
FROM products p
JOIN sales s
ON p.product_id=s.product_id
GROUP BY product_name,product_category
ORDER BY Prct_contribution DESC;
SELECT * FROM Product_Sales;


-- Is there any seasonality between the last three half yearly sales counted with the max(date) of sales. */
SELECT MAX(sale_date) FROM sales;

-- Query to calculate sales for the last three half-yearly periods
WITH half_yearly_sales as 
(SELECT
CASE 
	WHEN month_number BETWEEN 1 AND 6 THEN 'H1 ' 
	WHEN month_number BETWEEN 7 AND 12 THEN 'H2 ' 
END AS half_year,
year,
round(SUM(revenue),2) AS total_sales_revenue
FROM Sales
GROUP BY half_year, year
ORDER BY year DESC, half_year DESC)
-- Analyzing seasonality
SELECT half_year, year, total_sales_revenue,
LEAD(total_sales_revenue) OVER (ORDER BY year DESC, half_year DESC) AS prev_sales,
ROUND(((total_sales_revenue - LEAD(total_sales_revenue) OVER (ORDER BY year DESC, half_year DESC)) / LEAD(total_sales_revenue) OVER (ORDER BY year DESC, half_year DESC)) * 100, 2) AS percentage_change
FROM half_yearly_sales
ORDER BY year DESC, half_year DESC
LIMIT 3;

-- High demanded product among all locations as per the sales. 
SELECT 
    p.product_name,
    ROUND(SUM(s.revenue), 2) AS total_sales,
    SUM(units_sold) AS total_units_sold,
    SUM(i.stock_on_hand) AS store_count
FROM Products p
JOIN 
(SELECT product_id, SUM(revenue) AS revenue, SUM(units) AS units_sold
FROM Sales
GROUP BY product_id) s
ON p.product_id = s.product_id
JOIN 
(SELECT product_id, SUM(stock_on_hand) AS stock_on_hand
FROM Inventory
GROUP BY product_id) i
ON p.product_id = i.product_id
GROUP BY p.product_name, p.product_category
ORDER BY total_units_sold DESC;


/* Inventory health analysis */

-- Find out the avg_inventory as per the store and product. 
select store_name, product_name, round(avg(Stock_On_Hand)) as avg_inventory
from inventory i
join products p on i.product_id=p.product_id
join stores s on i.store_id=s.store_id
group by store_name, product_name
order by store_name, product_name;

-- Analyze the Inventory turnover ratio as per the store wise along with avg_inventory in a comparative report 

-- Aggregate Sales data per store
WITH aggregated_sales AS (
SELECT store_id,SUM(revenue) AS total_sales
FROM sales
GROUP BY store_id
),

-- Aggregate Inventory data per store
aggregated_inventory AS (
SELECT store_id, AVG(Stock_On_Hand) AS avg_inventory
FROM inventory
GROUP BY store_id
)

-- Combine the data from Sales and Inventory
SELECT 
    st.store_name, 
    ROUND(SUM(asales.total_sales), 2) AS total_sales,
    ROUND(AVG(ai.avg_inventory), 2) AS avg_inventory,
    ROUND(SUM(asales.total_sales) / NULLIF(AVG(ai.avg_inventory), 0), 2) AS inventory_turnover_ratio
FROM aggregated_sales asales
JOIN aggregated_inventory ai ON asales.store_id = ai.store_id
JOIN stores st ON asales.store_id = st.store_id
GROUP BY st.store_name
ORDER BY inventory_turnover_ratio DESC;































