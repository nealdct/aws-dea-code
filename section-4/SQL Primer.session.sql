CREATE DATABASE sqlprimer;

USE sqlprimer;

-- Create Customer Table
CREATE TABLE Customer(CustomerID int, LastName varchar(255), FirstName varchar(255), City varchar(255), State char(2));

-- Insert Customers
INSERT INTO Customer VALUES (101, "Lane", "Lois", "Metropolis", "IL");
INSERT INTO Customer VALUES (102, "Paige", "Turner", "New York", "NY");
INSERT INTO Customer VALUES (103, "Hazel", "Nutt", "Miami", "FL");
INSERT INTO Customer VALUES (104, "May", "Day", "Miami", "OH");
INSERT INTO Customer VALUES (201, "Goodnight", "Mary", "Metropolis", "IL");
INSERT INTO Customer VALUES (202, "Case", "Tiffany", "Albany", "NY");
INSERT INTO Customer VALUES (251, "Book", "Rita", "Austin", "TX");
INSERT INTO Customer VALUES (252, "May", "Day", "Cleveland", "OH");
INSERT INTO Customer VALUES (301, "Teak", "Anne", "Chicago", "IL");
INSERT INTO Customer VALUES (402, "Bugg", "Aida", "New York", "NY");
INSERT INTO Customer VALUES (503, "Mine", "Bea", "Miami", "FL");
INSERT INTO Customer VALUES (504, "Furter", "Frank", "Orlando", "FL");
INSERT INTO Customer VALUES (401, "Cuda", "Barry", "Pittsburgh", "PA");
INSERT INTO Customer VALUES (302, "Ateer", "Mark", "New York", "NY");
INSERT INTO Customer VALUES (303, "Ryta", "Tex", "Huntsville", "AL");
INSERT INTO Customer VALUES (304, "En", "Dee", "Newark", "NJ");

SELECT City, State FROM Customer;

-- Select unique customer cities
SELECT DISTINCT City, State FROM Customer;

-- Find customers who live in Miami 
SELECT LastName, FirstName 
FROM Customer
WHERE City = 'Miami';

SELECT LastName, FirstName 
FROM Customer
WHERE City = 'Miami' 
AND State = 'FL';

-- Find customers who live in NY or FL 
SELECT LastName, FirstName 
FROM Customer
WHERE State = 'NY' 
OR State = 'FL';

-- Find customers who don't live in NY 
SELECT LastName, FirstName 
FROM Customer
WHERE NOT State = 'NY';

-- Find customers who live in New York City or Albany, NY 
SELECT LastName, FirstName 
FROM Customer
WHERE State = 'NY' AND (City = 'New York' OR City = 'Albany');

-- Find customers with an ID greater than 500
SELECT LastName, FirstName 
FROM Customer
WHERE CustomerID > 500;

-- Find customers with an ID between 200 and 500
SELECT LastName, FirstName 
FROM Customer
WHERE CustomerID BETWEEN 200 AND 250;

-- Find customers with a last name that starts with A
SELECT *
FROM Customer
WHERE LastName LIKE 'A%';

-- Find customers who live in New York, Atlanta, or Chicago
SELECT *
FROM Customer
WHERE City IN ('New York', 'Atlanta', 'Chicago');

-- Find customers who live in New York and combine their names
SELECT CustomerID, CONCAT(FirstName,' ',LastName) AS CustName
FROM Customer
WHERE State = 'NY';

-- Add a primary key
ALTER TABLE Customer
ADD PRIMARY KEY (CustomerID);

-- Create Products Table
CREATE TABLE Product (
	ProductID int NOT NULL,
    Name varchar(100),
    ListPrice decimal(10,2),
    Sizes varchar(100),
    Category varchar(50),
    PRIMARY KEY (ProductID)
);    

-- Add products
INSERT INTO Product VALUES (541, "Polo shirt", 19.95, "S,M,L,XL", "Mens");
INSERT INTO Product VALUES (545, "Scarf", 24.95, NULL, "Accessories");
INSERT INTO Product VALUES (548, "Dress", 74.95, "0,2,4,6,8", "Womens");
INSERT INTO Product VALUES (549, "Shorts", 19.95, "XS,S,M,L", "Childrens");
INSERT INTO Product VALUES (551, "Sandals", 19.95, "5,6,7,8,9,10", "Womens");
INSERT INTO Product VALUES (552, "Cap", 9.95, NULL, "Accessories");
INSERT INTO Product VALUES (555, "Dress", 54.95, "0,2,4,6,8", "Womens");
INSERT INTO Product VALUES (558, "Pants", 49.95, "30W30L,30W32L,32W30L,32W32L", "Mens");

-- Find products without a size
SELECT *
FROM Product
WHERE Sizes IS NULL;

-- Find products with a size
SELECT *
FROM Product
WHERE Sizes IS NOT NULL;

-- List product id, name, and department
SELECT ProductID, Name, Category AS Department
FROM Product;

-- Find the average list price for Womens products
SELECT AVG(ListPrice) AS AvgPrice
FROM Product
WHERE Category = 'Womens';

-- Find how many accessory products
SELECT COUNT(ProductID) AS CountAccessories
FROM Product
WHERE Category = 'Accessories';

-- Find how many products in each category
SELECT Category, COUNT(ProductID) AS CountByCategory
FROM Product
GROUP BY Category
ORDER BY Category;

-- Create Orders Table
CREATE TABLE Orders (
    OrderID int NOT NULL,
    CustomerID int,
    SubTotal decimal(19, 2),
    Delivery decimal(6, 2) DEFAULT 0.00,
    OrderDate date DEFAULT(CURRENT_DATE),
    ShipDate date,
	DeliveryDate date,
    PRIMARY KEY (OrderID),
    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID)
);

-- Add some orders
INSERT INTO Orders VALUES (111, 103, 19.95, 8.50, "2024-02-15", "2024-02-16", "2024-02-17");
INSERT INTO Orders VALUES (112, 103, 24.50, 8.50, "2024-02-18", "2024-02-23", "2024-02-25");
INSERT INTO Orders VALUES (113, 101, 69.67, NULL, "2024-02-18", "2024-02-18", "2024-02-19");

-- List orders with most recent first
SELECT *
FROM Orders
ORDER BY OrderDate DESC;

-- Find the total sales in Februrary
SELECT SUM(SubTotal) AS OrderTotal
FROM Orders
WHERE OrderDate BETWEEN '2024-02-01' AND '2024-02-29';

-- Find how long customers are waiting to receivet their orders
SELECT AVG(DATEDIFF(DeliveryDate, OrderDate)) AS AvgCustWait, 
MAX(DATEDIFF(DeliveryDate, OrderDate)) AS MaxCustWait, 
MIN(DATEDIFF(DeliveryDate, OrderDate)) AS MinCustWait
FROM Orders;

SELECT Category, COUNT(ProductID) AS CountByCategory
FROM Product
GROUP BY Category
ORDER BY Category;

-- Join orders with customer table
SELECT Orders.OrderID, Customer.FirstName, Customer.LastName, Orders.OrderDate 
FROM Customer INNER JOIN Orders
ON Customer.CustomerID = Orders.CustomerID;

SELECT Orders.OrderID, Customer.FirstName, Customer.LastName, Orders.OrderDate 
FROM Customer LEFT OUTER JOIN Orders
ON Customer.CustomerID = Orders.CustomerID;

SELECT Orders.OrderID, Customer.FirstName, Customer.LastName, Orders.OrderDate 
FROM Customer RIGHT JOIN Orders
ON Customer.CustomerID = Orders.CustomerID;

-- Add an index to Orders table
CREATE INDEX index_orderdate ON Orders (OrderDate);

-- Add an index to Customer table
CREATE INDEX index_location ON Customer (City, State);

-- Use the indexes
SELECT * FROM Orders WHERE OrderDate BETWEEN '2024-02-01' AND '2024-02-29';

SELECT * FROM Customer WHERE City = 'Miami' AND State = 'FL';

-- Create a new table for sales products containing accessories
CREATE TABLE SaleProducts AS
SELECT ProductID, ROUND(ListPrice * 0.70,2) AS SalePrice
FROM Product
WHERE Category = 'Accessories';

Select * from SaleProducts;

-- Add Mens to sale products
INSERT INTO SaleProducts
SELECT ProductID, ROUND(ListPrice * 0.75,2) AS SalePrice
FROM Product
WHERE Category = 'Mens';

Select * from SaleProducts;

UPDATE Product
SET ListPrice = 29.95
WHERE ProductID IN (545, 549);

Select * from Product;
