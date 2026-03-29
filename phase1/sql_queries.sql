Creating a table customers with columns customer_id,customer_name,city,age with values using insertinto
  
CREATE TABLE customers 
  ( customer_id INT,
  customer_name VARCHAR(50),
  city VARCHAR(50),
  age INT ); 

INSERT INTO customers VALUES 
  (1, 'Ravi', 'Hyderabad', 25),
  (2, 'Sita', 'Chennai', 32),
  (3, 'Arun', 'Hyderabad', 28);

#To show all customers
SELECT * FROM customers;

#Show customers from Chennai
SELECT * FROM customers WHERE city = 'Chennai';

#Show customers with age > 25
SELECT * FROM customers WHERE age > 25;

#Show only customer_name and city
SELECT customer_name, city FROM customers;

#Count customers city-wise
SELECT city, COUNT(*) 
FROM customers 
GROUP BY city;
