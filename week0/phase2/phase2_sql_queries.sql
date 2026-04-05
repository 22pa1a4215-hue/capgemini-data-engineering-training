create table customers ( customer_id int, customer_name varchar(50), city varchar(50), age int );
insert into customers values (1,'ravi','hyderabad',25),(2,'sita','chennai',32),(3,'arun','hyderabad',28),
  (4,'teena','bangalore',25),(5,'teju','hyderabad',28),(6,'greeshma','chennai',27);

create table orders ( order_id int, customer_id int, amount int );
insert into orders values (101,1,1550),(102,5,5350),(104,3,1750),(105,5,3000),(106,6,5000),(107,null,2000);

select customer_id, sum(amount) as total_amount from orders where customer_id is not null group by customer_id;

select customer_id, sum(amount) as total_amount from orders where customer_id is not null group by customer_id 
  order by total_amount desc limit 3;

select c.city, sum(o.amount) as total_revenue from customers c inner join orders o on c.customer_id=o.customer_id 
  where o.customer_id is not null group by c.city;

select customer_id, avg(amount) as avg_amount from orders where customer_id is not null group by customer_id;

select customer_id, count(order_id) as order_count from orders where customer_id is not null group by customer_id having count(order_id)>1;

select customer_id, sum(amount) as total_amount from orders where customer_id is not null group by customer_id order by total_amount desc;