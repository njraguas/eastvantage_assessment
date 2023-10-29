select
    Customers.customer_id as Customer,
    age as Age,
    Items.item_name as Item,
    sum(No_Null_Orders.quantity) as Quantity
from Customers
inner join Sales
    on Sales.customer_id = Customers.customer_id
inner join (
    select
        *
    from Orders
    where quantity is not null
) as No_Null_Orders
    on No_Null_Orders.sales_id = Sales.sales_id
inner join Items
    on Items.item_id = No_Null_Orders.item_id
where Customers.age between 18 and 35
group by Customers.customer_id, age, Items.item_name;