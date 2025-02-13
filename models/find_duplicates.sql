select * from raw.ecomm.orders_au 
qualify count(*) over (partition by id)>1 order by id


