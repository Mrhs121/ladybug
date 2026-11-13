CREATE NODE TABLE User(uid INT64 PRIMARY KEY, uname STRING)
WITH (storage = 'ecom.users', format = 'iceberg_poc');
CREATE NODE TABLE Product(pid INT64 PRIMARY KEY, pname STRING)
WITH (storage = 'ecom.products', format = 'iceberg_poc');
CREATE REL TABLE Purchased(FROM User TO Product, oid INT64, amt INT64)
WITH (storage = 'ecom.orders KEY oid FROM uid TO pid', format = 'iceberg_poc');
