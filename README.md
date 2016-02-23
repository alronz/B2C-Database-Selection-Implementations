This project was created to show how to use NoSQL databases such as Redis, MongoDB, Neo4j and Cassandra to implement a real life data model of a B2C application and try to check how is the query expressiveness for each database.  The data model used for this experiment is the [TPCH benchmark](http://www.tpc.org/tpch/) data model shown below:



![image](https://s3.amazonaws.com/b2cbucket/tpch_schema.png)




To check the query expressiveness for each database, three SQL queries have been chosen from the same benchmark (TPCH) which are shown below:

##### Pricing Summary Report Query (Q1)

This query is used to report the amount of billed, shipped and returned items. The SQL query is shown below:

````
select   l_returnflag,   l_linestatus,   sum(l_quantity) as sum_qty,   sum(l_extendedprice) as sum_base_price,   sum(l_extendedprice*(1-l_discount)) as sum_disc_price,   sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,   avg(l_quantity) as avg_qty,   avg(l_extendedprice) as avg_price,   avg(l_discount) as avg_disc,   count(*) as count_orderfrom   lineitemwhere   l_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3)group by   l_returnflag,   l_linestatusorder by   l_returnflag,   l_linestatus;
````


##### Shipping Priority Query (Q3)

This query is used to get the 10 unshipped orders with the highest value. In order to do that, the query joins three tables (customer, order and lineitem) as shown below:


````
select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriorityfrom customer, orders, lineitemwhere c_mktsegment = '[SEGMENT]' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '[DATE]' and l_shipdate > date '[DATE]'group by l_orderkey, o_orderdate, o_shippriorityorder by revenue desc, o_orderdate;
````


##### Order Priority Checking Query (Q4)

This query is used to see how well the order priority system is working and gives indication of the customer satisfaction. The SQL query is shown below:

````
select o_orderpriority, count(*) as order_countfrom
 orderswhere o_orderdate >= date '[DATE]' and o_orderdate < date '[DATE]' + interval '3' month and exists (select *from lineitemwhere l_orderkey = o_orderkey and l_commitdate < l_receiptdate)group by o_orderpriorityorder by o_orderpriority;
````


##### Project structure

This project was created as separate projects for each database where each project can be run alone as a service and each query is built like an API that can be called to test the code. For example for the Cassandra database, you can find the project under "Cassandra/CassandraTPCHQueries/" and for MongoDB the project is under the path "MongoDB/TPCHQueries/" and so forth.

##### Usage

The data used as input for the projects of each database is a small set from the data generated using TPCH DBGEN tool. The data is stored in CSV files that correspond to each object in the TPCH benchmark schema shown above (customer, supplier, part, lineitem, order , etc ...). For more details about how to generate the data, please have a look at [this](http://kejser.org/tpc-h-data-and-query-generation/) blog post.  You can change the used input data by changing the content of the input files that are located under the "data" folder. For example, the input files for Cassandra are located in [this](https://github.com/alronz/B2C-Database-Selection-Implementations/tree/master/Cassandra/CassandraTPCHQueries/src/main/java/org/cassandra/tpcH/data) path.


To test the projects, you can use run the individual projects for each database and use swagger UI to call the APIs for each query. For example, you can run the (Cassandra/CassandraTPCHQueries/) project by running the following:

````
gradle run
````


Then you can find the swagger ui in the following path:


````
http://localhost:{port}/api/swagger
````






