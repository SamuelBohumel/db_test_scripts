# Test Database performance


This code shows the difference in query times before and after adding an index.
The sample 

## SELECT commands comparison after adding an index


Inserted 1000000 records in 264.16 seconds.

Running 10000 SELECT queries without index...

Average query time (no index): 0.06131 seconds

Creating index on 'city' column...

Running 10000 SELECT queries with index...

Average query time (with index): 0.00018 seconds