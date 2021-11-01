#README

##UDTFDistinctCount
Returns distinct values (in column 2) and count (in column 1). Column 1 is "time" column therefore it is a long of count eg 65 means there are 65 occurences)

create function distinct_count as 'nz.ope.iotdb.extras.UDTFDistinctCount'

example queries (using sample data)

select distinct_count(hardware) from root.ln.wf02.wt02;

Time|distinct_count(root.ln.wf02.wt02.hardware)
------------ | -------------
1970-01-01T12:00:05.064+12:00|v1
1970-01-01T12:00:05.016+12:00|v2

select distinct_count(status) from root.ln.wf02.wt02;

Time|distinct_count(root.ln.wf02.wt02.status)|
------------ | -------------
1970-01-01T12:00:05.064+12:00|false
1970-01-01T12:00:05.016+12:00|true

select distinct_count(temperature) from root.ln.wf01.wt01;

Time|distinct_count(root.ln.wf01.wt01.temperature)
------------ | -------------
1970-01-01T12:00:00.020+12:00|24.37
1970-01-01T12:00:00.009+12:00|24.12
1970-01-01T12:00:00.016+12:00|24.87
