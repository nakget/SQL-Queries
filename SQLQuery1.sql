-- Creates a partition function called myRangePF1 that will partition a table into four partitions
CREATE PARTITION FUNCTION myRangePF1 (int)
    AS RANGE LEFT FOR VALUES (1, 100, 1000) ;
GO
-- Creates a partition scheme called myRangePS1 that applies myRangePF1 to the four filegroups created above
CREATE PARTITION SCHEME myRangePS1
    AS PARTITION myRangePF1
    ALL TO ([PRIMARY]);
GO
-- Creates a partitioned table called PartitionTable that uses myRangePS1 to partition col1
CREATE TABLE PartitionTable (col1 int PRIMARY KEY, col2 char(10))
    ON myRangePS1 (col1) ;
GO

INSERT INTO PARTITIONTABLE VALUES(-2,'A'),(-3,'B')

INSERT INTO PARTITIONTABLE VALUES(7,'c'),(1006,'d')
--get the number of row in each partition
SELECT row_count, * FROM sys.dm_db_partition_stats 
WHERE object_id = OBJECT_ID('PartitionTable');


SELECT * 
FROM sys.tables AS t 
JOIN sys.indexes AS i 
    ON t.[object_id] = i.[object_id] 
    AND i.[type] IN (0,1) 
JOIN sys.partition_schemes ps 
    ON i.data_space_id = ps.data_space_id 
WHERE t.name = 'PartitionTable'; 
GO


SELECT 
    t.[object_id] AS ObjectID 
    , t.name AS TableName 
    , ic.column_id AS PartitioningColumnID 
    , c.name AS PartitioningColumnName 
FROM sys.tables AS t 
JOIN sys.indexes AS i 
    ON t.[object_id] = i.[object_id] 
    AND i.[type] <= 1 -- clustered index or a heap 
JOIN sys.partition_schemes AS ps 
    ON ps.data_space_id = i.data_space_id 
JOIN sys.index_columns AS ic 
    ON ic.[object_id] = i.[object_id] 
    AND ic.index_id = i.index_id 
    AND ic.partition_ordinal >= 1 -- because 0 = non-partitioning column 
JOIN sys.columns AS c 
    ON t.[object_id] = c.[object_id] 
    AND ic.column_id = c.column_id 
WHERE t.name = 'PartitionTable' ; 
GO



SELECT t.name AS TableName, i.name AS IndexName, p.partition_number, p.partition_id, i.data_space_id, f.function_id, f.type_desc, r.boundary_id, r.value AS BoundaryValue 
FROM sys.tables AS t
JOIN sys.indexes AS i
    ON t.object_id = i.object_id
JOIN sys.partitions AS p
    ON i.object_id = p.object_id AND i.index_id = p.index_id 
JOIN  sys.partition_schemes AS s 
    ON i.data_space_id = s.data_space_id
JOIN sys.partition_functions AS f 
    ON s.function_id = f.function_id
LEFT JOIN sys.partition_range_values AS r 
    ON f.function_id = r.function_id and r.boundary_id = p.partition_number
WHERE t.name = 'PartitionTable' AND i.type <= 1
ORDER BY p.partition_number;