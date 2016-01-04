/*	NOTE: Only works w/ SQL Server 2012+
    Merging identical records with different validity dates.
*/
USE [master]


IF OBJECT_ID('mergeTest') IS NOT NULL
	DROP TABLE mergeTest

CREATE TABLE mergeTest			-- Create table with test data
(
	[id] int NOT NULL,
	[data] char(1) NOT NULL,
	[from] date NOT NULL,
	[to] date NOT NULL
);

INSERT INTO mergeTest ([id],[data],[from],[to]) VALUES		-- Insert records w/ different validity dates
	(1,'a','2015-01-01','2015-01-05'),	--1
	(1,'a','2015-01-05','2015-01-10'),	--2
	(1,'a','2015-01-10','2015-01-14'),	--3
	(1,'b','2015-01-14','2015-01-15'),	--4
	(1,'a','2015-01-15','2015-01-20'),	--5
	(1,'a','2015-01-20','2015-01-25'),	--6
	(1,'a','2015-01-25','2015-01-30'),	--7
	(1,'a','2015-01-30','2015-02-04'),	--8
	(2,'c','2015-01-01','2015-01-05'),	--9
	(2,'c','2015-01-05','2015-01-10')	--10

SELECT * FROM mergeTest

/*	This SELECT function uses a Common Table Expression along with Analytic functions over a partition.
	The data set is partitioned on similar primary key and data columns and ordered by 'from' dates.
	A 'last' and 'next' column is added with 'to' date of prev row and 'from' date of next row.
	For each partition, rows are selected (for each partition) that represent the first and last records 
	of identical data. For e.g. rows 5,6,7,8 are reduced to 5,8.
*/

;WITH partitionedData AS (
	SELECT *,	LAG([to],1,NULL) OVER(PARTITION BY [id],[data] ORDER BY [from]) AS [last],
				LEAD([from],1,NULL) OVER(PARTITION BY [id],[data] ORDER BY [from]) AS [next]
	FROM mergeTest)
SELECT [id],[data],[from],[to],[last],[next] INTO #temp
	FROM partitionedData
	WHERE [last] IS NULL OR [next] IS NULL OR [last]<>[from] OR [next]<>[to]
;

SELECT * FROM #temp

/*	Now all redundant 'sandwiched' records have been filtered out, only the extreme records are left.
	This MERGE function matches rows on primary key and data, and If the 'to' date of said record matches
	'from' date of another similar record, then the said record is extended to encapsulate the other record's
	'to' date. For example row 5's 'to' date is extended to equal row 8's 'to' date.
*/

MERGE INTO #temp as m1
	USING #temp as m2
	ON m1.id=m2.id AND m1.data=m2.data
WHEN MATCHED
	AND (m1.[to]=m2.[from])
	THEN
	UPDATE SET	m1.[to]=m2.[to]
;

SELECT * FROM #temp

/*	The MERGE function has done its job of extending records. However there are still 2 records with
	identical data. For e.g. rows 9,10 exist even though row 9 now has all the required information. This 
	block modifies such redundant rows so their 'last' and 'from' columns become asynchronous.
*/

;WITH repartitionedData AS (
	SELECT [id],[data],[from],[to],	LAG([to],1,NULL) OVER(PARTITION BY [id],[data] ORDER BY [from]) AS [last],
				LEAD([from],1,NULL) OVER(PARTITION BY [id],[data] ORDER BY [from]) AS [next]
	FROM #temp)
SELECT [id],[data],[from],[to],[last],[next] INTO #temptemp
	FROM repartitionedData
	WHERE [last] IS NULL OR [next] IS NULL OR [last]<>[from] OR [next]<>[to]
;

SELECT * FROM #temptemp

/* Asynchronous rows are deleted
*/

DELETE FROM #temptemp
	WHERE [from]<[last]

SELECT * FROM #temptemp

/*	However, blocks of data with >2 rows (like rows 5 through 8) could not be merged because of the filtered out
	rows (i.e. rows 6,7). Applying MERGE again on the updated data set.
*/

MERGE INTO #temptemp as m1
	USING #temptemp as m2
	ON m1.id=m2.id AND m1.data=m2.data
WHEN MATCHED
	AND (m1.[from]=m2.[next])
	THEN
	UPDATE SET	m1.[from]=m2.[from],
				m1.[last]=CASE WHEN ((m2.[last] IS NULL) OR (m2.[next] IS NULL)) THEN NULL ELSE m1.[last] END	--if row absorbing from is extreme, then current row is also extreme
;

SELECT * FROM #temptemp

TRUNCATE TABLE mergeTest		-- resetting original table

/* The MERGE corrected all rows with the correct 'from' and 'to' dates. And the only rows we are interested in are
	the extreme rows i.e. with 'last' or 'next' == NULL. SELECTing on that criterion and INSERTing into original table.
*/
	
INSERT INTO mergeTest			-- inserting processed records into table + some last minute filtering
	SELECT [id],[data],[from],MAX([to])
	FROM #temptemp
		WHERE [next] IS NULL OR [last] IS NULL
	GROUP BY [id],[data],[from]

SELECT * FROM mergeTest

DROP TABLE #temp
DROP TABLE #temptemp