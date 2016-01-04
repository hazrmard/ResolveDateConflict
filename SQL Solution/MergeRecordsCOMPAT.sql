/* NOTE: Compatible w/ SQL server 2008 R2 */
CREATE TABLE #mergeTest
(
	[id] int NOT NULL,
	[data] date,
	[from] date NOT NULL,
	[to] date NOT NULL
);

INSERT INTO #mergeTest ([id],[data],[from],[to]) VALUES		--testing null data value handling
	(1,NULL,'2015-01-01','2015-01-05'),	--1
	(1,NULL,'2015-01-05','2015-01-10'),	--2
	(1,'2000-01-01','2015-01-10','2015-01-14'),	--3
	(1,'2000-01-03','2015-01-14','2015-01-15'),	--4
	(1,'2000-01-01','2015-01-15','2015-01-20'),	--5
	(1,'2000-01-01','2015-01-20','2015-01-22'),	--5
	(1,'2000-01-01','2015-01-22','2015-01-25'),	--6
	(1,'2000-01-01','2015-01-25','2015-01-30'),	--7
	(1,NULL,'2015-01-30','2015-02-04'),	--8
	(2,'2000-01-05','2015-01-01','2015-01-05'),	--9
	(2,'2000-01-05','2015-01-05','2015-01-10')	--10

SELECT * FROM #mergeTest 
GO
;

SELECT * INTO #tempSingle								--isolate single records. Single records need no processing.
	FROM (
		SELECT	[id], [data], MIN([from]) as [from], MIN([to]) as [to],
				COUNT([id]) as [grpsz]
		FROM #mergeTest
		GROUP BY [id], [data]) AS [selection]
	WHERE [grpsz]=1;
ALTER TABLE #tempSingle
	DROP COLUMN [grpsz];
GO
;

SELECT * INTO #tempRemainingtemp						--isolate records w/ more than 2 entries. They need to be reduced to single records
	FROM (
		SELECT	[id], [data],							--get [id] and [data] of duplicate records
				COUNT([id]) as [grpsz]
		FROM #mergeTest
		GROUP BY [id], [data]) AS [selection]
	WHERE [grpsz]>=2;
ALTER TABLE #tempRemainingTemp
	DROP COLUMN [grpsz]
SELECT * FROM #tempRemainingtemp
SELECT * INTO #temp										--get all duplicate records into #temp
	FROM (
		SELECT [b].*
		FROM #tempRemainingtemp AS [a]
		JOIN #mergeTest AS [b]
		ON		[a].[id]=[b].[id]
			AND	([a].[data]=[b].[data] OR [a].[data] IS NULL AND [b].[data] IS NULL)) AS [selection];

DROP TABLE #tempRemainingtemp;
Go
SELECT * INTO #tempRemaining
	FROM #temp;
DROP TABLE #temp;
GO
;
SELECT * FROM #tempRemaining
BEGIN
SELECT t1.*, t2.[from] as [prevfrom] INTO #temp0		--filter in records where previous 'to' date matched current 'from' date when grouped by id and data
	FROM #tempRemaining AS t1
	JOIN #tempRemaining AS t2
	ON		t2.[to] = t1.[from]
		AND	t1.[id] = t2.[id]
		AND ([t1].[data]=[t2].[data] OR [t1].[data] IS NULL AND [t2].[data] IS NULL)

SELECT t1.*, t2.[prevfrom] INTO #temp1					--add records that did not have a previous 'to' date b/c they were the extreme records in their group
	FROM #tempRemaining AS t1
	LEFT JOIN #temp0 AS t2
	ON		t1.[id]=t2.[id]
		AND	([t1].[data]=[t2].[data] OR [t1].[data] IS NULL AND [t2].[data] IS NULL)
		AND	t1.[from] = t2.[from];

DROP TABLE #temp0;

SELECT t1.*, t2.[to] as [nextto] INTO #temp2			--filter in records where current 'to' date matched next 'from' date when grouped by id and data
	FROM #temp1 AS t1
	JOIN #temp1 AS t2
	ON		t2.[from] = t1.[to]
		AND	t1.[id] = t2.[id]
		AND ([t1].[data]=[t2].[data] OR [t1].[data] IS NULL AND [t2].[data] IS NULL);

SELECT t1.*, t2.[nextto] INTO #temp						--add records that did not have a next 'from' date b/c they were the extreme records in their group
	FROM #temp1 AS t1
	LEFT JOIN #temp2 AS t2
	ON		t1.[id]=t2.[id]
		AND	([t1].[data]=[t2].[data] OR [t1].[data] IS NULL AND [t2].[data] IS NULL)
		AND	t1.[from] = t2.[from];

DROP TABLE #temp2;
DROP TABLE #temp1;

DELETE FROM #temp										--delete redundant records
	WHERE	[prevfrom] IS NOT NULL
		AND	[nextto] IS NOT NULL;

WITH cte AS (											--select records that got reduced to singles and insert them into singles account
	SELECT [id], [data], [from], [to]
		FROM [#temp]
		WHERE	[prevfrom] IS NULL
			AND	[nextto] IS NULL)
DELETE FROM cte
OUTPUT deleted.* INTO #tempSingle

/* ALL DUPLICATE RECORDS ARE NOW REDUCED TO PAIRS*/

SELECT * FROM #temp;
END

WITH cte1 AS (											--first record/set
    SELECT [id], [data], [from] , ROW_NUMBER() OVER(ORDER BY [id], [data], [from]) AS [row]
        FROM #temp
        WHERE [prevfrom] IS NULL),
    cte2 AS (											--last record/set
    SELECT [to], ROW_NUMBER() OVER(ORDER BY [id], [data], [from]) AS [row]
        FROM #temp
        WHERE [nextto] IS NULL)
SELECT t1.id, t1.data, t1.from, t2.to INTO #tempResult						--merging records and dates
    FROM cte1 AS t1
    JOIN cte2 AS t2
    ON t1.[row]=t2.[row];


TRUNCATE TABLE #mergeTest;								--insert single records and merged records into original table
INSERT INTO #mergeTest
	SELECT * FROM #tempResult;
INSERT INTO #mergeTest
	SELECT * FROM #tempSingle;

SELECT * FROM #mergeTest
	ORDER BY [id],[from];
