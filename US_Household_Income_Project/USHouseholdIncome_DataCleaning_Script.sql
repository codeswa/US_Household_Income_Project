# US HOUSEHOLD PROJECT:

SELECT * FROM us_household_income;
SELECT * FROM us_household_income_statistics;

ALTER TABLE us_household_income_statistics RENAME COLUMN `ï»¿id` TO `id`;

SELECT COUNT(id) FROM us_household_income;
SELECT COUNT(id) FROM us_household_income_statistics;

-- Identify duplicates in us_household_income
SELECT id, COUNT(id) FROM us_household_income GROUP BY id HAVING COUNT(id) > 1; -- There are 6 duplicates

-- Remove duplicates in us_household_income. There is a unique column `row_id` to use for removing duplicates.
DELETE FROM us_household_income WHERE row_id IN (
SELECT row_id FROM (
SELECT row_id, id, ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) AS row_num
FROM us_household_income) duplicates
WHERE row_num > 1);

-- Identify duplicates in us_household_income_statistics
SELECT id, COUNT(id) FROM us_household_income_statistics GROUP BY id HAVING COUNT(id) > 1; -- No duplicates

-- Identify naming mistakes
SELECT State_Name, COUNT(State_Name) FROM us_household_income GROUP BY State_Name; -- Naming mistakes in column State_Name

-- Identified naming mistakes in State_Name `Alabama` and `Georgia` above.
-- UPDATE the names for the naming mistakes in State_Name:

UPDATE us_household_income
SET State_Name = 'Georgia'
WHERE State_Name = 'georia';

UPDATE us_household_income
SET State_Name = 'Alabama'
WHERE State_Name = 'alabama';

SELECT * FROM us_household_income WHERE Place = ''; -- populate valid data in blank Place
UPDATE us_household_income
SET Place = 'Autaugaville'
WHERE County = 'Autauga County'
AND City = 'Vinemont';

SELECT Type, COUNT(Type) FROM us_household_income GROUP BY Type;
UPDATE us_household_income
SET Type = 'Borough'
WHERE Type = 'Boroughs';

SELECT DISTINCT AWater
FROM us_household_income
WHERE (AWater IN ('',0,NULL)); -- Only 0 values in the table for AWater. Some places do have 0 water

SELECT DISTINCT ALand
FROM us_household_income
WHERE (ALand IN ('',0,NULL)); -- Only 0 values in the table for ALand. Some places do have 0 land

SELECT AWater, ALand
FROM us_household_income
WHERE (AWater IN ('',0,NULL))
AND (ALand IN ('',0,NULL)); -- No values in the table for both

# AUTOMATED DATA CLEANING PROJECT:

SELECT * FROM us_household_income;
DROP PROCEDURE IF EXISTS copy_and_clean_data_US_household;

DELIMITER $$
CREATE PROCEDURE copy_and_clean_data_US_household()
BEGIN
-- Creating a table for cleaned data
	CREATE TABLE IF NOT EXISTS `us_household_income_Cleaned` (
	  `row_id` int DEFAULT NULL,
	  `id` int DEFAULT NULL,
	  `State_Code` int DEFAULT NULL,
	  `State_Name` text,
	  `State_ab` text,
	  `County` text,
	  `City` text,
	  `Place` text,
	  `Type` text,
	  `Primary` text,
	  `Zip_Code` int DEFAULT NULL,
	  `Area_Code` int DEFAULT NULL,
	  `ALand` int DEFAULT NULL,
	  `AWater` int DEFAULT NULL,
	  `Lat` double DEFAULT NULL,
	  `Lon` double DEFAULT NULL,
	  `TimeStamp` timestamp DEFAULT NULL     -- added this new column to have a timestamp while debug
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    
-- Copy data to new table
	INSERT INTO us_household_income_Cleaned
    SELECT *, current_timestamp
    FROM us_household_income;

-- Data Cleaning Steps:
-- Remove duplicates in us_household_income. There is a unique column `row_id` to use for removing duplicates.
	DELETE FROM us_household_income_Cleaned WHERE row_id IN (
	SELECT row_id FROM (
	SELECT row_id, id, ROW_NUMBER() OVER(PARTITION BY id, `TimeStamp` ORDER BY id, `TimeStamp`) AS row_num  
    -- `TimeStamp` added so that in next copy of data only duplicates based on both id and timstamp is removed.
	FROM us_household_income_Cleaned) duplicates
	WHERE row_num > 1);

-- UPDATE the names for the naming mistakes in State_Name:
	UPDATE us_household_income_Cleaned
	SET State_Name = 'Georgia'
	WHERE State_Name = 'georia';

	UPDATE us_household_income_Cleaned
	SET State_Name = 'Alabama'
	WHERE State_Name = 'alabama';

	UPDATE us_household_income_Cleaned
	SET Place = 'Autaugaville'
	WHERE County = 'Autauga County'
	AND City = 'Vinemont';

	UPDATE us_household_income
	SET Type = 'Borough'
	WHERE Type = 'Boroughs';
END $$
DELIMITER ;

CALL copy_and_clean_data_US_household();

-- Debugging or checking stored procedure works:
/* Perform below checks on both us_household_income and us_household_income_Cleaned. Then see the difference. */

-- Check duplicates count in us_household_income.
SELECT row_id FROM (
SELECT row_id, id, ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) AS row_num
FROM us_household_income) duplicates                  -- Later do the same for us_household_income_Cleaned
WHERE row_num > 1;            -- 6 rows of duplicates

-- Check the row count
SELECT COUNT(row_id) FROM us_household_income;         -- Total row count

-- Check the name mistake in Georgia,etc
SELECT State_Name, COUNT(State_Name) FROM us_household_income GROUP BY State_Name;  -- Georgia, CDP, etc

# CREATE EVENT:
DROP EVENT IF EXISTS event_for_procedure_data_cleaning;

DELIMITER $$
CREATE EVENT event_for_procedure_data_cleaning
ON SCHEDULE EVERY 30 DAY
DO 
BEGIN
	CALL copy_and_clean_data_US_household();
END $$

# CREATE TRIGGER:
DELIMITER $$
CREATE TRIGGER transfer_clean_data
AFTER INSERT ON us_household_income
FOR EACH ROW
BEGIN
	CALL copy_and_clean_data_US_household(); -- Commit statements like CREATE TABLE in the stored procedure 'copy_and_clean_data_US_household' is not allowed inside TRIGGER. Hence cannot use TRIGGER for this case.
END $$
DELIMITER ;
