
------ SECTION B USER-DEFINED FUNCTION-----

CREATE OR REPLACE FUNCTION month_from_date(rental_date TIMESTAMP)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE 
    return_of_month TEXT;  
BEGIN
    return_of_month := TO_CHAR(rental_date,'Month');
    RETURN return_of_month;  
END;
$$;

-----TESTING FUNCTION IN SECTION B-----
SELECT month_from_date('2021-08-22 15:30:00'); -- AUGUST SHOULD BE THE ANSWER --

-----CREATING DETAILED TABLE FOR SECTION C-----
CREATE TABLE detailed_table_rentals_month (
    rental_date TIMESTAMP,
    category_name VARCHAR(45),
    rental_count INT,
    film_title VARCHAR(255),
    store_id INT,
    customer_id INT,
    rental_id INT 
);

-----CREATING SUMMARY TABLE FOR SECTION C-----
CREATE TABLE summary_table_rentals_month(
	month VARCHAR,
	category_name VARCHAR(50),
	total_rentals INT
);
-----CHECKING TABLES IN SECTION C-----
SELECT * FROM detailed_table_rentals_month;
SELECT * FROM summary_table_rentals_month ORDER BY month, total_rentals ASC ;

----- SECTION E TRIGGER FUNCTION FOR SUMMARY TABLE-----
CREATE OR REPLACE FUNCTION trigger_summary()
RETURNS TRIGGER 
LANGUAGE plpgsql
AS $$
BEGIN
   
    DELETE FROM summary_table_rentals_month;

    
    INSERT INTO summary_table_rentals_month (month, category_name, total_rentals)
    SELECT 
        month,
        category_name,
        total_rentals
    FROM (
        SELECT 
            month_from_date(d.rental_date) AS month,
            d.category_name,
            COUNT(*) AS total_rentals,
            ROW_NUMBER() OVER (PARTITION BY month_from_date(d.rental_date) ORDER BY COUNT(*) DESC) AS rank --TO SHOW TOP CATEGORY INSTEAD OF ALL CATEGORIES FOR SIMPLICITY
        FROM 
            detailed_table_rentals_month d
        GROUP BY 
            month_from_date(d.rental_date), d.category_name
    ) AS ranked
    WHERE ranked.rank = 1;

    RETURN NEW; 
END;
$$;

CREATE TRIGGER summary_trigger_fn
AFTER INSERT OR DELETE OR UPDATE ON detailed_table_rentals_month
FOR EACH STATEMENT 
EXECUTE FUNCTION trigger_summary();

-----SECTION D INSERTING THE RAW DATA INTO THE DETAIL TABLE, THIS WILL ALSO POPULATE THE SUMMARY TABLE AS WELL-----
INSERT INTO detailed_table_rentals_month (rental_id, rental_date, category_name, rental_count, film_title, store_id, customer_id)
SELECT 
    r.rental_id,
    r.rental_date,
    c.name AS category_name,
    COUNT(*) AS rental_count,
    f.title AS film_title,
    i.store_id,
    r.customer_id
FROM 
    rental r
JOIN 
    payment p ON r.rental_id = p.rental_id
JOIN 
    inventory i ON r.inventory_id = i.inventory_id
JOIN 
    film f ON i.film_id = f.film_id
JOIN 
    film_category fc ON f.film_id = fc.film_id
JOIN 
    category c ON fc.category_id = c.category_id
WHERE 
    r.rental_date >= '2005-06-01 00:00:00'
    AND r.rental_date <= '2005-09-30 23:59:59'
GROUP BY 
    r.rental_id, r.rental_date, c.name, f.title, i.store_id, r.customer_id;

-----CHECK IF DATA POPULATED CORRECTLY IN BOTH TABLES-----
SELECT * FROM detailed_table_rentals_month;
SELECT * FROM summary_table_rentals_month ORDER BY month, total_rentals ASC ;

-- INSERT INTO DETAILED TABLE--
INSERT INTO detailed_table_rentals_month (rental_date, category_name, rental_count, film_title, store_id, customer_id)
VALUES
    ('2005-09-15 10:00:00', 'Comedy', 250, 'The Great Laugh', 1, 201);


----- SECTION F STORED PROCEDURE TO REFRESH DATA -----
CREATE OR REPLACE PROCEDURE rentals_refresh()
LANGUAGE plpgsql
AS $$
BEGIN
	DELETE FROM detailed_table_rentals_month;
    
    INSERT INTO detailed_table_rentals_month (rental_id, rental_date, category_name, rental_count, film_title, store_id, customer_id)
    SELECT
		r.rental_id,
        r.rental_date,
        c.name AS category_name,
        COUNT(*) AS rental_count,
        f.title AS film_title,
        i.store_id,
        r.customer_id
    FROM
        rental r
    JOIN
        payment p ON r.rental_id = p.rental_id
    JOIN
        inventory i ON r.inventory_id = i.inventory_id
    JOIN
        film f ON i.film_id = f.film_id
    JOIN
        film_category fc ON f.film_id = fc.film_id
    JOIN
        category c ON fc.category_id = c.category_id
    WHERE
        r.rental_date >= '2005-06-01 00:00:00' 
        AND r.rental_date <= '2005-09-30 23:59:59'  -- Up to the last second of September 31st
    GROUP BY
        r.rental_id, r.rental_date, c.name, f.title, i.store_id, r.customer_id;
	RETURN;
END;
$$

CALL rentals_refresh(); -----CALLING THAT REFRESH -----

SELECT * FROM detailed_table_rentals_month; 
SELECT * FROM summary_table_rentals_month;






