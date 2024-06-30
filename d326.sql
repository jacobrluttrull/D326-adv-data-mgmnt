
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

-----TESTING-----
SELECT month_from_date('2021-08-22'); -- AUGUST SHOULD BE THE ANSWER --

CREATE TABLE detailed_table_rentals_month (
    rental_date TIMESTAMP,
    category_name VARCHAR(45),
    rental_count INT,
    film_title VARCHAR(255),
    store_id INT,
    customer_id INT,
    rental_id INT,  -- Define rental_id column separately
    PRIMARY KEY(rental_id),  -- Define primary key constraint at the bottom
    FOREIGN KEY(rental_id) REFERENCES rental(rental_id)  -- Define foreign key constraint
);



CREATE TABLE summary_table_rentals_month(
	month VARCHAR,
	category_name VARCHAR(50),
	total_rentals INT,
	PRIMARY KEY(month, category_name)
);

SELECT * FROM detailed_table_rentals_month;
SELECT * FROM summary_table_rentals_month ORDER BY total_rentals ASC
LIMIT 15

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
    r.rental_date >= '2005-07-01 00:00:00'
    AND r.rental_date <= '2005-08-31 23:59:59'
GROUP BY 
    r.rental_id, r.rental_date, c.name, f.title, i.store_id, r.customer_id;


CREATE OR REPLACE FUNCTION trigger_summary()
RETURNS TRIGGER 
LANGUAGE plpgsql
AS $$
BEGIN
	DELETE FROM summary_table_rentals_month;
	INSERT INTO summary_table_rentals_month (month, category_name, total_rentals)
	SELECT month_from_date(rental_date),
	d.category_name,
	COUNT(*)
	FROM detailed_table_rentals_month d
	GROUP BY month_from_date(rental_date), d.category_name;
	RETURN NEW;
END;
$$

CREATE TRIGGER summary_trigger_fn
AFTER INSERT OR DELETE OR UPDATE ON detailed_table_rentals_month
FOR EACH STATEMENT 
EXECUTE FUNCTION trigger_summary();
		
CREATE OR REPLACE PROCEDURE rentals_refresh()
LANGUAGE plpgsql
AS $$
BEGIN
	DELETE FROM detailed_table_rentals_month;
    
    INSERT INTO detailed_table_rentals_month (rental_timestamp, category_name, rental_count, film_title, store_id, customer_id)
    SELECT
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
        r.rental_date >= '2005-07-01 00:00:00' 
        AND r.rental_date <= '2005-08-31 23:59:59'  -- Up to the last second of August 31st
    GROUP BY
        r.rental_date, c.name, f.title, i.store_id, r.customer_id;
	RETURN;
END;
$$

CALL rentals_refresh();

SELECT * FROM detailed_table_rentals_month;
SELECT * FROM summary_table_rentals_month;


