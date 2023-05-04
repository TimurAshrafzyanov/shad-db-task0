------- schema

DROP SCHEMA IF EXISTS shad_db CASCADE;
CREATE SCHEMA IF NOT EXISTS shad_db;

DROP TABLE IF EXISTS shad_db.employees;
CREATE TABLE IF NOT EXISTS shad_db.employees(
    id            SERIAL NOT NULL PRIMARY KEY,
    director_id   INTEGER,
    username      TEXT NOT NULL,
    usersurname   TEXT NOT NULL
);

------- task 1

DROP FUNCTION IF EXISTS shad_db.insert_employee;
CREATE OR REPLACE FUNCTION shad_db.insert_employee(
    username      TEXT,
    usersurname   TEXT,
    director_id   INTEGER
) RETURNS VOID AS $$
    BEGIN
        INSERT INTO shad_db.employees(username, usersurname, director_id)
        VALUES(username, usersurname, COALESCE(director_id, -1));
    END;
$$ LANGUAGE plpgsql;

SELECT shad_db.insert_employee('Oliver', 'Quine', NULL);
SELECT shad_db.insert_employee('John', 'Diggle', 1);
SELECT shad_db.insert_employee('Barry', 'Allen', 1);
SELECT shad_db.insert_employee('Filisity', 'Smoke', 1);
SELECT shad_db.insert_employee('Cisco', 'Ramon', 3);
SELECT shad_db.insert_employee('Kateline', 'Snow', 3);
SELECT shad_db.insert_employee('Kara', 'El', 4);
SELECT shad_db.insert_employee('Kal', 'El', 4);
SELECT shad_db.insert_employee('Diana', 'Prince', 7);
SELECT shad_db.insert_employee('Artur', 'Karry', 7);


------- helper for 2, 5, 8, 10

DROP VIEW IF EXISTS shad_db.self_place_stats;
CREATE OR REPLACE RECURSIVE VIEW shad_db.self_place_stats(
    employee_id, full_name, employee_rank, directors_list, directors_id_array
) AS
    SELECT
        id AS employee_id,
        username || ' ' || usersurname AS full_name,
        1 AS employee_rank,
        username || ' ' || usersurname AS directors_list,
        ARRAY[id] AS directors_id_array
    FROM shad_db.employees
    WHERE director_id = -1
    
    UNION ALL
    
        SELECT
            e.id,
            username || ' ' || usersurname AS full_name,
            sps.employee_rank + 1 AS employee_rank,
            (
                sps.directors_list || ' > ' || username || ' ' || usersurname
            ) AS directors_list,
            ARRAY_APPEND(sps.directors_id_array, e.id)
        FROM shad_db.employees AS e
        INNER JOIN self_place_stats AS sps ON e.director_id = sps.employee_id;


------- task 2

DROP FUNCTION IF EXISTS shad_db.move_employee;
CREATE OR REPLACE FUNCTION shad_db.move_employee(
    empl_id      INTEGER,
    new_director_id  INTEGER
) RETURNS VOID AS $$
    DECLARE
        directors_array INTEGER ARRAY DEFAULT ARRAY[]::INTEGER[];
        old_director_id INTEGER := -1;
    BEGIN
        directors_array := (
            SELECT directors_id_array
            FROM shad_db.self_place_stats
            WHERE employee_id = new_director_id
        );
        old_director_id := (
            SELECT director_id
            FROM shad_db.employees
            WHERE id = empl_id
        );
        UPDATE shad_db.employees
        SET director_id = new_director_id
        WHERE id = empl_id;
        
        IF ARRAY_POSITION(directors_array, empl_id) IS NOT NULL
        THEN 
            UPDATE shad_db.employees
            SET director_id = old_director_id
            WHERE id = new_director_id;
        END IF;
    END;
$$ LANGUAGE plpgsql;

SELECT shad_db.move_employee(9, 5);


------- task 3

DROP FUNCTION IF EXISTS shad_db.get_group;
CREATE OR REPLACE FUNCTION shad_db.get_group(
    dir_id  INTEGER
) RETURNS TABLE(director_name TEXT, group_employees TEXT[]) AS $$
    BEGIN
        director_name := (
            SELECT username || ' ' || usersurname
            FROM shad_db.employees
            WHERE id = dir_id
        );
        group_employees = (
            SELECT ARRAY_AGG(username || ' ' || usersurname ORDER BY id)
            FROM shad_db.employees
            WHERE director_id = dir_id
        );
        RETURN NEXT;
    END;
$$ LANGUAGE plpgsql;

SELECT * FROM shad_db.get_group(1);


------- helper for 4

DROP VIEW IF EXISTS shad_db.self_employees_stats;
CREATE OR REPLACE VIEW shad_db.self_employees_stats(
    employee_id, full_name, employees_count
) AS
    SELECT
        dir.id AS employee_id,
        dir.username || ' ' || dir.usersurname,
        COUNT(*) AS employees_count
    FROM shad_db.employees AS empl
    INNER JOIN shad_db.employees AS dir ON empl.director_id = dir.id
    GROUP BY dir.id;


------- task 4

DROP FUNCTION IF EXISTS shad_db.get_leaf_employees;
CREATE OR REPLACE FUNCTION shad_db.get_leaf_employees() RETURNS TABLE(employee_name TEXT) AS $$
    BEGIN
        RETURN QUERY (
            SELECT username || ' ' || usersurname AS employee_name
            FROM shad_db.employees
            WHERE id NOT IN (
                SELECT employee_id
                FROM shad_db.self_employees_stats
            )
        );
    END;
$$ LANGUAGE plpgsql;

SELECT shad_db.get_leaf_employees();


------- task 5

DROP FUNCTION IF EXISTS shad_db.get_directors_list;
CREATE OR REPLACE FUNCTION shad_db.get_directors_list(
    empl_id  INTEGER
) RETURNS TEXT AS $$
    BEGIN
        RETURN (
            SELECT directors_list
            FROM shad_db.self_place_stats
            WHERE employee_id = empl_id
        );
    END;
$$ LANGUAGE plpgsql;

SELECT shad_db.get_directors_list(5);


------- task 6

DROP FUNCTION IF EXISTS shad_db.get_group_size;
CREATE OR REPLACE FUNCTION shad_db.get_group_size(
    group_id  INTEGER
) RETURNS INTEGER AS $$
    BEGIN
        RETURN (
            WITH RECURSIVE employees_in_group AS (
               SELECT group_id AS employee_id
            
               UNION
            
               SELECT id
               FROM shad_db.employees AS e
               INNER JOIN employees_in_group AS eig ON e.director_id = eig.employee_id
            )
            SELECT COUNT(*) FROM employees_in_group
        );
    END;
$$ LANGUAGE plpgsql;

SELECT shad_db.get_group_size(10);


------- task 7

DROP FUNCTION IF EXISTS shad_db.validate_graph;
CREATE OR REPLACE FUNCTION shad_db.validate_graph() RETURNS VOID AS $$
    DECLARE
        roots_count INTEGER := -1;
        nodes_count INTEGER := -1;
        curr_id INTEGER := -1;
        iter_id INTEGER := -1;
        counter INTEGER := -1;
    BEGIN
        roots_count := (
            SELECT COUNT(*)
            FROM shad_db.employees
            WHERE director_id = -1
        );
        IF roots_count > 1
        THEN 
            RAISE EXCEPTION 'Only one root is possible, got %', roots_count;
        END IF;
        
        nodes_count := (
            SELECT COUNT(*)
            FROM shad_db.employees
        );
        
        FOR curr_id IN (SELECT id FROM shad_db.employees) LOOP
            counter := 0;
            iter_id := curr_id;
            WHILE (counter < nodes_count + 2 AND iter_id IS NOT NULL AND iter_id != -1) LOOP
                iter_id = (
                    SELECT director_id
                    FROM shad_db.employees
                    WHERE id = iter_id
                );
                counter := counter + 1;
            END LOOP;
            
            IF iter_id IS NOT NULL AND iter_id != -1
            THEN 
                RAISE EXCEPTION 'There is a cycle, employee with id % is in it', iter_id;
            END IF;
        END LOOP;
    END;
$$ LANGUAGE plpgsql;

SELECT shad_db.validate_graph();


------- task 8

DROP FUNCTION IF EXISTS shad_db.get_rank;
CREATE OR REPLACE FUNCTION shad_db.get_rank(
    empl_id  INTEGER
) RETURNS INTEGER AS $$
    BEGIN
        RETURN (
            SELECT employee_rank
            FROM shad_db.self_place_stats
            WHERE employee_id = empl_id
        );
    END;
$$ LANGUAGE plpgsql;

SELECT shad_db.get_rank(10);


------- task 9

DROP FUNCTION IF EXISTS shad_db.draw;
CREATE OR REPLACE FUNCTION shad_db.draw() RETURNS TEXT AS $$
    DECLARE
        stack_array INTEGER ARRAY DEFAULT ARRAY[]::INTEGER[];
        stack_deep TEXT ARRAY DEFAULT ARRAY[]::TEXT[];
        curr_id INTEGER := -1;
        curr_deep TEXT := '';
        curr_name TEXT := '';
        result TEXT := '';
    BEGIN
        stack_array := ARRAY(
            SELECT id
            FROM shad_db.employees
            WHERE director_id = -1
        );
        stack_deep := ARRAY(
            SELECT ''
            FROM shad_db.employees
            WHERE director_id = -1
        );
        WHILE CARDINALITY(stack_array) > 0 LOOP
            curr_id := stack_array[ARRAY_UPPER(stack_array, 1)];
            curr_deep := stack_deep[ARRAY_UPPER(stack_deep, 1)];
            stack_array := TRIM_ARRAY(stack_array, 1);
            stack_deep := TRIM_ARRAY(stack_deep, 1);
            stack_array := ARRAY_CAT(
                stack_array,
                ARRAY(
                    SELECT id
                    FROM shad_db.employees
                    WHERE director_id = curr_id
                )
            );
            stack_deep := ARRAY_CAT(
                stack_deep,
                ARRAY(
                    SELECT curr_deep || '   '
                    FROM shad_db.employees
                    WHERE director_id = curr_id
                )
            );
            curr_name := ARRAY_TO_STRING(
                ARRAY(
                    SELECT username || ' ' || usersurname
                    FROM shad_db.employees
                    WHERE id = curr_id
                ),
                ''
            );
            result := result || curr_deep || curr_name || '\n';
        END LOOP;
        RETURN result;
    END;
$$ LANGUAGE plpgsql;

SELECT shad_db.draw();


------- task 10

DROP FUNCTION IF EXISTS shad_db.get_employee_path;
CREATE OR REPLACE FUNCTION shad_db.get_employee_path(
    first_id   INTEGER,
    second_id  INTEGER
) RETURNS TEXT AS $$
    DECLARE
        first_rank INTEGER := -1;
        second_rank INTEGER := -1;
        tmp INTEGER := -1;
        left_result TEXT := '';
        right_result TEXT := '';
        common_name TEXT := '';
        left_name TEXT := '';
        right_name TEXT := '';
    BEGIN
        first_rank := (
            SELECT employee_rank
            FROM shad_db.self_place_stats
            WHERE employee_id = first_id
        );
        second_rank := (
            SELECT employee_rank
            FROM shad_db.self_place_stats
            WHERE employee_id = second_id
        );
        IF first_rank < second_rank
        THEN 
            tmp := first_rank;
            first_rank := second_rank;
            second_rank := tmp;
            
            tmp:= first_id;
            first_id := second_id;
            second_id = tmp;
        END IF;
        
        WHILE first_id != second_id LOOP
            right_name := (
                SELECT username || ' ' || usersurname
                FROM shad_db.employees
                WHERE id = first_id
            );
            IF right_result = ''
            THEN
                right_result := right_name;
            ELSE
                right_result := right_name || ' > ' || right_result;
            END IF;
            first_id := (
                SELECT director_id
                FROM shad_db.employees
                WHERE id = first_id
            );
            
            IF first_rank = second_rank
            THEN 
                left_name := (
                    SELECT username || ' ' || usersurname
                    FROM shad_db.employees
                    WHERE id = second_id
                );
                IF left_result = ''
                THEN
                    left_result := left_name;
                ELSE
                    left_result := left_result || ' < ' || left_name;
                END IF;
                second_id := (
                    SELECT director_id
                    FROM shad_db.employees
                    WHERE id = second_id
                );
            ELSE
                first_rank := first_rank - 1;
            END IF;
        END LOOP;
        
        common_name := (
            SELECT username || ' ' || usersurname
            FROM shad_db.employees
            WHERE id = second_id
        );
        
        IF left_result != ''
        THEN 
            left_result := left_result || ' < ';
        END IF;
        IF right_result != ''
        THEN 
            right_result := ' > ' || right_result;
        END IF;
        
        RETURN left_result || COALESCE(common_name, 'Board of Directors') || right_result;
    END;
$$ LANGUAGE plpgsql;

SELECT shad_db.get_employee_path(10, 2);