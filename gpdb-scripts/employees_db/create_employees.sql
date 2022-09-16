DROP SCHEMA IF EXISTS employees CASCADE;
CREATE SCHEMA employees;
set search_path to employees;

SELECT 'CREATING DATABASE STRUCTURE' as "INFO";

DROP TABLE IF EXISTS dept_emp,
                     dept_manager,
                     titles,
                     salaries, 
                     employees, 
                     departments
                    CASCADE;

CREATE TABLE employees (
    emp_no      INT             NOT NULL,
    birth_date  DATE            NOT NULL,
    first_name  VARCHAR(14)     NOT NULL,
    last_name   VARCHAR(16)     NOT NULL,
    gender      VARCHAR(1)      NOT NULL CHECK (gender = 'M' or gender = 'F'),    
    hire_date   DATE            NOT NULL
)
DISTRIBUTED BY (emp_no);

CREATE TABLE departments (
    dept_no     CHAR(4)         NOT NULL,
    dept_name   VARCHAR(40)     NOT NULL
)
DISTRIBUTED BY (dept_no);

CREATE TABLE dept_manager (
   dept_no      CHAR(4)         NOT NULL,
   emp_no       INT             NOT NULL,
   from_date    DATE            NOT NULL,
   to_date      DATE            NOT NULL
)
DISTRIBUTED BY (dept_no, emp_no); 

CREATE TABLE dept_emp (
    emp_no      INT             NOT NULL,
    dept_no     CHAR(4)         NOT NULL,
    from_date   DATE            NOT NULL,
    to_date     DATE            NOT NULL
)
DISTRIBUTED BY (emp_no, dept_no);

CREATE TABLE titles (
    emp_no      INT             NOT NULL,
    title       VARCHAR(50)     NOT NULL,
    from_date   DATE            NOT NULL,
    to_date     DATE
)
DISTRIBUTED BY (emp_no, title)
PARTITION BY RANGE(from_date) 
(
    PARTITION yr  START ('1985-01-01'::date)
                  END ('2003-12-31'::date) INCLUSIVE
                  EVERY ('1 year'::interval)
);

CREATE TABLE salaries (
    emp_no      INT             NOT NULL,
    salary      INT             NOT NULL,
    from_date   DATE            NOT NULL,
    to_date     DATE            NOT NULL
)
DISTRIBUTED BY (emp_no, from_date)
PARTITION BY RANGE(from_date) 
(
    PARTITION yr  START ('1985-01-01'::date)
                  END ('2003-12-31'::date) INCLUSIVE
                  EVERY ('1 year'::interval)
);

SELECT 'LOADING departments' as "INFO";
\copy departments from departments.csv delimiter '|'; 

SELECT 'LOADING employees' as "INFO";
\copy employees from employees.csv delimiter '|'; 

SELECT 'LOADING dept_emp' as "INFO";
\copy dept_emp from dept_emp.csv delimiter '|'; 

SELECT 'LOADING dept_manager' as "INFO";
\copy dept_manager from dept_manager.csv delimiter '|'; 

SELECT 'LOADING titles' as "INFO";
\copy titles from titles.csv delimiter '|'; 

SELECT 'LOADING salaries' as "INFO";
\copy salaries from salaries.csv delimiter '|'; 
