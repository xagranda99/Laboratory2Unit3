--Creacion de la dimension Customers
CREATE TABLE IF NOT EXISTS public.dim_customers
(
    "CustomerId" integer NOT NULL,
    "FirstName" character varying(40) NOT NULL,
    "LastName" character varying(20) NOT NULL,
    "Company" character varying(80) NOT NULL,
    "Address" character varying(70) NOT NULL,
    "City" character varying(40) NOT NULL,
    "State" character varying(40) NOT NULL,
    "Country" character varying(40) NOT NULL,
    "PostalCode" character varying(10) NOT NULL,
    "Phone" character varying(24) NOT NULL,
    "Fax" character varying(24) NOT NULL,
    "Email" character varying(60) NOT NULL,
    CONSTRAINT dim_customers_pkey PRIMARY KEY ("CustomerId")
)

--Creacion de la dimension Employees
CREATE TABLE IF NOT EXISTS public.dim_employees
(
    "EmployeeId" integer NOT NULL,
    "LastName" character varying(20) NOT NULL,
    "FirstName" character varying(20) NOT NULL,
    "Title" character varying(30) NOT NULL,
    "BirthDate" character varying(25) NOT NULL,
    "HireDate" character varying(25) NOT NULL,
    "Address" character varying(70) NOT NULL,
    "City" character varying(40) NOT NULL,
    "State" character varying(40) NOT NULL,
    "Country" character varying(40) NOT NULL,
    "PostalCode" character varying(10) NOT NULL,
    "Phone" character varying(24) NOT NULL,
    "Fax" character varying(24) NOT NULL,
    "Email" character varying(60) NOT NULL,
    CONSTRAINT dim_employees_pkey PRIMARY KEY ("EmployeeId")
)

--Creacion de la dimension Artists
CREATE TABLE IF NOT EXISTS public.dim_artists
(
    "ArtistId" integer NOT NULL,
    "ArtistName" character varying(150) NOT NULL,
    CONSTRAINT dim_artists_pkey PRIMARY KEY ("ArtistId")
)

--Creacion de la dimension Tracks
CREATE TABLE IF NOT EXISTS public.dim_tracks
(
    "TrackId" integer NOT NULL,
    "TrackName" character varying(200) NOT NULL,
    "Album" character varying(200) NOT NULL,
    "Genre" character varying(100) NOT NULL,
    "MediaType" character varying(200) NOT NULL,
    "Composer" character varying(200) NOT NULL,
    "Milliseconds" bigint NOT NULL,
    "Bytes" bigint NOT NULL,
    "Price" numeric(5, 2) NOT NULL,
    PRIMARY KEY ("TrackId")
);


--Creacion de la tabla de hechos Fact_Sales
CREATE TABLE IF NOT EXISTS public.fact_sales
(
    "FactId" integer NOT NULL,
    "InvoiceId" integer NOT NULL,
    "CustomerId" integer NOT NULL,
    "EmployeeId" integer NOT NULL,
    "TrackId" integer NOT NULL,
    "ArtistId" integer NOT NULL,
    "LocationId" integer NOT NULL,
    "TimeId" integer NOT NULL,
    "Price" numeric(5, 2) NOT NULL,
    PRIMARY KEY ("FactId")
);


--Creacion de la dimension Tiempo
CREATE TABLE dim_time
(
  date_dim_id              INT NOT NULL,
  date_actual              DATE NOT NULL,
  epoch                    BIGINT NOT NULL,
  day_suffix               VARCHAR(4) NOT NULL,
  day_name                 VARCHAR(9) NOT NULL,
  day_of_week              INT NOT NULL,
  day_of_month             INT NOT NULL,
  day_of_quarter           INT NOT NULL,
  day_of_year              INT NOT NULL,
  week_of_month            INT NOT NULL,
  week_of_year             INT NOT NULL,
  week_of_year_iso         CHAR(10) NOT NULL,
  month_actual             INT NOT NULL,
  month_name               VARCHAR(9) NOT NULL,
  month_name_abbreviated   CHAR(3) NOT NULL,
  quarter_actual           INT NOT NULL,
  quarter_name             VARCHAR(9) NOT NULL,
  year_actual              INT NOT NULL,
  first_day_of_week        DATE NOT NULL,
  last_day_of_week         DATE NOT NULL,
  first_day_of_month       DATE NOT NULL,
  last_day_of_month        DATE NOT NULL,
  first_day_of_quarter     DATE NOT NULL,
  last_day_of_quarter      DATE NOT NULL,
  first_day_of_year        DATE NOT NULL,
  last_day_of_year         DATE NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             BOOLEAN NOT NULL
);

--Creacion de la dimension Location
CREATE TABLE IF NOT EXISTS public.dim_location
(
    "LocationId" integer NOT NULL,
    "Address" character varying(60) COLLATE pg_catalog."default" NOT NULL,
    "City" character varying(60) COLLATE pg_catalog."default" NOT NULL,
    "State" character varying(40) COLLATE pg_catalog."default" NOT NULL,
    "Country" character varying(40) COLLATE pg_catalog."default" NOT NULL,
    "PostalCode" character varying(10) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT dim_location_pkey PRIMARY KEY ("LocationId")
)


--//////////////////////////////////////////////////////////////////////////////////////////////////////////
--Generacion de datos de la dimension tiempo: fechas desde el 2009/01/01 hasta 10 anios despues de la ultima factura.
INSERT INTO dim_time
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_dim_id,
       datum AS date_actual,
       EXTRACT(EPOCH FROM datum) AS epoch,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'TMDay') AS day_name,
       EXTRACT(ISODOW FROM datum) AS day_of_week,
       EXTRACT(DAY FROM datum) AS day_of_month,
       datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS day_of_quarter,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'TMMonth') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(YEAR FROM datum) AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
           ELSE FALSE
           END AS weekend_indr
FROM (SELECT '2009-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 5477) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;

COMMIT;

--Relaciones de la tabla de hechos
ALTER TABLE public.fact_sales
    ADD CONSTRAINT fk_customerid FOREIGN KEY ("CustomerId")
    REFERENCES public.dim_customers ("CustomerId")
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE public.fact_sales
    ADD CONSTRAINT fk_employeeid FOREIGN KEY ("EmployeeId")
    REFERENCES public.dim_employees ("EmployeeId")
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE public.fact_sales
    ADD CONSTRAINT fk_trackid FOREIGN KEY ("TrackId")
    REFERENCES public.dim_tracks ("TrackId")
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE public.fact_sales
    ADD CONSTRAINT fk_artistid FOREIGN KEY ("ArtistId")
    REFERENCES public.dim_artists ("ArtistId")
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE public.fact_sales
    ADD CONSTRAINT fk_locationid FOREIGN KEY ("LocationId")
    REFERENCES public.dim_location ("LocationId")
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE public.fact_sales
    ADD CONSTRAINT fk_timeid FOREIGN KEY ("TimeId")
    REFERENCES public.dim_time (date_dim_id)
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;