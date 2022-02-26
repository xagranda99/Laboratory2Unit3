import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from datetime import datetime
import psycopg2

def log(logfile, message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y'
    #Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(logfile,"a") as f: 
        f.write('[' + timestamp + ']: ' + message + '\n')
        print(message)

def transform():

    log(logfile, "-------------------------------------------------------------")
    log(logfile, "Inicia Fase De Transformacion")
    df_fact_sales = pd.read_sql_query("""SELECT it.InvoiceLineId as FactId,i.InvoiceId, c.CustomerId, c.SupportRepId as EmployeeId, it.TrackId, ar.ArtistId,c.CustomerId as LocationId, strftime('%Y%m%d', datetime(i.InvoiceDate)) as TimeId, it.UnitPrice*it.Quantity as Price from invoice_items it
        JOIN invoices i ON i.InvoiceId = it.InvoiceId
        JOIN customers c ON c.CustomerId = i.CustomerId
        JOIN tracks t ON t.TrackId = it.TrackId
        JOIN albums al ON al.AlbumId = t.AlbumId
        JOIN artists ar ON ar.ArtistId = al.ArtistId
        ORDER BY 1;
        """, con=engine.connect())

    df_customers = pd.read_sql_query("""SELECT 
        CustomerId, 
        FirstName, 
        LastName, 
        COALESCE(Company, 'N/A') as Company, 
        Address, 
        City, 
        COALESCE(State, 'N/A') as State, 
        Country, COALESCE(PostalCode, 'N/A') as PostalCode, 
        COALESCE(Phone, 'N/A') as Phone, 
        COALESCE(Fax, 'N/A') as Fax, 
        Email 
        FROM customers;
        """, con=engine.connect())
    df_employees = pd.read_sql_query("""SELECT 
        EmployeeId, 
        LastName, 
        FirstName, 
        Title, 
        BirthDate, 
        HireDate, 
        Address, 
        City, 
        State, 
        Country, 
        PostalCode, 
        Phone, 
        Fax, 
        Email 
        FROM employees;
        """, con=engine.connect())
    df_location = pd.read_sql_query("""SELECT 
        CustomerId as LocationId, 
        Address, 
        City, 
        COALESCE(State, 'N/A') as State, 
        Country, 
        COALESCE(PostalCode, 'N/A') as PostalCode 
        FROM customers;
        """, con=engine.connect())
    df_tracks = pd.read_sql_query("""SELECT t.TrackId, t.Name as TrackName, al.Title as Album, g.Name as Genre, mt.Name as MediaType, COALESCE(t.Composer, 'N/A') as Composer, t.Milliseconds, t.Bytes, t.UnitPrice as Price 
        FROM tracks t
        JOIN albums al on t.AlbumId = al.AlbumId
        JOIN genres g on t.GenreId = g.GenreId
        JOIN media_types mt on mt.MediaTypeId = t.MediaTypeId
        ORDER BY 1;
        """, con=engine.connect())
    df_artists = pd.read_sql_query("""select ar.ArtistId, ar.Name as ArtistName from artists ar;""", con=engine.connect())

    log(logfile, "Finaliza Fase De Transformacion")
    log(logfile, "-------------------------------------------------------------")
    return df_fact_sales,df_customers,df_employees,df_location,df_tracks,df_artists
   
def load():
    """ Connect to the PostgreSQL database server """
    conn_string = 'postgresql://postgres:172164@localhost/DW_Sales_Music'
    db = create_engine(conn_string)
    conn = db.connect()
    try:
        log(logfile, "-------------------------------------------------------------")
        log(logfile, "Inicia Fase De Carga")
        df_customers.to_sql('dim_customers', conn, if_exists='append',index=False)
        df_employees.to_sql('dim_employees', conn, if_exists='append',index=False)
        df_location.to_sql('dim_location', conn, if_exists='append',index=False)
        df_tracks.to_sql('dim_tracks', conn, if_exists='append',index=False)
        df_artists.to_sql('dim_artists', conn, if_exists='append',index=False)
        df_fact_sales.to_sql('fact_sales', conn, if_exists='append',index=False)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        log(logfile, "Finaliza Fase De Carga")
        log(logfile, "-------------------------------------------------------------")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally: 
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def extract():
    log(logfile, "--------------------------------------------------------")
    log(logfile, "Inicia Fase De Extraccion")
    metadata = MetaData()
    metadata.create_all(engine)
    log(logfile, "Finaliza Fase De Extraccion")
    log(logfile, "--------------------------------------------------------")


if __name__ == '__main__':
    
    logfile = "ProyectoETL_logfile.txt"
    log(logfile, "ETL Trabajo iniciado.")
    engine = create_engine('sqlite:///chinook.db')
    extract()
    (df_fact_sales,df_customers,df_employees,df_location,df_tracks,df_artists) = transform()
    load()
    log(logfile, "ETL Trabajo finalizado.")
