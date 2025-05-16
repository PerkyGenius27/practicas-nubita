import snowflake.connector

# Parámetros de conexión
conn = snowflake.connector.connect(
    user='PERKYGENIUS27',
    password='Sopomanitanio27',
    account='bdfuagx-fv18223',
    warehouse='COMPUTE_WH',
    database='practicas_nubita',
    schema='LND',
    role='ACCOUNTADMIN'
)

cur = conn.cursor()

# 1. Crear stage temporal si no existe
cur.execute("CREATE OR REPLACE STAGE my_upload_stage;")

# 2. Subir el archivo desde local al stage
cur.execute("PUT file:///Users/davidcabreranoguera/Documents/MasterBD/practicas/synthetic_beverage_sales_data.csv @my_upload_stage AUTO_COMPRESS=TRUE;")

# 3. Crear la tabla
cur.execute("""
    CREATE OR REPLACE TABLE lnd_syn_t_beverage_sales_data (
        -- Define aquí las columnas con sus tipos. Ejemplo:
        Order_ID STRING,
        Customer_ID STRING,
        Customer_Type STRING,
        Product STRING,
        Category STRING,
        Unit_Price FLOAT,
        Quantity INT,
        Discount FLOAT,
        Total_Price FLOAT,
        Region STRING,
        Order_Date DATE
    );
""")

# 4. Cargar los datos desde el archivo comprimido
cur.execute("""
    COPY INTO lnd_syn_t_beverage_sales_data
    FROM @my_upload_stage/synthetic_beverage_sales_data.csv.gz
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
""")

cur.close()
conn.close()
