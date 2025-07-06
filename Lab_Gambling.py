import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
import pandas as pd
from sqlalchemy import create_engine

#Para este desafío, utilizarás el conjunto de datos "NYC 311 Service Requests". 
#Creo la DB nyc311 en sql >> En mi caso MySQL Workbench

# Conectar a la base de datos
#Crea una conexión entre Python (pandas/SQLAlchemy) y tu base de datos MySQL local llamada nyc311, usando root y la contraseña.
engine = create_engine("mysql+pymysql://root:7777@localhost:3306/IHGambling")

# #Carga el csv en un dataframe de PANDAS
df = pd.read_csv("/Users/tonzo/Desktop/Data_Science_ironhack/Instrucciones_ejercicios/customer.csv", low_memory=False)
#print(df.head())

df.to_sql('customer', con=engine, if_exists='replace', index=False)
# Mostrar columnas
#print(df.columns)

####
# preg1 = pd.read_sql("""
#     SELECT `Title`, `FirstName`, `LastName`,`DateOfBirth`
#     FROM customer  
# """, con=engine)
# print(preg1)

####
# preg2 = pd.read_sql("""
#     SELECT `CustomerGroup`, COUNT(*) AS Total_group 
#     FROM customer
#     GROUP BY `CustomerGroup`    
# """, con=engine)
#print(preg2)
#En excel con muchos lo hariamos con una tabla dinamica o pivot table.

####
account = pd.read_csv("/Users/tonzo/Desktop/Data_Science_ironhack/Instrucciones_ejercicios/account.csv", low_memory=False)
account.to_sql('account', con=engine, if_exists='replace', index=False)
#print(account)


# preg3 = pd.read_sql("""
#     SELECT c.*, a.currencycode 
#     FROM customer c JOIN account a 
#     ON c.CustId = a.CustId;
# """, con=engine)
#print(preg3)
#En Excel ????????


##### 4
apuestas = pd.read_csv("/Users/tonzo/Desktop/Data_Science_ironhack/Instrucciones_ejercicios/apuestas.csv", low_memory=False)
apuestas.to_sql('apuestas', con=engine, if_exists='replace', index=False)
# print(apuestas)
productos = pd.read_csv("/Users/tonzo/Desktop/Data_Science_ironhack/Instrucciones_ejercicios/producto.csv", low_memory=False)
productos.to_sql('productos', con=engine, if_exists='replace', index=False)
# print(productos)

preg4 = pd.read_sql("""
    SELECT p.product, a.BetDate, SUM(a.Bet_Amt) AS total_apostado
    FROM apuestas a 
    JOIN productos p 
    ON (a.ClassId = p.CLASSID) AND (a.CategoryId = p.CATEGORYID)
    GROUP BY p.product, a.BetDate
    ;
""", con=engine)
print(preg4)
#En Excel ????????