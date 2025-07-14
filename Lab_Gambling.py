import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
import pandas as pd
from sqlalchemy import create_engine, text, Date

# # Conectar a la base de datos
engine = create_engine("mysql+pymysql://root:7777@localhost:3306/IHGambling")

#Cargamos las tablas que seran usadas para las consultas SQL
apuestas = pd.read_csv("/Users/tonzo/Desktop/Data_Science_ironhack/Sesion15/apuestas.csv", low_memory=False)
apuestas['BetDate'] = pd.to_datetime(apuestas['BetDate'], format='%m/%d/%y', errors='coerce')
apuestas['BetDate'] = pd.to_datetime(apuestas['BetDate'], format='%m/%d/%y', errors='coerce')
apuestas.to_sql(
    'apuestas',
    con=engine,
    if_exists='replace',
    index=False,
    dtype={'BetDate': Date()})

productos = pd.read_csv("/Users/tonzo/Desktop/Data_Science_ironhack/Sesion15/producto.csv", low_memory=False)
productos.to_sql('productos', con=engine, if_exists='replace', index=False)

customer = pd.read_csv("/Users/tonzo/Desktop/Data_Science_ironhack/Sesion15/customer.csv", low_memory=False)
customer.to_sql('customer', con=engine, if_exists='replace', index=False)

account = pd.read_csv("/Users/tonzo/Desktop/Data_Science_ironhack/Sesion15/account.csv", low_memory=False)
account.to_sql('account', con=engine, if_exists='replace', index=False)

####
preg1 = pd.read_sql("""
    SELECT `Title`, `FirstName`, `LastName`,`DateOfBirth`
    FROM customer  
""", con=engine)
print(preg1)

###
preg2 = pd.read_sql("""
    SELECT `CustomerGroup`, COUNT(*) AS Total_group 
    FROM customer
    GROUP BY `CustomerGroup`    
""", con=engine)
print(preg2)
#En excel si tuviesemos muchos registros, lo hariamos con una tabla dinamica o pivot table.

####
preg3 = pd.read_sql("""
    SELECT c.*, a.currencycode 
    FROM customer c JOIN account a 
    ON c.CustId = a.CustId;
""", con=engine)
print(preg3)
#En Excel colocaria ambas tablas en hojas separadas y usaria la funcion XLOOKUP para traer el código de moneda desde la hoja clientes a la hoja cuentas.

##### 4
preg4 = pd.read_sql("""
    SELECT p.product, a.BetDate, SUM(a.Bet_Amt) AS total_apostado
    FROM apuestas a 
    JOIN productos p 
    ON (a.ClassId = p.CLASSID) AND (a.CategoryId = p.CATEGORYID)
    GROUP BY p.product, a.BetDate
    ;
""", con=engine)
print(preg4)
#Igual que en el punto anterior, colocariamos la informacion en. tablas separadas en distintas hojas o sheets, 
#E insertariamos una tabla dinamica usando una columna extra para combinar o unir la informacion de ambas tablas.

#### 5
apuestas.to_sql(
    'apuestas',
    con=engine,
    if_exists='replace',
    index=False,
    dtype={'BetDate': Date()}
)
    
preg5 = pd.read_sql("""
    SELECT p.product, a.BetDate, SUM(a.Bet_Amt) AS total_apostado
    FROM apuestas a 
    JOIN productos p 
      ON a.ClassId = p.CLASSID AND a.CategoryId = p.CATEGORYID
    WHERE a.BetDate >= '2012-11-01' AND p.product = 'Sportsbook'
    GROUP BY p.product, a.BetDate
    ORDER BY a.BetDate;
""", con=engine)

print(preg5)

###En Excel podriamos usar una tabla dinamica, en la que agrupariamos por fceha y luego por producto

#### 6 todos los productos pero divididos por el código de moneda y el grupo de clientes del cliente, en lugar de por día y producto.
# # También le gustaría solo transacciones que ocurrieron después del 1 de diciembre
preg6 = pd.read_sql("""
    SELECT 
        p.product,
        ac.CurrencyCode,
        ac.CustId,
        a.BetDate,
        SUM(a.Bet_Amt) AS total_apostado
    FROM apuestas a 
    JOIN productos p 
      ON a.ClassId = p.CLASSID AND a.CategoryId = p.CATEGORYID
    JOIN account ac
      ON a.AccountNo = ac.AccountNo
    WHERE a.BetDate >= '2012-12-01'
    GROUP BY p.product, ac.CurrencyCode, ac.CustId,a.BetDate
    ORDER BY p.product, ac.CurrencyCode, ac.CustId;
""", con=engine)
print(preg6)


#### 7 consulta SQL que muestre a todos los jugadores Título, Nombre y Apellido 
# y un resumen de su cantidad de apuesta para el período completo de noviembre.

preg7 = pd.read_sql("""
SELECT 
    c.Title, 
    c.FirstName, 
    c.LastName,
    a.BetDate,
    COUNT(a.BetCount) as Num_apuestas_Noviembre
FROM 
    customer c
LEFT JOIN 
    account ac ON c.CustId = ac.CustId
LEFT JOIN 
    apuestas a 
    ON a.AccountNo = ac.AccountNo 
    AND a.BetDate BETWEEN '2012-11-01' AND '2012-12-01'
GROUP BY 
    c.Title, c.FirstName, c.LastName, a.BetDate;
                    """, con=engine)
print(preg7)

#### 8
preg8_1 = pd.read_sql("""
SELECT 
    a.AccountNo,
    COUNT(DISTINCT p.product) AS productos_distintos
FROM apuestas a
JOIN productos p
  ON a.ClassId = p.CLASSID AND a.CategoryId = p.CATEGORYID
GROUP BY a.AccountNo
ORDER BY productos_distintos DESC;
                    """, con=engine)
print(preg8_1)

preg8_2 = pd.read_sql("""
SELECT AccountNo
FROM (
    SELECT a.AccountNo, COUNT(DISTINCT p.product) AS cuenta
    FROM apuestas a
    JOIN productos p
      ON a.ClassId = p.CLASSID AND a.CategoryId = p.CATEGORYID
    WHERE p.product IN ('Sportsbook', 'Vegas')
    GROUP BY a.AccountNo
) sub
WHERE cuenta = 2;
""", con=engine)

print(preg8_2)

#### 9
preg9 = pd.read_sql("""
  SELECT
    a.AccountNo,
    SUM(CASE 
          WHEN p.product = 'sportsbook' AND a.bet_amt > 0 
          THEN a.bet_amt 
          ELSE 0 
        END) AS sportsbook_total,
    SUM(CASE 
          WHEN p.product <> 'sportsbook' AND a.bet_amt > 0 
          THEN a.bet_amt 
          ELSE 0 
        END) AS other_products_total
  FROM apuestas a
  JOIN productos p
    ON a.ClassId    = p.CLASSID
   AND a.CategoryId = p.CATEGORYID
  WHERE a.bet_amt > 0
  GROUP BY a.AccountNo
  HAVING SUM(CASE WHEN p.product = 'sportsbook' THEN 1 ELSE 0 END) > 0
     AND SUM(CASE WHEN p.product <> 'sportsbook' THEN 1 ELSE 0 END) = 0
  ORDER BY sportsbook_total DESC;
""", con=engine)
print(preg9)


#### 10
preg10 = pd.read_sql("""
SELECT 
    c.FirstName,
    c.LastName,
    c.CustomerGroup,
    fav.AccountNo,
    fav.product,
    fav.total_apostado
FROM (
    SELECT 
        a.AccountNo,
        p.product,
        SUM(a.Bet_Amt) AS total_apostado,
        ROW_NUMBER() OVER (
            PARTITION BY a.AccountNo 
            ORDER BY SUM(a.Bet_Amt) DESC
        ) AS rn
    FROM apuestas a
    JOIN productos p 
      ON a.ClassId = p.CLASSID AND a.CategoryId = p.CATEGORYID
    GROUP BY a.AccountNo, p.product
) fav
JOIN account ac ON fav.AccountNo = ac.AccountNo
JOIN customer c ON ac.CustId = c.CustId
WHERE fav.rn = 1
ORDER BY c.LastName, c.FirstName;
""", con=engine)
print(preg10)


#Mirando los datos abstractos en la pestaña "Student_School" en la hoja de cálculo de Excel, 
#por favor responde las siguientes preguntas:
student = pd.read_csv("/Users/tonzo/Desktop/Data_Science_ironhack/Sesion15/Student_school.csv", low_memory=False)
student.to_sql('student', con=engine, if_exists='replace', index=False)

school = pd.read_csv("/Users/tonzo/Desktop/Data_Science_ironhack/Sesion15/school.csv", low_memory=False)
school.to_sql('school', con=engine, if_exists='replace', index=False)

#### 11 >> consulta que devuelva a los 5 mejores estudiantes basándose en el GPA
preg11 = pd.read_sql("""
    SELECT *
    FROM student
    ORDER BY GPA DESC
    LIMIT 5;
""", con=engine)
print(preg11)


#### 12
preg12 = pd.read_sql("""
    SELECT sc.school_id, sc.school_name ,COUNT(st.student_id) AS number_of_students
    FROM school sc
    LEFT JOIN student st
      ON sc.school_id = st.school_id
    GROUP BY sc.school_id, sc.school_name
    ORDER BY sc.school_name;
""", con=engine)
print(preg12)

# #### 13 
preg13 = pd.read_sql("""
  SELECT
    ranking.school_name,
    ranking.student_name,
    ranking.GPA
  FROM (
    SELECT
      sc.school_name,
      st.student_name,
      st.GPA,
      ROW_NUMBER() OVER (
        PARTITION BY sc.school_id
        ORDER BY st.GPA DESC
      ) AS rn
    FROM student st
    JOIN school sc
      ON st.school_id = sc.school_id
  ) AS ranking
  WHERE rn <= 3
  ORDER BY ranking.school_name, ranking.GPA DESC;
""", con=engine)
print(preg13)