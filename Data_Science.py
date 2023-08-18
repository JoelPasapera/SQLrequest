import pymysql
import numpy as np
import pandas as pd
import sqlalchemy
import ast

# recuerda
# 1 todas las respuestas de sql son tablas.
# 2 las consultas NO son case sensitive (se interpreta igual mayuscula como miniscula, exepto postgre sql)
# Establecer la conexión a la base de datos

conexion = pymysql.connect(
    host="localhost",
    port=3306,
    user="root",
    passwd="Joel3630004",
    database="mysql",
)

# Crear un cursor para ejecutar las consultas
cursor = conexion.cursor()


# CREAR UNA FUNCION QUE IMPRIMA LOS RESULTADOS EN LA TERMINAL
def c(consulta):
    # ACLARACIÓN IMPORTANTE: execute() es más apropiado para consultas dinámicas, consultas SQL una vez con un conjunto de valores, consultas que afectan una sola fila y consultas que afectan múltiples tablas. Usar executemany cuando debe usarse execute, producirá un error o no se ejecutará la consulta
    try:
        cursor.execute(consulta)
        print("-----SQL-INPUT-----")
        print(f"{consulta};")
        # Obtener los resultados de la consulta
        resultados = cursor.fetchall()
        if not resultados:
            pass
        else:
            print()
            print("-----SQL-OUTPUT----")
        # Imprimir los resultados en la terminal
        for resultado in resultados:
            print(resultado)
        print()
    except Exception as e:
        print("Error al ejecutar la consulta:", e)


# Nombre de la base de datos
db = "estadistica"
# """ configuración del nombre y atributos de la tabla y mas.. """
nombre_tabla = "datos"
PrimaryKey_name = "N"
PrimaryKey_type = "int"
tipo_de_dato = "DECIMAL(10, 2)"

# para nombre_tabla = str("data").strip().title().replace(" ", "") or "numeros"
# aqui limpia la entrada porsiacaso
data_valores = (
    input("Dame los valores separados por , sin espacios:  ").strip().replace(" ", "")
)
# aqui colocar el numero de columnas o numero de atributos o la cantidad de encabezados (sin considerar el de la llave primaria solo de los datos)
atributos = int(input("Inserta el numero de columnas:  "))

# llenar la lista con la sintaxis del tipo de dato elegido
lista_columnas = []
# la cantidad de columnas
lista_encabezados = []

# SE CREA LAS COLUMNAS Y EL TIPO DE DATO
for i in range(1, atributos + 1):
    cod = f"columna{i} {tipo_de_dato}"
    cod2 = f"columna{i}"
    lista_columnas.append(cod)
    lista_encabezados.append(cod2)


def insert():
    # ACLARACIÓN IMPORTANTE: se usará executemany en lugar de execute() cuando se desee realizar consultas con múltiples conjuntos de valores. Por ejemplo insertar o actualizar múltiples filas de datos de una tabla en una sola consulta. (se inserta mediante listas), execute no puede hacerlo pero si executemany. Usar executemany cuando debe usarse execute, producirá un error o no se ejecutará la consulta.
    try:
        # Create the VALUES clause dynamically based on the number of columns
        values_clause = ",".join(["%s"] * atributos)
        # Create the INSERT statement
        consulta = f"INSERT INTO {nombre_tabla} ({','.join(lista_encabezados)}) VALUES ({values_clause})"
        print("-----SQL-INPUT-----")
        print(consulta)

        # Create a list of tuples with the values to insert
        lista_input = ast.literal_eval(f"[{data_valores}]")
        arreglo = np.array(lista_input)
        valores = []
        for i in range(0, len(arreglo), atributos):
            valores.append(tuple(arreglo[i : i + atributos]))
        # Execute the INSERT statement using executemany()
        cursor.executemany(consulta, valores)
        # Commit the changes
        conexion.commit()
    # procesar los resultados de la consulta
    except Exception as e:
        print("Error al ejecutar la consulta:", e)


# Esta clase transforma los datos de corrido str() en una matriz.
class ValuesTable:
    def __init__(self, _matriz, _concat):
        self._matriz = _matriz
        self._concat = _concat

    # Es importante tener en cuenta que el código asume que el número de elementos en la cadena de entrada es divisible por el número de columnas especificado por el usuario. Si esto no es cierto, se producirá un error al intentar crear la matriz utilizando la función reshape().
    autoincremento_inicio = 1
    # bits = np.int16  # cambiar a 64 bits si son muchos datos
    # igualando valores
    data = data_valores
    column = atributos
    # crear un vector usando un separador especifico y el tipo de bits elegido
    arreglo = np.fromstring(data, sep=",")  # .astype(bits)
    size = int(arreglo.size)
    row = int(size / column)
    matriz = arreglo.reshape(column, row)
    # Transponer la matriz
    transpose = np.transpose(matriz)
    # Convertir la matriz a cadenas de texto
    matriz_str = transpose.astype(str)
    comillas = '"'
    separador = ',"'
    # Concatenar los separadores de argumentos adelante y atrás de cada elemento de la matriz
    matriz_final = np.char.add(np.char.add(separador, matriz_str), comillas)
    # Imprimir la lista resultante
    # print(matriz_final)
    # creando la columna de la primary key
    Pkey_columna = np.full((row, 1), "(null")
    Pkey_columna[0, 0] = "(1"
    # print(Pkey_columna)
    # Creando la columna que cierra parentesis
    cerrar_columna = np.full((row, 1), "),")
    cerrar_columna[int(row - 1), 0] = ")"
    # uniendo la columna de la llave primaria con los datos
    unido = np.hstack((Pkey_columna, matriz_final, cerrar_columna))
    # print(unido)
    # convirtiendo la matriz en un texto str
    texto_bruto = np.array_str(unido.flatten())
    texto_limpio = (
        texto_bruto.replace("[", "").replace("]", "").replace("'", "").replace(" ", "")
    )
    # type_texto_limpio = type(texto_limpio)
    print(texto_limpio)

    def InfoMatriz(self):
        print("La matriz de valores se veria así: ")
        print(self._matriz)

    def InfoConcat(self):
        print(
            "El codigo MySQL para insertar en la terminal es así (copialo y pegalo): "
        )
        print(self._concat)

    def Matriz(self):
        return self._matriz

    def Concat(self):
        return self._concat


ValuesSQL = ValuesTable(ValuesTable.unido, ValuesTable.texto_limpio)
# ValuesSQL.InfoMatriz()
# print(ValuesSQL)

# /-------INICIO DE CODIGO SQL CON PYTHON -----/

# muestra la base de datos
c("show databases")

# crear la base de datos
c(f"drop database if exists {db}")
c(f"create database {db}")
# acceder a la base de datos
c(f"use {db}")
c("show databases")


# crear la tabla
c(f"drop table if exists {nombre_tabla}")


# concatenando...
atributo = ", ".join(lista_columnas)  # columa+tipo+argumentos
columnas = ", ".join(lista_encabezados)  # solo columnas
# -------GENERACION DE CODIGO SQL -------
# creacion de la tabla
codigo_total = f"create table {nombre_tabla}({PrimaryKey_name} INT(255) primary key auto_increment, {atributo})auto_increment = 1"
c(codigo_total)
# descripción de la tabla
describe = f"describe {nombre_tabla}"
c(describe)
# insertar los atributos y sus datos a la tabla
insert()
# ver los datos de todos los atributos de la tabla
ver = f"SELECT * FROM {nombre_tabla}"  # cambiar el * por el atributo deseado para mayor rendimiento
c(ver)
# Imprimir los resultados de todas las consultas en la terminal
print("datos completados exitosamente 😀")

from sqlalchemy import create_engine

# Establecer la conexión con la base de datos MySQL utilizando SQLAlchemy
engine = create_engine("mysql+pymysql://root:Joel3630004@localhost/estadistica")

# Leer los datos de la tabla utilizando Pandas
df = pd.read_sql(f"SELECT * FROM {nombre_tabla}", con=engine, index_col=PrimaryKey_name)

# Mostrar el DataFrame
print(df)


consultas = input("Deseas realizar una consulta adicional? RESPONDE: SI O NO  ")
consultass = str(consultas).strip().upper().replace(" ", "") or "NO"
if consultass == "SI":
    print(
        "inserta un numero segun tu consulta:\n 1 Obtener una fila segun su indice \n 2 Obtener filas donde los valores de una columna estén entre 2 datos numericos \n 3 cambiar el nombre de los atributos o encabezados o columnas \n 4 Agregar una nueva columna"
    )
    opcioon = int(input(" -->  "))
    if opcioon == 1:
        abc = int(input("Numero de fila a consultar:  "))
        buscar = f"SELECT * FROM {nombre_tabla} WHERE {PrimaryKey_name} = {abc}"
        c(buscar)
    elif opcioon == 2:
        abb = input("Nombre de la columna:  ")
        abc = int(input("Primer numero:  "))
        abcc = int(input("Segundo numero:  "))
        buscar = f"SELECT * FROM {nombre_tabla} WHERE {abb} BETWEEN {abc} AND {abcc}"
        c(buscar)
    elif opcioon == 3:
        nombre_columna_vieja = input("Columna a modificar:  ")
        nombre_columna_nueva = int(input("Nuevo nombre:  "))
        buscar = f"ALTER TABLE {nombre_tabla} CHANGE {nombre_columna_vieja} {nombre_columna_nueva} {tipo_de_dato}"
    elif opcioon == 4:
        pass

else:
    print("Chau")


# Cerrar la conexión a la base de datos
# conexion.close()
