# Revision 6 del codigo
# Estructura de Datos y Algoritmia 
# Algoritmo de Ordenamiento de Base de Datos en Python
# Martin Medina 4.264.956
# David Medina  5.150.238

# Importo las librerias necesarias
import sqlite3 as sl
import pandas as pd
import psutil
import time
import csv
import os

# Utilizo un enfoque orientado a objetos
# Clase "Principal"
class Main:
    def main():
        os.system("cls" if os.name=="nt" else "clear")
        # Elimina los archivos generados cada vez que se ejecuta
        try:
            if os.path.exists("ruc/merge.txt") or os.path.exists("ruc/merge_original.txt" or os.path.exists("ruc/ruc.db")):
                os.remove("ruc/merge.txt")
                os.remove("ruc/merge_original.txt")
                os.remove("ruc/ruc.db")
        except:
            pass

        print("\033[92mAlgoritmo de Ordenamiento de Base de Datos en Python")
        print("         Estructura de Datos y Algoritmia II        ")
        print("            \033[93mMartin Medina & David Medina            ")        
        print("              4.264.956       5.150.238\033[0m\n            ")

        UnirArchivos.main()
        CrearDb.main()
        InsertarRegistros.main()
        Bubble.main()
        Insertion.main()
        Merge.main()
        TiempoEjecucion.main()

# Clase con el metodo para concatenar los archivos utilizar archivos provistos
class UnirArchivos:
    def main():
        # Inicializo el tiempo de ejecución
        tiempoInicio = time.time()
        
        # # Inicializo variables como vacias
        data0 = data1 = data2 = data3 = data4 = data5 = data6 = data7 = data8 = data9 = ""

        # Cargo los archivos a las variables especificando el tipo de encodeo utilizando solo 3 archivos
        with open("ruc/ruc0.txt",	encoding="utf-8") as fp:
            data0 += fp.read()
        # with open("ruc/ruc1.txt",	encoding="utf-8") as fp:
        #     data1 = fp.read()
        with open("ruc/ruc2.txt",	encoding="utf-8") as fp:
            data2 = fp.read()
        # with open("ruc/ruc3.txt",	encoding="utf-8") as fp:
        #     data3 = fp.read()
        with open("ruc/ruc4.txt",	encoding="utf-8") as fp:
            data4 = fp.read()
        # with open("ruc/ruc5.txt",	encoding="utf-8") as fp:
        #     data5 = fp.read()
        # with open("ruc/ruc6.txt",	encoding="utf-8") as fp:
        #     data6 = fp.read()
        # with open("ruc/ruc7.txt",	encoding="utf-8") as fp:
        #     data7 = fp.read()
        # with open("ruc/ruc8.txt",	encoding="utf-8") as fp:
        #     data8 = fp.read()
        # with open("ruc/ruc9.txt",	encoding="utf-8") as fp:
        #     data9 = fp.read()

        # # # Concateno los archivos
        # data0 += data1
        data0 += data2
        # data0 += data3
        data0 += data4
        # data0 += data5
        # data0 += data6
        # data0 += data7
        # data0 += data8
        # data0 += data9

        # Crea el nuevo archivo  
        with open ("ruc/merge_original.txt", "w", encoding="utf-8") as fp:
            fp.write(data0)

        # Utilizo el framework pandas para crear un archivo con mejor formato para la importación
        file = pd.read_csv("ruc/merge_original.txt", sep = "|")
        file.drop("Unnamed: 4", inplace=True, axis=1)

        # Guardo el archivo procesado para su carga a la base de datos
        file.to_csv("ruc/merge.txt", index=False)

        # Imprimo el tiempo total de la ejecución y el uso de cpu
        print("Los archivos se concatenaron en %s segundos." % (time.time() - tiempoInicio))        
        print("Se utilizo un {}% del procesador durante la concatenación.".format(psutil.cpu_percent()))
        print("--------------------------------------------------------------------------------------")

# Clase con el metodo para crear la base de datos
class CrearDb:
    def main():
        # Inicializo el tiempo de ejecución
        tiempoInicio = time.time()

        # Creo la conexión a la base de datos
        db = sl.connect("ruc/ruc.db")
        c = db.cursor()
        
        # Genero las tablas
        c.execute("""CREATE TABLE IF NOT EXISTS ruc(
            'NUMERO DE IDENTIFICACION' INT,
            'NOMBRE' TEXT,
            'DIGITO VERIFICADOR' CHARACTER(1),
            'RUC VIEJO' VARCHAR(12)
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS ruc_burbuja(
            'NUMERO DE IDENTIFICACION' INT,
            'NOMBRE' TEXT,
            'DIGITO VERIFICADOR' CHARACTER(1),
            'RUC VIEJO' VARCHAR(12)
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS ruc_insercion(
            'NUMERO DE IDENTIFICACION' INT,
            'NOMBRE' TEXT,
            'DIGITO VERIFICADOR' CHARACTER(1),
            'RUC VIEJO' VARCHAR(12)
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS ruc_merge(
            'NUMERO DE IDENTIFICACION' INT,
            'NOMBRE' TEXT,
            'DIGITO VERIFICADOR' CHARACTER(1),
            'RUC VIEJO' VARCHAR(12)
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS lenguaje(
            'LENGUAJE DE PROGRAMACION' VARCHAR(8),
            'METODO DE ORDENACION' VARCHAR(10),
            'TIEMPO DE EJECUCION (s)' INT
        )""")
        db.commit()

        # Imprimo el tiempo total de la ejecución
        print("La base de datos se creó en %s segundos." % (time.time() - tiempoInicio))
        print("--------------------------------------------------------------------------------------")

# Clase con el metodo para importar los registros a la base de datos
class InsertarRegistros:
    def main():
        # Inicializo el tiempo de ejecución
        tiempoInicio = time.time()
        # Creo la conexión a la base de datos
        db = sl.connect("ruc/ruc.db")
        c = db.cursor()                
        file = open("ruc\merge.txt", encoding="utf-8")
        # Asigno el contenido del archivo a una variable especificando el delimitador
        contents = list(csv.reader(file, delimiter=','))
        # Utilizo el comando executemany para insertar registros a la tablas
        insertRecords = "INSERT INTO ruc ('NUMERO DE IDENTIFICACION', 'NOMBRE', 'DIGITO VERIFICADOR', 'RUC VIEJO') VALUES (?, ?, ?, ?)"
        c.executemany(insertRecords, contents)    
        db.commit()
        db.close()
        # Imprimo el tiempo total de la ejecución
        print("Los registros se importaron en %s segundos." % (time.time() - tiempoInicio))
        print("--------------------------------------------------------------------------------------")

# Clase con el metodo para visualizar el tiempo de ejecucion de los metodos de ordenamiento
class TiempoEjecucion:
    def main():
        # Creo la conexión a la base de datos
        db = sl.connect("ruc/ruc.db")
        c = db.cursor()        
        # Llamo a la base de datos para sacar datos
        items = c.execute("SELECT * FROM lenguaje").fetchall()
        # Impresión con formato
        print("\n\033[94mLENGUAJE DE PROGRAMACION" + "\tMETODO DE ORDENACION" + "\tTIEMPO DE EJECUCION (segundos)")
        for item in items:
            print("\033[96m" + str(item[0]) + " \t\t\t        " + str(item[1]) + " \t\t" + str(item[2]))

# Clase con el metodo para solicitar n registros de la base de datos
class Query:
    def main():
        # Me conecto a la base de datos
        db = sl.connect("ruc/ruc.db")        
        # Creo un cursor
        c = db.cursor()
        # Llamamos a la base de datos utilizando SELECT limitando a 5000 registros
        items = c.execute("SELECT * FROM ruc LIMIT 5000").fetchall()
        # Cierro la conexión
        db.close()
        return items

# Clase con el metodo de ordenamiento burbuja
class Bubble:
    def bubbleSort(arr):
        global tiempoFinal
        tiempoInicio = time.time()
        
        desordenada = True

        while desordenada:
            desordenada = False
            for i in range(len(arr)-1):
                if arr[i][1] > arr[i+1][1]:
                    # Creo 4 variables temporales para almacenar los datos del vector i
                    x0 = arr[i][0]
                    y0 = arr[i][1]
                    z0 = arr[i][2]
                    a0 = arr[i][3]
                    
                    # Creo 4 variables temporales para almacenar los datos del vector i+1
                    x1 = arr[i+1][0]
                    y1 = arr[i+1][1]
                    z1 = arr[i+1][2]
                    a1 = arr[i+1][3]

                    # Asigno los valores ordenados
                    arr[i] = [x1, y1, z1, a1]
                    arr[i+1] = [x0, y0, z0, a0]
                    desordenada = True
        
        tiempoFinal = time.time() - tiempoInicio
    def main():

        db = Query.main()
        Bubble.bubbleSort(db)

        conn = sl.connect("ruc/ruc.db")
        c = conn.cursor()

        c.execute("INSERT INTO lenguaje VALUES (:lenguaje, :metodo, :tiempo)",
                {
                    'lenguaje': "Python",
                    'metodo': "Burbuja",
                    'tiempo': tiempoFinal
                })

        conn.commit()

        # Utilizo el comando executemany para insertar registros a la tabla ordenada
        insertRecords = "INSERT INTO ruc_burbuja ('NUMERO DE IDENTIFICACION', 'NOMBRE', 'DIGITO VERIFICADOR', 'RUC VIEJO') VALUES (?, ?, ?, ?)"
        c.executemany(insertRecords, db) 
        conn.commit()
        conn.close()

        print("Se ordenaron los registros con el metodo burbuja en %s segundos." % tiempoFinal)
        print("Se utilizo un {}% del procesador durante el ordenamiento burbuja.".format(psutil.cpu_percent()))
        print("--------------------------------------------------------------------------------------")

# Clase con el metodo de ordenamiento por inserción
class Insertion:
    def insertionSort(arr):
        tiempoInicio = time.time()
        global tiempoFinal

        # Recorre desde 1 hasta len(arr)
        for step in range(1, len(arr)):
            key = arr[step][1]

            j = step - 1

            # Creo 4 variables temporales para almacenar los datos del vector i+1
            x1 = arr[j+1][0]
            y1 = arr[j+1][1]
            z1 = arr[j+1][2]
            a1 = arr[j+1][3]
            
            # Compara la llave con cada elemento de la izquierda hasta encontrar un elemento menor
            while j >= 0 and key < arr[j][1]:
                # Creo 4 variables temporales para almacenar los datos del vector i
                x0 = arr[j][0]
                y0 = arr[j][1]
                z0 = arr[j][2]
                a0 = arr[j][3]
                # Intercambio valores
                arr[j + 1] = [x0, y0, z0, a0]

                j = j - 1            
            
            # Intercambio valores
            arr[j + 1] = [x1, y1, z1, a1]
        
        tiempoFinal = time.time() - tiempoInicio

    def main():
        db = Query.main()
        Insertion.insertionSort(db)

        conn = sl.connect("ruc/ruc.db")
        c = conn.cursor()

        c.execute("INSERT INTO lenguaje VALUES (:lenguaje, :metodo, :tiempo)",
                {
                    'lenguaje': "Python",
                    'metodo': "Inserción",
                    'tiempo': tiempoFinal
                })  

        conn.commit()

        # Utilizo el comando executemany para insertar registros a la tabla ordenada
        insertRecords = "INSERT INTO ruc_insercion ('NUMERO DE IDENTIFICACION', 'NOMBRE', 'DIGITO VERIFICADOR', 'RUC VIEJO') VALUES (?, ?, ?, ?)"
        c.executemany(insertRecords, db) 
        conn.commit()
        conn.close()

        print("Se ordenaron los registros por inserción en %s segundos." % tiempoFinal)
        print("Se utilizo un {}% del procesador durante el ordenamiento por inserción.".format(psutil.cpu_percent()))
        print("--------------------------------------------------------------------------------------")

# Clase con el metodo de ordenamiento rapido
class Merge:
    def mergeSort(arr):
        if len(arr) > 1:    
            # Encuentro el medio del arreglo
            mid = len(arr)//2    
            # Divido los elementos del arreglo
            L = arr[:mid]    
            # en 2 mitades
            R = arr[mid:]    
            # Ordeno la primera mitad
            Merge.mergeSort(L)    
            # Ordeno la segunda mitad
            Merge.mergeSort(R)
    
            i = j = k = 0
    
            # Copio los datos a arreglos temporales L[] y R[]
            while i < len(L) and j < len(R):
                if L[i][1] < R[j][1]:

                    arr[k] = L[i]
                    i += 1
                else:
                    arr[k] = R[j]
                    j += 1
                k += 1
    
            # Reviso si falto algun elemento
            while i < len(L):
                arr[k] = L[i]
                i += 1
                k += 1
    
            while j < len(R):
                arr[k] = R[j]
                j += 1
                k += 1

    def main():
        db = Query.main()

        tiempoInicio = time.time()

        Merge.mergeSort(db)

        tiempoFinal = time.time() - tiempoInicio

        conn = sl.connect("ruc/ruc.db")
        c = conn.cursor()

        c.execute("INSERT INTO lenguaje VALUES (:lenguaje, :metodo, :tiempo)",
                {
                    'lenguaje': "Python",
                    'metodo': "Mezcla   ",
                    'tiempo': tiempoFinal
                })  
        conn.commit()

        # Utilizo el comando executemany para insertar registros a la tabla ordenada
        insertRecords = "INSERT INTO ruc_merge ('NUMERO DE IDENTIFICACION', 'NOMBRE', 'DIGITO VERIFICADOR', 'RUC VIEJO') VALUES (?, ?, ?, ?)"
        c.executemany(insertRecords, db) 
        conn.commit()
        conn.close()

        print("Se ordenaron los registros por el metodo por mezcla en %s segundos." % tiempoFinal) 
        print("Se utilizo un {}% del procesador durante el ordenamiento por mezcla.".format(psutil.cpu_percent()))
        print("--------------------------------------------------------------------------------------")  

Main.main()
input("\n\033[0mPresione cualquier tecla para continuar...")