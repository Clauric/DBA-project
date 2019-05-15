# Database Python assignment
# Student Name: Mark Biggar
# Student Number: G00376334

import pymysql as psq
import pymongo as pmo
import pandas as pd
import re
import time

# Note: the user name and password are assumed to be "root" for the MySQL database. If it is not, change the psq.connect("localhost",User name,Password,...) to the correct inputs
# There is a 2 second pause between the code printing an output, and returning the the main menu

df_C = None
dc_C = None

def SQL_Country():
    if df_C is None:
        conn = psq.connect("localhost","root","fi3Keepo","world",cursorclass=psq.cursors.DictCursor)
        query = "SELECT * FROM country"

        with conn:
            cursor = conn.cursor()
            cursor.execute(query)
            country = cursor.fetchall()

        df_C = pd.DataFrame(country, columns=["Code", "Name", "Continent", "Population", "HeadOfState"])

    return df_C

################################

def SQL_City():
    if dc_C is None:
        conn = psq.connect("localhost","root","fi3Keepo","world",cursorclass=psq.cursors.DictCursor)
        query = "SELECT * FROM city"

        with conn:
            cursor = conn.cursor()
            cursor.execute(query)
            city = cursor.fetchall()

        dc_C = pd.DataFrame(city, columns=["ID", "CountryCode", "District", "Population"])
    
    return dc_C

################################

def assignment():

    conn = psq.connect("localhost","root","fi3Keepo","world",cursorclass=psq.cursors.DictCursor)

    main_menu()
   
    while True:

        Input_Value = input("Please input an 'x' or a number between 1 & 7: ")

        if Input_Value in ["8", "9", "0"]:
            Input_Value = input("Please input an 'x' or a number between 1 & 7: ")
            
        elif len(Input_Value) > 1:
            Input_Value = input("Please input an 'x' or a number between 1 & 7: ")
            
        elif (Input_Value.isdigit()) == True:
            Choice = int(Input_Value)
            break

        elif Input_Value == "x" or Input_Value == "X":
            print()
            print("Thank you for taking part. Good bye")
            print()
            exit()
        
        else:
            Input_Value = input("Please input an 'x' or a number between 1 & 7: ")

    if Choice == 1:
        query = "SELECT * FROM city"

        with conn:
            cursor = conn.cursor()
            cursor.execute(query)
            city = cursor.fetchmany(15)
            
            print()
            print("First 15 entries on table")
            print()
            print("ID   :   CountryCode :   District    :   Population")
            
            for row in city:
                print(row["ID"], row["Name"]," :   ",row["CountryCode"]," :   ",row["District"]," :   ",row["Population"])     # insert spacers for legibility purposes

        Sleep_Timer()

    elif Choice == 2:

        print()
        
        sign_input = input("Please enter <, <=, =, >=, > for population check ")
        Pop_Lim = input("Please input a number for population parameter ")

        if sign_input == "<":
            query = "SELECT * FROM city Where Population < %s"
        elif sign_input == "=":
            query = "SELECT * FROM city Where Population = %s"
        elif sign_input == ">":
            query = "SELECT * FROM city Where Population > %s"
        elif sign_input == "<=":
            query = "SELECT * FROM city Where Population <= %s"
        elif sign_input == ">=":
            query = "SELECT * FROM city Where Population >= %s"
        else:
            print("Sorry, you entered an incorrect sign. Please try again")
            
        with conn:
            cursor = conn.cursor()
            cursor.execute(query, Pop_Lim)
            city = cursor.fetchall()
            
            if len(city) >= 50:
                print()
                print("This search will return ",len(city), "rows of data.")
                Ret_Lim = int(input("How many rows of data do you want to display? "))
                print()
                
                with conn:
                    cursor = conn.cursor()
                    cursor.execute(query, Pop_Lim)
                    city = cursor.fetchmany(Ret_Lim)
                    
                    print("ID   :   CountryCode :   District    :   Population")
                    for row in city:
                        print(row["ID"], row["Name"]," :   ",row["CountryCode"]," :   ",row["District"]," :   ",row["Population"])     # insert spacers for legibility purposes
                    print()
                    print(Ret_Lim,"rows of data returned, as requested.")
            
            elif len(city) == 0:
                print()
                print("This search did not return any results. Sorry.")    
            
            else:
                print()
                print("ID   :   CountryCode :   District    :   Population")
                
                for row in city:
                    print(row["ID"], row["Name"]," :   ",row["CountryCode"]," :   ",row["District"]," :   ",row["Population"])     # insert spacers for legibility purposes
                
                print()
                print("Your input returned ", len(city), "rows.")

        Sleep_Timer()
    
    elif Choice == 3:
        print()
        print("Enter new city details")
        print()

        nCity = input("New city name: ")
        CC = input("3 letter country code:  ").upper()
        nDistrict = input("District where city is located: ")
        nPop = input("Population of new city (no commas):  ")

        print()

        conn = psq.connect("localhost","root","fi3Keepo","world",cursorclass=psq.cursors.DictCursor)

        query = "INSERT INTO city (Name, CountryCode, District, Population) VALUES ( %s, %s, %s, %s);"

        with conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query, (nCity, CC, nDistrict, nPop))
                conn.commit()
                print()
                print("You just added", nCity, "in", nDistrict, "from", CC, "with a population of", nPop, "to the database.")
            
            except psq.err.IntegrityError:
                print()
                print("Country code does not exist")
            
            except psq.err.DataError:
                print()
                print("Either 1) Country code entered is more than 3 letters, or 2) comma in the population entered")

        Sleep_Timer()

    elif Choice == 6:
   
        conn = psq.connect("localhost","root","fi3Keepo","world",cursorclass=psq.cursors.DictCursor)
        query = "SELECT * FROM country"

        with conn:
            cursor = conn.cursor()
            cursor.execute(query)
            country = cursor.fetchall()

        df_C = pd.DataFrame(country, columns=["Code", "Name", "Continent", "Population", "HeadOfState"])

        print()
        cCode = input("Enter 1 - 5 letters of a country you wish to see:    ")
        
        qSix = df_C[df_C["Name"].str.contains(cCode, flags=re.IGNORECASE)]
        
        print()
        print(qSix)
        print()
        print("Your search returned", len(qSix), "rows of data")
        
        Sleep_Timer()

    elif Choice == 7:

        conn = psq.connect("localhost","root","fi3Keepo","world",cursorclass=psq.cursors.DictCursor)
        query = "SELECT * FROM country"

        with conn:
            cursor = conn.cursor()
            cursor.execute(query)
            country = cursor.fetchall()

        df_C = pd.DataFrame(country, columns=["Code", "Name", "Continent", "Population", "HeadOfState"])

        df_C["Population"] = df_C["Population"].apply(pd.to_numeric, errors="coerce")  

        print()

        Symbol = input("Please enter <, <=, =, >=, > for population check:   ")
        nPop = int(input("Enter population parameter: "))

        if Symbol == "<":
            qSeven = df_C[(df_C["Population"] < nPop)]
        elif Symbol == "=":
            qSeven = df_C[(df_C["Population"] == nPop)]
        elif Symbol == ">":
            qSeven = df_C[(df_C["Population"] > nPop)]
        elif Symbol == "<=":
            qSeven = df_C[(df_C["Population"] <= nPop)]
        elif Symbol == ">=":
            qSeven = df_C[(df_C["Population"] >= nPop)]
        else:
            print("Sorry, you entered an incorrect sign. Please start again")

        print()
        print(qSeven)
        print()
        print("Your search returned", len(qSeven), "rows of data.")

        Sleep_Timer()

################################

def main_menu():

   print()
   print("1) View 15 cities")                                          
   print("2) View cities by population")                                          
   print("3) Add new city")                                                        
   print("4) Find car be engine size")                                                    
   print("5) Add new car")
   print("6) View countries by name")
   print("7) View countries by population")
   print("x) Exit application")
   print()

################################

def Sleep_Timer():
    time.sleep(2)
    print()
    print("============================================================================================")
    print()
    assignment()

################################

def SQL_untry():
    conn = psq.connect("localhost","root","fi3Keepo","world",cursorclass=psq.cursors.DictCursor)
    query = "SELECT * FROM country"

    with conn:
        cursor = conn.cursor()
        cursor.execute(query)
        country = cursor.fetchall()

        global df 
        df = pd.DataFrame(country, columns=["Code", "Name", "Continent", "Population", "HeadOfState"])
    
################################

assignment()