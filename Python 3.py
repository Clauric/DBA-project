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

df_C = None													# Set dataframe to None
dc_C = None													# Set dataframe to None

def SQL_Country():											# SQL functions for country
    if df_C is None:									
        conn = psq.connect("localhost","root","root","world",cursorclass=psq.cursors.DictCursor)	# Settings to connect to the DB
        query = "SELECT * FROM country"						# Query to select everything in Country table of db

        with conn:											# Using the connecction settings
            cursor = conn.cursor()							# Create a cursor
            cursor.execute(query)							# Write the query in SQL
            country = cursor.fetchall()						# Set 'Country" as everything in table

        return pd.DataFrame(country, columns=["Code", "Name", "Continent", "Population", "HeadOfState"])	# return everything in the Country array in the 5 columns as a Pandas dataframe

################################

def SQL_City():												# SQL functions for city
    if dc_C is None:
        conn = psq.connect("localhost","root","fi3Keepo","world",cursorclass=psq.cursors.DictCursor)	# Settings to connect to the DB
        query = "SELECT * FROM city"						# Query to select everything in City table of db

        with conn:											# Using the connecction settings
            cursor = conn.cursor()							# Create a cursor
            cursor.execute(query)							# Write the query in SQL
            city = cursor.fetchall()						# Set 'City" as everything in table

        return = pd.DataFrame(city, columns=["ID", "CountryCode", "District", "Population"])	# return everything in the City array in the 5 columns as a Pandas dataframe

################################

def assignment():

#   conn = psq.connect("localhost","root","root","world",cursorclass=psq.cursors.DictCursor)	# Settings to connect to the DB

    main_menu()												# Call the menu function
   
    while True:												# I/O handling

        Input_Value = input("Please input an 'x' or a number between 1 & 7: ")

        if Input_Value in ["8", "9", "0"]:					# If the number is not available in the menu
            Input_Value = input("Please input an 'x' or a number between 1 & 7: ")
            
        elif (Input_Value.isdigit()) == True:				# If the entry is a number
            Choice = int(Input_Value)						# Convert to number
            break											# Break out of while loop

        elif Input_Value == "x" or Input_Value == "X":		# Close out function if x is presses
            print()
            print("Thank you for taking part. Good bye")
            print()
            exit()											# Exit function
        
        else:
            Input_Value = input("Please input an 'x' or a number between 1 & 7: ")	# Ask for a new number to be inputted

    if Choice == 1:											# Option to print top 15 rows of the City table									
	if dc_C is None:										# If the dataframe is empty
	    dc_C = SQL_City()									# Call the return from SQL_City function

	print()
	print(dc_C.head(15))									# Print first 15 rows of dataframe

#	query = "SELECT * FROM city"

#	with conn:
#		cursor = conn.cursor()
#		cursor.execute(query)
#		city = cursor.fetchmany(15)
            
#		print()
#		print("First 15 entries on table")
#		print()
#		print("ID   :   CountryCode :   District    :   Population")
            
#	for row in city:
#		print(row["ID"], row["Name"]," :   ",row["CountryCode"]," :   ",row["District"]," :   ",row["Population"])     # insert spacers for legibility purposes

        Sleep_Timer()										# 2 second pause before going back to top of function

    elif Choice == 2:										# Option to select inputs by population

        if dc_C is None:									# If the dataframe is empty
	    dc_C = SQL_City()									# Call the return from SQL_City function
	
	print()
        
        sign_input = input("Please enter <, <=, =, >=, > for population check ")	# Sign input for check
        Pop_Lim = input("Please input a number for population parameter ")		# Population parameter check

        if sign_input == "<":								# Less than
            qTwo = dc_C[dc_C["Population"] < Pop_Lim]		# Create new dataframe based on sign and population limit
        elif sign_input == "=":								# Equal to
            qTwo = dc_C[dc_C["Population"] == Pop_Lim		# Create new dataframe based on sign and population limit
        elif sign_input == ">":								# Greater than
            qTwo = dc_C[dc_C["Population"] > Pop_Lim		# Create new dataframe based on sign and population limit
        elif sign_input == "<=":							# Less than or equal to
            qTwo = dc_C[dc_C["Population"] <= Pop_Lim		# Create new dataframe based on sign and population limit
        elif sign_input == ">=":							# Greater than or equal to
            qTwo = dc_C[dc_C["Population"] >= Pop_Lim		# Create new dataframe based on sign and population limit
        else:												# Incorrect sign used. Start process again
            print("Sorry, you entered an incorrect sign. Please try again")
            
#	with conn:
#		cursor = conn.cursor()
#		cursor.execute(query, Pop_Lim)
#		city = cursor.fetchall()
            
            if len(dc_C) >= 30:								# Check length of return
                
		print()
                
		print("This search will return ",len(dc_C), "rows of data.")	# Print out warning
		print("How many rows of data do you want to display?")			# Offer chance to reduce number of rows returned		
		print("For % of rows, enter a number less than 1. (e.g. 0.75 for 75%)")	# Instruction for percentage of rows
		print("For number of rows, enter a whole number.")				# Instruction for number of rows
                Ret_Lim = float(input("Please make a selection:	"))		# Offer chance to reduce number of rows returned
                print()

		if Ret_Lim < 1:										# For % of rows
			print(qTwo.sample(frac = Ret_Lim)				# Print fraction of rows
			print()
			print((round(Len(dc_c) * Ret_Lim,0),"rows were returned.") 	# Specify how many rows printed out
		
		elif Ret_Lim >= 1:									# For number of rows
			print(qTwo.sample(n = Ret_Limit))
			print()
			print(Ret_Lim,"rows of data returned, as requested.")		# Specify how many rows printed

#		with conn:
#			cursor = conn.cursor()
#			cursor.execute(query, Pop_Lim)
#			city = cursor.fetchmany(Ret_Lim)
                    
#			print("ID   :   CountryCode :   District    :   Population")
#			for row in city:
#				print(row["ID"], row["Name"]," :   ",row["CountryCode"]," :   ",row["District"]," :   ",row["Population"])     # insert spacers for legibility purposes
#			print()
#			print(Ret_Lim,"rows of data returned, as requested.")
            
            elif len(dc_C) == 0:							# If length of return is 0
                print()
                print("This search did not return any results. Sorry.")    		# Print no results found
            
            else:											# All other cases
		print()
		print(dc_c)											# Print out the results
		print()
		print("Your input returned ", len(city), "rows.")	# Specify how many rows printed
		print()
#		print("ID   :   CountryCode :   District    :   Population")
                
#               for row in city:
#                    print(row["ID"], row["Name"]," :   ",row["CountryCode"]," :   ",row["District"]," :   ",row["Population"])     # insert spacers for legibility purposes
                
#                print()
#	         print("Your input returned ", len(city), "rows.")

        Sleep_Timer()										# 2 second pause before going back to top of function
    
    elif Choice == 3:										# Adding data to DB in City table
    # Note that there is no check in place to determine if the new city already exists in the database
	
	print()
        print("Enter new city details")							# Initial start
        print()

        nCity = input("New city name: ")						# New city name
        CC = input("3 letter country code:  ").upper()			# Country code for new city
        nDistrict = input("District where city is located: ")	# District where city is located
        nPop = input("Population of new city (no commas):  ")	# Population of new city

        print()
	
        conn = psq.connect("localhost","root","fi3Keepo","world",cursorclass=psq.cursors.DictCursor)	# Settings to connect to the DB

        query = "INSERT INTO city (Name, CountryCode, District, Population) VALUES ( %s, %s, %s, %s);"	# SQL insertion query

        with conn:
            try:												# See if this works
                cursor = conn.cursor()							# Create cursor in SQL
                cursor.execute(query, (nCity, CC, nDistrict, nPop))			# Execute query entering data into SQL
                conn.commit()									# Commit the query
                print()
                print("You just added", nCity, "in", nDistrict, "from", CC, "with a population of", nPop, "to the database.")	# Confirmation of entry
            
            except psq.err.IntegrityError:						# If there is an integrity error
                print()
                print("Country code does not exist")			# Print that the country code does not exist
            
            except psq.err.DataError:							# If there is a data error
                print()
                print("Either 1) Country code entered is more than 3 letters, or 2) comma in the population entered")	# Print that either of these solutions could be the problem

        Sleep_Timer()											# 2 second pause before going back to top of function

    elif Choice == 6:											# See all countries that contain some characters
   
        x = SQL_Country()										# Set x to be the SQL_country dataframe
        
        if x is None:											# If x is None
            SQL_Country()										# Run SQL_Country function
            df_C = SQL_Country()								# Make df_C equal to SQL_Country

        print()
        cCode = input("Enter 1 - 5 letters of a country you wish to see:    ")	# Enter some characters in the country name
        
        qSix = df_C[df_C["Name"].str.contains(cCode, flags=re.IGNORECASE)]		# Create a new dataframe with the country code, and ignoring case of the letters entered
        
        print()
        print(qSix)												# Print out new dataframe
        print()
        print("Your search returned", len(qSix), "rows of data.")		# Specify number of rows returned
        
        Sleep_Timer()											# 2 second pause before going back to top of function

    elif Choice == 7:											# See all countries by population

        print()

        x = SQL_Country()										# Set x to be the SQL_country dataframe
        
        if x is None:											# If x is None
            SQL_Country()										# Run SQL_Country function
            df_C = SQL_Country()								# Make df_C equal to SQL_Country

        Symbol = input("Please enter <, <=, =, >=, > for population check:   ")		# Sign to be used for determining population
        nPop = int(input("Enter population parameter: "))		# Population criteria

        if Symbol == "<":										# Less than
            qSeven = df_C[df_C["Population"] < nPop]			# Create new dataframe based on sign and population limit
        elif Symbol == "=":										# Equal to
            qSeven = df_C[df_C["Population"] == nPop]			# Create new dataframe based on sign and population limit
        elif Symbol == ">":										# Greater than
            qSeven = df_C[df_C["Population"] > nPop]			# Create new dataframe based on sign and population limit
        elif Symbol == "<=":									# Less than or equal to 
            qSeven = df_C[df_C["Population"] <= nPop]			# Create new dataframe based on sign and population limit
        elif Symbol == ">=":									# Greater than or equal to
            qSeven = df_C[df_C["Population"] >= nPop]			# Create new dataframe based on sign and population limit
        else:													# Incorrect sign used. Start process again
            print("Sorry, you entered an incorrect sign. Please start again")

        print()
        print(qSeven)											# Print out new dataframe
        print()
        print("Your search returned", len(qSeven), "rows of data")		# Specify the number of rows returned

        Sleep_Timer()											# 2 second pause before going back to top of function

################################

def main_menu():												# Function for main menu

   print()														# Print out menu options
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

def Sleep_Timer():												# Program pause function
    time.sleep(2)												# Pause program for 2 seconds
    print()
    print("============================================================================================")
    print()
    assignment()												# Call assignment function

################################


assignment()