# Database Python assignment
# Student Name: Mark Biggar
# Student Number: G00376334

import pymysql as psq
import pymongo
import pandas as pd
import re
import time

from pymongo import MongoClient

# Note: the user name and password are assumed to be "root" for the MySQL database. If it is not, change the psq.connect("localhost",User name,Password,...) to the correct inputs
# There is a 2 second pause between the code printing an output, and returning the the main menu

df_C = None													                                # Set dataframe to None
dc_C = None													                                # Set dataframe to None

################################

def assignment():

    main_menu()												                                # Call the menu function
   
    while True:												                                # I/O handling

        Input_Value = input("Please input an 'x' or a number between 1 & 7: ")

        if Input_Value in ["8", "9", "0"]:					                                # If the number is not available in the menu
            Input_Value = input("Please input an 'x' or a number between 1 & 7: ")
            
        elif (Input_Value.isdigit()) == True:				                                # If the entry is a number
            Choice = int(Input_Value)						                                # Convert to number
            break											                                # Break out of while loop

        elif Input_Value == "x" or Input_Value == "X":		                                # Close out function if x is presses
            print()
            print("Thank you for taking part. Good bye")
            print()
            exit()											                                # Exit function
        
        else:
            Input_Value = input("Please input an 'x' or a number between 1 & 7: ")	        # Ask for a new number to be inputted

    global dc_C
    global df_C

    if Choice == 1:											                                # Option to print top 15 rows of the City table									

        if dc_C is None:										                            # If the dataframe is empty
            dc_C = SQL_City()									                            # Call the return from SQL_City function
                        
        print()
        print(dc_C.head(15))									                            # Print first 15 rows of dataframe

        Sleep_Timer()										                                # 2 second pause before going back to top of function

    elif Choice == 2:										                                # Option to select inputs by population

        if dc_C is None:									                                # If the dataframe is empty
	        dc_C = SQL_City()									                            # Call the return from SQL_City function
	
        print()
        
        sign_input = input("Please enter <, <=, =, >=, > for population check ")	        # Sign input for check
        Pop_Lim = int(input("Please input a number for population parameter "))		        # Population parameter check

        dc_C["Population"] = dc_C["Population"].apply(pd.to_numeric, errors="coerce")  

        if sign_input == "<":								                                # Less than
            qTwo = dc_C[dc_C["Population"] < Pop_Lim]		                                # Create new dataframe based on sign and population limit
        elif sign_input == "=":								                                # Equal to
            qTwo = dc_C[dc_C["Population"] == Pop_Lim]		                                # Create new dataframe based on sign and population limit
        elif sign_input== ">":							                                    # Greater than
            qTwo = dc_C[dc_C["Population"] > Pop_Lim]		                                # Create new dataframe based on sign and population limit
        elif sign_input == "<=":							                                # Less than or equal to
            qTwo = dc_C[dc_C["Population"] <= Pop_Lim]		                                # Create new dataframe based on sign and population limit
        elif sign_input == ">=":							                                # Greater than or equal to
            qTwo = dc_C[dc_C["Population"] >= Pop_Lim]		                                # Create new dataframe based on sign and population limit
        else:												                                # Incorrect sign used. Start process again
            print("Sorry, you entered an incorrect sign. Please try again")
            
        if len(qTwo) >= 30:								                                    # Check length of return
                
            print()
            print("This search will return ",len(qTwo), "rows of data.")	                # Print out warning
            print()
            print("How many rows of data do you want to display? (max ~200)")			    # Offer chance to reduce number of rows returned		
            print("For percentage of rows, enter a number less than 1. (e.g. 0.75 for 75%)")	# Instruction for percentage of rows
            print()
            print("For number of rows, enter a whole number.")				                # Instruction for number of rows
            
            Ret_Lim = (input("Please make a selection:	"))		                            # Offer chance to reduce number of rows returned
            print()

            try:                                                                            # Determine the Ret_Lim input
                if "." in Ret_Lim:                                                          # Check if the input contains a fullstop
                    Ret_Lim = float(Ret_Lim)                                                # Convert to float
            
                else:                                                                       # If no fullstop
                    Ret_Lim = int(Ret_Lim)                                                  # Convert to integer
            
            except ValueError:                                                              # Error handling
                print()
                print("Thank you for entering neither an integer nor a decimal.")           # Commentary on error
                print("This program will now close")                                     
                exit()                                                                      # Exit the program

            if Ret_Lim < 1:										                            # For % of rows
                print(qTwo.sample(frac = Ret_Lim))				                            # Print fraction of rows
                print()
                print(round(len(qTwo) * Ret_Lim,0),"rows were returned.") 	                # Specify how many rows printed out
            
            elif Ret_Lim >= 1:									                            # For number of rows
                print(qTwo.sample(n = Ret_Lim))
                print()
                print(Ret_Lim,"rows of data returned, as requested.")		                # Specify how many rows printed
            
        elif len(qTwo) == 0:							                                    # If length of return is 0
            print()
            print("This search did not return any results. Sorry.")    		                # Print no results found
        
        else:											                                    # All other cases
            print()
            print(qTwo)											                            # Print out the results
            print()
            print("Your input returned ", len(qTwo), "rows.")	                            # Specify how many rows printed
            print()

        Sleep_Timer()										                                # 2 second pause before going back to top of function
    
    elif Choice == 3:										                                # Adding data to DB in City table
    # Note that there is no check in place to determine if the new city already exists in the database
	    
        print()
        print("Enter new city details")                                                     # Title of operation
        print()

        nCity = input("New city name: ")						                            # New city name
        CC = input("3 letter country code:  ").upper()			                            # Country code for new city
        nDistrict = input("District where city is located: ")	                            # District where city is located
        nPop = input("Population of new city (no commas):  ")	                            # Population of new city

        print()
	
        conn = psq.connect("localhost","root","fi3Keepo","world",cursorclass=psq.cursors.DictCursor)	# Settings to connect to the DB

        query = "INSERT INTO city (Name, CountryCode, District, Population) VALUES ( %s, %s, %s, %s);"	# SQL insertion query

        with conn:
            try:												                            # See if this works
                cursor = conn.cursor()							                            # Create cursor in SQL
                cursor.execute(query, (nCity, CC, nDistrict, nPop))			                # Execute query entering data into SQL
                conn.commit()									                            # Commit the query
                print()
                print("You just added", nCity, "in", nDistrict, "from", CC, "with a population of", nPop, "to the database.")	# Confirmation of entry
                dc_C = None                                                                # Forces a repolling of the city table after an addition to the table

            except psq.err.IntegrityError:						                            # If there is an integrity error
                print()
                print("Country code does not exist")			                            # Print that the country code does not exist
            
            except psq.err.DataError:							                            # If there is a data error
                print()
                print("Either 1) Country code entered is more than 3 letters, or 2) comma in the population entered")	# Print that either of these solutions could be the problem

        Sleep_Timer()											                            # 2 second pause before going back to top of function

    elif Choice == 4:                                                                       # Search engine sizes

        client = MongoClient("Localhost",27017)				                                # Set Mongo Client to local hose

        db = client.Assignment								                                # Set db as the DB required
        collection = db.project								                                # Set the collection to the collection required

        eSize = float(input("Enter engine size:   "))                                       # Ask user for engine size they want

        query = {"car.engineSize": eSize}                                                   # Query for mongodb

        lCheck = collection.count_documents(query)                                          # Check number of documents returned

        if lCheck == 0:                                                                     # If the number of documents is 0
            print()
            print("Sorry, no results were found")                                           # Print out that there were no returns

        else:                                                                               # If there are to be documents returned
            rDocs = collection.find(query)                                                  # Get the document
            print()
            for doc in rDocs:                                                               # For each document returned
                print(doc["_id"], "¦", doc["car"]["reg"], "¦", doc["car"]["engineSize"], "¦",doc["addresses"])  # Print out the ID, car registration, engine size, and address

        Sleep_Timer()										                                # 2 second pause before going back to top of function

    elif Choice == 5:

        print()

        IDCheck = 3                                                                         # Variable to control for IDs
        while IDCheck != 0:                                                                 # Continue with process until IDCheck is equal to 0
            ID_in = input("Enter ID to be used. It can be a letter(s) or number:    ")      # Request user to input an ID for the new entry

            if "." in ID_in:                                                                # If the input has a full stop in it
                ID_in = float(ID_in)                                                        # Convert the number to a float
            elif ID_in.isdigit() == True:                                                   # If the input is a number
                ID_in = int(ID_in)                                                          # Convert the number to an integer
            else:                                                                           # Otherwise
                ID_in = ID_in.upper()                                                       # Convert the input to capitalised letters

            query = {"_id" : ID_in}                                                         # Create a mongodb query
            IDCheck = collection.count_documents(query)                                     # Use the query to count the number of documents with the specified ID

            if IDCheck != 0:                                                                # If the ID count is not 0
                print("Sorry that ID is in use. Please choose another ID")                  # Ask user to input a new ID

        print()
        RegCheck = 3                                                                        # Variable to control for registration
        while RegCheck != 0:                                                                # Continue with process until RegCheck is equal to 0
            Reg_in = input("Enter registration of vehicle. Please use format of YYY-CC-NNNNN:   ")  # Request user to input a registration for new entry      
            query = {"car.reg": Reg_in}                                                     # Create a mongo query
            RegCheck = collection.count_documents(query)                                    # Ise the query to count the number of documents with the specified registration number

            if RegCheck != 0:                                                               # If the registration count is not 0
                print("Sorry that registration is already in use. Please enter another registration")   # Ask user to input a new registration

        print()
        EngSize_in = float(input("Please enter an engine size for the vehicle:"))           # Request the user to input the engine size

        New_Entry ={"_id": ID_in, "car": [{"reg": Reg_in, "engineSize": EngSize_in}]}       # Create new file for insertion into database

        collection.insert_one(New_Entry).inserted_id                                        # Insert the new document into the database

        input_check = collection.count_documents({"_id" : ID_in})                           # Count the number of documents with the new ID from the insertion file

        if input_check == 1:                                                                # If the number of documents returned is 1
            print()
            print("New document with ID of", ID_in, "with a registration of", Reg_in, "and an engine size of", EngSize_in, "has been added to the database")    # Confirm the insertion was successful
            
        else:
            print()
            print("Something went wrong. Please try again.")                                # Something went wrong
    
        Sleep_Timer()                                                                       # 2 second pause before going back to top of function

    elif Choice == 6:											                            # See all countries that contain some characters
   
        if df_C is None:											                        # If dataframe is None
            SQL_Country()										                            # Run SQL_Country function
            df_C = SQL_Country()								                            # Make df_C equal to SQL_Country

        print()
        cCode = input("Enter 1 - 5 letters of a country you wish to see:    ")	            # Enter some characters in the country name
        
        qSix = df_C[df_C["Name"].str.contains(cCode, flags=re.IGNORECASE)]		            # Create a new dataframe with the country code, and ignoring case of the letters entered
        
        print()
        print(qSix)												                            # Print out new dataframe
        print()
        print("Your search returned", len(qSix), "rows of data.")		                    # Specify number of rows returned
        
        Sleep_Timer()											                            # 2 second pause before going back to top of function

    elif Choice == 7:											                            # See all countries by population

        if df_C is None:											                        # If dataframe is None
            SQL_Country()										                            # Run SQL_Country function
            df_C = SQL_Country()								                            # Make df_C equal to SQL_Country

        print()
        Symbol = input("Please enter <, <=, =, >=, > for population check:   ")		        # Sign to be used for determining population
        nPop = int(input("Enter population parameter: "))		                            # Population criteria

        if Symbol == "<":										                            # Less than
            qSeven = df_C[df_C["Population"] < nPop]			                            # Create new dataframe based on sign and population limit
        elif Symbol == "=":										                            # Equal to
            qSeven = df_C[df_C["Population"] == nPop]			                            # Create new dataframe based on sign and population limit
        elif Symbol == ">":										                            # Greater than
            qSeven = df_C[df_C["Population"] > nPop]			                            # Create new dataframe based on sign and population limit
        elif Symbol == "<=":									                            # Less than or equal to 
            qSeven = df_C[df_C["Population"] <= nPop]			                            # Create new dataframe based on sign and population limit
        elif Symbol == ">=":									                            # Greater than or equal to
            qSeven = df_C[df_C["Population"] >= nPop]			                            # Create new dataframe based on sign and population limit
        else:													                            # Incorrect sign used. Start process again
            print("Sorry, you entered an incorrect sign. Please start again")

        print()
        print(qSeven)											                            # Print out new dataframe
        print()
        print("Your search returned", len(qSeven), "rows of data")		                    # Specify the number of rows returned

        Sleep_Timer()											                            # 2 second pause before going back to top of function

################################

def main_menu():                                                                            # Function for main menu
# Print out menu options

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

def Sleep_Timer():												                            # Program pause function
    time.sleep(2)												                            # Pause program for 2 seconds
    print()
    print("============================================================================================")   # Insert a break line
    print()
    assignment()												                            # Call assignment function

################################

def SQL_Country():											                                # SQL functions for country
    if df_C is None:									
        conn = psq.connect("localhost","root","fi3Keepo","world",cursorclass=psq.cursors.DictCursor)	# Settings to connect to the DB
        query = "SELECT * FROM country"						                                # Query to select everything in Country table of db

        with conn:											                                # Using the connecction settings
            cursor = conn.cursor()							                                # Create a cursor
            cursor.execute(query)							                                # Write the query in SQL
            country = cursor.fetchall()						                                # Set 'Country" as everything in table

    df1 = pd.DataFrame(country, columns=["Code", "Name", "Continent", "Population", "HeadOfState"])	# Set query into a dataframe
    df1["Population"] = df1["Population"].apply(pd.to_numeric, errors="coerce")             # Convert the population column to a number

    return df1                                                                              # Return the dataframe
################################

def SQL_City():												                                # SQL functions for city
    if dc_C is None:
        conn = psq.connect("localhost","root","fi3Keepo","world",cursorclass=psq.cursors.DictCursor)	# Settings to connect to the DB
        query = "SELECT * FROM city"						                                # Query to select everything in City table of db

        with conn:											                                # Using the connecction settings
            cursor = conn.cursor()							                                # Create a cursor
            cursor.execute(query)							                                # Write the query in SQL
            city = cursor.fetchall()						                                # Set 'City" as everything in table

    df2 = pd.DataFrame(city, columns=["ID", "Name", "CountryCode", "District", "Population"])	    # Set query into a dataframe
    df2["Population"] = df2["Population"].apply(pd.to_numeric, errors="coerce")             # Convert the population column to a number

    return df2                                                                              # Return the dataframe
################################


assignment()