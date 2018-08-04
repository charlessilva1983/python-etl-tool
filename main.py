import sys
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from db_credentials import db_credentials_mysql as mysql
from sql_queries import mysql_queries
from filesutil import searchFile, markFileAsProcessed

def performReadFile(argfilename):
    # Define the function that load files into a Pandas dataframe
    # read the file with Pandas
    csv_data = pd.read_csv(argfilename, sep=',')
    return csv_data

def runQuery(conn, query):
    # Define the function that perform simple queries
    # Create a cursor
    source_cursor = conn.cursor()
    # Call task the store procedure
    source_cursor.execute(query)
    # Store the returned data
    data = source_cursor.fetchall()
    # Close the cursor
    source_cursor.close()
    return data

def performTask(conn, task, valid):
    # Define the function that perform task and validation queries
    runQuery(conn, task)
    return runQuery(conn, valid)[0]

def getStringConnection(dbname):
    # Define the function that create and return the connection string
    strconn = ''
    # Customize the connection string based on the database
    if(dbname == 'mysql'):
        strconn = 'mysql+pymysql://'+mysql['user']+':'+mysql['password']+'@'+mysql['host']+':3306/challenge'
    return strconn

def main():
    # Connect all the defined funtions to perform the complete main business logic
    if(len(sys.argv) == 2):
        print("Starging job!\n")
        # Remove the scriptname in the args array
        del sys.argv[0]
        # Take the filename argument
        filepattern=sys.argv[0]
        # Search files that match the pattern
        filesToProcess = searchFile(filepattern)
        # Create a empty Pandas dataframe
        csv_data = pd.DataFrame()
        # Load all files into a Pandas dataframe
        for afile in filesToProcess:
            print("Reading file:", afile,"\n")
            # try to read the file
            try:
                data = performReadFile(afile)
                print('Lines in the file {}:'.format(afile),len(data))
                # append the data into the final dataframe
                csv_data = csv_data.append(data)
            except FileNotFoundError as error:
                print("Error while loading the file {}".format(filepattern))
                print('error message: {}'.format(error))
                raise sys.exit()
        
        lenOfFinalDataset = len(csv_data)
        print('Total of lines read from the files:',lenOfFinalDataset)

        if (lenOfFinalDataset > 0):
            # Connect to server on localhost
            print("Connecting to:", mysql['db'])
            strconn = getStringConnection('mysql')
            #print(strconn)
            engine = create_engine(strconn, echo=False)
            #print('Engine works!')

            # Insert the data into the database
            print("Loading the data to the database...\n")
            csv_data.to_sql(name='<table_x>', con=engine, if_exists = 'append', index=False)
            print("Data loaded to the database!\n")
            # Open the connection to perform queries
            conn=engine.raw_connection()

            try:
                # Perform the queryes stored into mysql_queries
                for idx, task in enumerate(mysql_queries):
                    if task != 'valid':
                        print("Performing Task{}...".format(idx))
                        if task == 't0':
                            print("Counting registers into the table {}.".format('<table_x>'))
                            data = runQuery(conn, mysql_queries[task])[0]
                        else:
                            data = performTask(conn, mysql_queries[task], mysql_queries['valid'])
                        print("Inserted rows:", data[0])
                        print("Task {} successfully completed!\n".format(idx))

            except Exception as error:
                print("Task for {} has error".format(mysql['db']))
                print('error message: {}'.format(error))

            print("Closing connection to", mysql['db'])
            # Close the connection
            conn.close()
        else:
            print("There are no registers to load")

        # Move files already loaded to processed directory
        for afile in filesToProcess:
            print("Moving file:", afile,"to processed directory")
            markFileAsProcessed(afile)

        print("End of the process!")
    else:
        print("Number of arguments incorrect!")
        print("Usage: python main.py <filename.csv>")
        raise sys.exit()

if __name__ == "__main__":
    main()