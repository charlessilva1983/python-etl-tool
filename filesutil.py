import glob
from datetime import date
from variables import default_directory
import os

# Define arguments and constants
today = date.today()
directory=default_directory
Y, M, D = today.year, today.month, today.day

# Format the directory name
directory = directory.format(Y,'{0:02d}'.format(M),'{0:02d}'.format(D))

def searchFile(pattern):
    # Define the function that apply recursive search into the directory to find files
    foundFiles = []
    for filename in glob.iglob(directory+'/'+pattern, recursive=True):
        foundFiles.append(filename)
    return foundFiles
# print(searchFile('*.csv'))

def markFileAsProcessed(filename):
    filename=filename.replace(directory,'')[1:]
    localdir = directory+"/processed/"
    try:
        print("Directory:",directory)
        print("Filename:",filename)
        print("Localdir:",localdir)
        os.makedirs(os.path.dirname(localdir), exist_ok=True)
        # Move the processed file to the processed files directory
        os.rename(directory+'/'+filename, localdir+filename)
    except Exception as error:
        print("Error while loading the file {}".format(filename))
        print('error message: {}'.format(error))
        #raise sys.exit():