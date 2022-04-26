import csv
import logging
import os
import re
from _csv import writer
from os.path import exists, dirname, join

import sniffer as sniffer
from dotenv import load_dotenv

global input_file_path
global input_file_name
global output_file_path
global stage_file_path
sniffer = csv.Sniffer()


# Infer the dialect of the file
def get_dialect(file_name):
    with open(file_name, 'r', newline='') as fn:
        snip = fn.read(2048)
        fn.seek(0)
        return sniffer.sniff(snip)


# Write the final output file
def writer(header, data, filename, dialect, ctr):
    # Open the file in append mode
    with open(filename, "a+", newline="") as csvfile:
        # Write the dictionary values
        write = csv.DictWriter(csvfile, fieldnames=header, dialect=dialect)
        # Write the header only for the first file
        if ctr == 0:
            write.writeheader()
        # Write the data to the file
        write.writerows(data)


# Read multiple files, get last_id from the first file, add last_id to every row in consecutive file
def updater(filename, last_id, dialect, ctr, logger):
    logger.info('updater: ' + filename + ' Last Id ' + str(last_id))
    # Read staging File
    with open(filename, 'r', newline="") as file:
        # For each row in the file create a dictionary
        data = [row for row in csv.DictReader(file, dialect=dialect)]
        for row in data:
            # Add last_id with the Capacity
            value = last_id + float(row['Capacity/mA.h/g'])
            # Assign the value to the Capacity Dictionary
            row['Capacity/mA.h/g'] = value
            # Convert ewe to float
            ewe = float(row['Ewe/V'])
            # Assign the value back to the dictionary
            row['Ewe/V'] = ewe
    # Get the header from the dictionary
    header = data[0].keys()
    # Write the output file
    writer(header, data, output_file_path, dialect, ctr)
    # Return the last value in the file as last_id
    last_id = value
    return last_id


# Define logging method, logs are written to process.log
def log():
    log_format = "%(levelname)s %(asctime)s - %(message)s"
    logging.basicConfig(filename="process.log", level=logging.DEBUG, format=log_format, filemode='w')
    logger = logging.getLogger()
    return logger


# Set the global variables read from .env file
def set_vars(logger):
    logger.info(dirname(__file__))
    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)
    global input_file_path
    input_file_path = os.environ.get('input_file_path')
    global output_file_path
    output_file_path = os.environ.get('output_file_path')
    global input_file_name
    input_file_name = os.environ.get('input_file_name')
    global stage_file_path
    stage_file_path = os.environ.get('stage_file_path')


# Get the File Count in the staging directory
def get_file_count(file_path, logger):
    file_count = len([name for name in os.listdir(file_path)])
    logger.info('File Count: ' + str(file_count))
    return file_count


# Create staging files from RAW Input File
def file_writer(data, filename):
    with open(filename, "a+", newline="") as csvfile:
        csvfile.write(data)


# Cleanup Staging Directory
def stage_cleanup(logger):
    path = stage_file_path
    for f in os.listdir(path):
        logger.info("stage cleanup : " + f)
        os.remove(os.path.join(path, f))


# Find extraneous data from processing using regular expressions
# If extraneous data is found then return 1
def find_extraneous(line, logger):
    regex = r"\S.mpr"
    matches = re.finditer(regex, line, re.MULTILINE)
    for matchNum, match in enumerate(matches, start=1):
        if matchNum > 0:
            return 1


def file_split(logger):
    # Cleanup Staging folder
    stage_cleanup(logger)
    # Determine raw input file name using path and name combination
    filename = input_file_path + input_file_name
    # Set the file name counter to 0, the total file count depends on the number of headers
    # of the form Capacity/mA.h/g	Ewe/V
    counter = 0
    # Read the RAW input file
    with open(filename, "r") as input_file:
        # For each line in the file
        for line in input_file:
            # Find the header of the form .mpr
            # if found then skip the row from processing
            match = find_extraneous(line, logger)
            # If match is 1 then continue to the next row in the file
            if match == 1:
                continue
            # If the line starts with Capacity/mA.h/g	Ewe/V
            # then identify it as a new file
            elif line.startswith('Capacity/mA.h/g	Ewe/V'):
                # Generate a new file name using a running counter
                # starting from zero
                stage_file_name = stage_file_path + 'File' + str(counter) + '.csv'
                file_writer(line, stage_file_name)
                counter = counter + 1
            else:
                # If it's a blank line skip from processing
                if line != "\n":
                    # Write the record
                    file_writer(line, stage_file_name)


# Cleanup old file
# Gets the number of files
# For each of the files call the updater function requires as input <File Name>, <Last Id>, <Dialect>, <Logger>
# updater function returns last_id of each file which is passed to the next consecutive file
def process(logger):
    # If the output file from the previous run exists
    # Delete the file
    if exists(output_file_path):
        os.remove(output_file_path)
    # Get the # of staged files
    file_count = get_file_count(stage_file_path, logger)
    # For each file in the stage directory call the updater function returns last_id of each file
    # For the first file the last_id is passed as Zero, the last row of capacitance is returned as the last_id
    # The last_id from the first file is passed to the consecutive files
    for i in range(0, file_count):
        # Derive the File Name
        filename = stage_file_path + 'File' + str(i) + '.csv'
        # For the first file the last_id is zero
        if i == 0:
            dialect = get_dialect(filename)
            last_id = updater(filename, 0, dialect, i, logger)
        # For all the other files last_id is passed from the previous file
        else:
            last_id = updater(filename, last_id, dialect, i, logger)


# main method
def main():
    # Initialize logger function
    logger = log()
    # Load the variables from the .env file
    set_vars(logger)
    # Split into multiple files based on the header condition of the raw file
    file_split(logger)
    # Processing of Staged Files
    process(logger)


# Calling main method
if __name__ == "__main__":
    main()
