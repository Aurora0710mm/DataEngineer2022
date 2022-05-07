# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import time
import psycopg2
import argparse
import re
import csv
import psycopg2.extras
import io
import pandas as pd

DBname = "postgres"
DBuser = "postgres"
DBpwd = "lxf142127"
TableName = 'CensusData'
Datafile = "filedoesnotexist"  # name of the data file to be loaded
CreateDB = False  # indicates whether the DB table should be (re)-created

def initialize():
  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
  parser.add_argument("-c", "--createtable", action="store_true")
  args = parser.parse_args()

  global Datafile
  Datafile = args.datafile
  global CreateDB
  CreateDB = args.createtable

def new_file():
    df = pd.read_csv(Datafile)
    df2 = df.drop(index=0)
    df2.to_csv('test.csv')

def main():
    initialize()
    new_file()


if __name__ == "__main__":
    main()



