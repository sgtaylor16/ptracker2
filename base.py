import pandas as pd
import sqlite3 as sql3

con = sql3.connect("example.db")
cur = con.cursor()

schema = dict()
schema['PARTS'] = 'exceldb.xlsx'
schema['VENDORS'] = 'Vendors.xlsx'
schema['QUOTES'] = 'Quotes.xlsx'
schema['POs'] = 'POs.xlsx'
schema['PartsList'] = 'PartsList.xlsx'

df_parts = pd.read_excel(schema['PARTS'])
df_vendors = pd.read_excel(schema['VENDORS'])
df_quotes = pd.read_excel(schema['QUOTES'])
df_POs = pd.read_excel(schema['POs'])

#region Create Tables

def createPartsTable():
    cur.execute("""CREATE TABLE PARTS(
        PN TEXT PRIMARY KEY NOT NULL,
        PARTNAME TEXT NOT NULL,
        QTY INT NOT NULL,
        MPREDICTED TEXT,
        MACTUAL TEXT,
        FPREDICTED TEXT,
        FACTUAL TEXT)
        """)
    con.commit()

def createVendorsTable():
    cur.execute("""CREATE TABLE VENDORS(
        ID INT PRIMARY KEY NOT NULL,
        VENDORNAME TEXT NOT NULL)"""
        )
    con.commit()

def createQuotesTable():
    cur.execute("""CREATE TABLE QUOTES(
        ID INT PRIMARY KEY NOT NULL,
        VENDORID INT NOT NULL,
        QUOTEDATE TEXT,
        PN TEXT NOT NULL,
        NRE REAL,
        VARIABLE REAL,
        LEADTIME_WKS INT,
        FOREIGN KEY(VENDORID) REFERENCES VENDORS(ID),
        FOREIGN KEY(PN) REFERENCES PARTS(PN))"""
    )
    con.commit()

def createPOTable():
    cur.execute("""CREATE TABLE PO(
        ID INT PRIMARY KEY NOT NULL,
        VENDORID INT NOT NULL,
        PN TEXT NOT NULL,
        QTY INT,
        NRE REAL,
        VARIABLE REAL,
        LEADTIME_WKS INT,
        DATEPLACED TEXT,
        DATERECEIVED TEXT,
        FOREIGN KEY(VENDORID) REFERENCES VENDORS(ID),
        FOREIGN KEY(PN) REFERENCES PARTS(PN))"""
    )
    con.commit()



def createAllTables():
    createPartsTable()
    createVendorsTable()
    createQuotesTable()
    createPOTable()

#endregion

def primaryKeyCheck(column):
    '''Checks to make sure there are no duplicated values in a primary key column'''
    dupcheck = column.duplicated(False)
    if dupcheck.value_counts()[False] == len(column):
        pass
    else:
        errormessage = column[dupcheck]
        raise Exception(print(errormessage))
    return None

def foreignKeyCheck(copycolumn,mastercolumn):
    checkvalues = mastercolumn.values
    nokeylist  = []
    for value in copycolumn:
        if value not in checkvalues:
            nokeylist.append(value)
    if len(nokeylist) == 0:
        pass
    else:
        raise Exception(print(nokeylist))

#region Check functions

def checkPartsExcel():
    #Check to make sure all primary keys are unique
    primaryKeyCheck(df_parts['PN'])
    return None
    
def checkVendorsExcel():

    #Check to make sure all primary keys are unique
    primaryKeyCheck(df_vendors['ID'])

    return None

def checkQuotesExcel():

    #Check to make sure all primary keys are unique
    primaryKeyCheck(df_quotes['PN'])

    #Check to make sure vendorID column is in the vendors table
    foreignKeyCheck(df_quotes['VENDORID'],df_vendors['ID'])

    #Check to make sure parts column is in the parts table
    foreignKeyCheck(df_quotes['PN'],df_parts['PN'])

    return None

def checkPOsExcel():

    #Check to make sure all primary keys are unique
    primaryKeyCheck(df_POs['ID'])

    #Check to make sure vendorID column is in the vendors table
    foreignKeyCheck(df_POs['ID'],df_vendors['ID'])

    #Check to make sure parts column is in the parts table
    foreignKeyCheck(df_POs['PN'],df_parts['PN'])

    return None

#endregion

#region Read tables

def readPartsExcel():
    '''Reads the Excel Parts table into the database'''

    checkPartsExcel()

    df = df_parts
    for index,row in df.iterrows():

        cur.execute("""INSERT INTO PARTS (PN,PARTNAME,QTY,MPREDICTED,MACTUAL,FPREDICTED,FACTUAL)
        VALUES(?,?,?,?,?,?,?)""", (row['PN'],row['PARTNAME'],row['QTY'],row['MPREDICTED'],row['MACTUAL'],
        row['FPREDICTED'],row['FACTUAL'])
        )
    con.commit()

def readVendorsExcel():

    checkVendorsExcel()

    '''Reads the Excel Vendors table into the database'''
    df = df_vendors
    for index, row in df.iterrows():
        cur.execute("""INSERT INTO VENDORS(ID,VENDORNAME)
        VALUES (?,?)""",(row['ID'],row['VENDORNAME'])   
        )
    con.commit()

def readQuotesExcel():

    checkQuotesExcel()

    '''Reads the Excel Quotes table into the database'''
    df = df_quotes
    df['QUOTEDATE'] = df['QUOTEDATE'].astype(str)
    for index, row in df.iterrows():
        cur.execute("""INSERT INTO QUOTES(ID,VENDORID,QUOTEDATE,PN,NRE,VARIABLE,LEADTIME_WKS)
        VALUES(?,?,?,?,?,?,?)""",(row['ID'],row['VENDORID'],row['QUOTEDATE'],row['PN'],row['NRE'],
        row['VARIABLE'],row['LEADTIME_WKS'])
        )
    con.commit()
    
def readPOsExcel():

    checkPOsExcel()

    df_POs['DATEPLACED'] = df_POs['DATEPLACED'].astype(str)
    df_POs['DATERECEIVED'] = df_POs['DATERECEIVED'].astype(str)
    for index, row in df_POs.iterrows():
        cur.execute("""INSERT INTO PO(ID,VENDORID,PN,QTY,NRE,VARIABLE,LEADTIME_WKS,DATEPLACED,DATERECEIVED)
        VALUES(?,?,?,?,?,?,?,?,?)""",(row["ID"],row["VENDORID"],row["PN"],row["QTY"],row["NRE"],row["VARIABLE"],
        row["LEADTIME_WKS"],row["DATEPLACED"],row["DATERECEIVED"])
        )
    con.commit()

#endregion


