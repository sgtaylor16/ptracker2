import pandas as pd
import sqlite3 as sql3
from dateutil.parser import parse
import datetime

#con = sql3.connect("example.db")
con = sql3.connect(":memory:")
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
df_PL = pd.read_excel(schema['PartsList'])

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

def createPartsList():
    cur.execute("""CREATE TABLE PL(
        FN INT PRIMARY KEY NOT NULL,
        PN TEXT NOT NULL,
        QTY INT NOT NULL,
        FOREIGN KEY(PN) REFERENCES PARTS(PN))"""
    )

def createAllTables():
    createPartsTable()
    createVendorsTable()
    createQuotesTable()
    createPOTable()
    createPartsList()

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

def checkParts():
    #Check to make sure all primary keys are unique
    primaryKeyCheck(df_parts['PN'])
    return None
    
def checkVendors():

    #Check to make sure all primary keys are unique
    primaryKeyCheck(df_vendors['ID'])

    return None

def checkQuotes():

    #Check to make sure all primary keys are unique
    primaryKeyCheck(df_quotes['PN'])

    #Check to make sure vendorID column is in the vendors table
    foreignKeyCheck(df_quotes['VENDORID'],df_vendors['ID'])

    #Check to make sure parts column is in the parts table
    foreignKeyCheck(df_quotes['PN'],df_parts['PN'])

    return None

def checkPOs():

    #Check to make sure all primary keys are unique
    primaryKeyCheck(df_POs['ID'])

    #Check to make sure vendorID column is in the vendors table
    foreignKeyCheck(df_POs['ID'],df_vendors['ID'])

    #Check to make sure parts column is in the parts table
    foreignKeyCheck(df_POs['PN'],df_parts['PN'])

    return None

def checkPartsList():

    #Check to make sure all primary keys are uniuqe
    primaryKeyCheck(df_PL['FN'])

    #Check to make sure pn column is in the parts table
    foreignKeyCheck(df_PL['PN'],df_parts['PN'])

    return None

#endregion

#region Read tables

def readPartsExcel():
    '''Reads the Excel Parts table into the database'''

    checkParts()

    df = df_parts
    for index,row in df.iterrows():

        cur.execute("""INSERT INTO PARTS (PN,PARTNAME,QTY,MPREDICTED,MACTUAL,FPREDICTED,FACTUAL)
        VALUES(?,?,?,?,?,?,?)""", (row['PN'],row['PARTNAME'],row['QTY'],row['MPREDICTED'],row['MACTUAL'],
        row['FPREDICTED'],row['FACTUAL'])
        )
    con.commit()

def readVendorsExcel():

    checkVendors()

    '''Reads the Excel Vendors table into the database'''
    df = df_vendors
    for index, row in df.iterrows():
        cur.execute("""INSERT INTO VENDORS(ID,VENDORNAME)
        VALUES (?,?)""",(row['ID'],row['VENDORNAME'])   
        )
    con.commit()

def readQuotesExcel():

    checkQuotes()

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

    checkPOs()

    df_POs['DATEPLACED'] = df_POs['DATEPLACED'].astype(str)
    df_POs['DATERECEIVED'] = df_POs['DATERECEIVED'].astype(str)
    for index, row in df_POs.iterrows():
        cur.execute("""INSERT INTO PO(ID,VENDORID,PN,QTY,NRE,VARIABLE,LEADTIME_WKS,DATEPLACED,DATERECEIVED)
        VALUES(?,?,?,?,?,?,?,?,?)""",(row["ID"],row["VENDORID"],row["PN"],row["QTY"],row["NRE"],row["VARIABLE"],
        row["LEADTIME_WKS"],row["DATEPLACED"],row["DATERECEIVED"])
        )
    con.commit()

def readPartsListExcel():

    checkPartsList()

    for index, row in df_PL.iterrows():
        cur.execute("""INSERT INTO PL(FN,PN,QTY) VALUES(?,?,?)""",(row['FN'],row['PN'],row['QTY']))
        con.commit()

#endregion

def ShortageList(filepath):

    cur.execute("""SELECT PARTS.PN,PARTS.QTY, totals.Total_Required, (totals.Total_Required - PARTS.QTY) Shortage
    FROM PARTS
    INNER JOIN
    (SELECT PN, (sum(QTY)) "Total_Required"
    FROM PL
    GROUP BY PN) totals
    ON PARTS.PN = totals.PN
    WHERE Shortage > 0""")

    tuplist = cur.fetchall()

    df = pd.DataFrame(columns = ['PN','Part_Name','Total_Required', 'On_Hand','Shortage'],
        data = tuplist)


    return df.to_csv(filepath,index=False)

def SummaryList(filepath):

    cur.execute("""SELECT PARTS.PN, PARTS.PARTNAME, PARTS.QTY, totals.Total_Required, (totals.Total_Required - PARTS.QTY) Shortage
    FROM PARTS
    INNER JOIN
    (SELECT PN, (sum(QTY)) "Total_Required"
    FROM PL
    GROUP BY PN) totals
    ON PARTS.PN = totals.PN
    """)

    tuplelist = cur.fetchall()

    df = pd.DataFrame(columns = ['PN','Part_Name','Total_Required', 'On_Hand','Shortage'],
        data = tuplelist)

    return df.to_csv(filepath,index=False)

def SummaryListDelivery():
   
    cur.execute("""SELECT pldata.PN, pldata.PARTNAME, pldata.QTY, pldata.Total_Required,pldata.SHORTAGE, PO.DATEPLACED,PO.LEADTIME_WKS
    FROM 
    (SELECT PARTS.PN, PARTS.PARTNAME, PARTS.QTY, totals.Total_Required, (totals.Total_Required - PARTS.QTY) Shortage
    FROM PARTS
    INNER JOIN
    (SELECT PN, (sum(QTY)) "Total_Required"
    FROM PL
    GROUP BY PN) totals
    ON PARTS.PN = totals.PN) pldata
    LEFT JOIN
    PO
    ON pldata.PN = PO.PN""")

    tuplist = cur.fetchall()

    df = pd.DataFrame(columns = ['PN','PartName','QtyOnHand','Total Required','Shortage','DatePlaced','LeadTime'],
    data = tuplist)

    def addwks(startdate,leadtime):
        try:
            newtd = datetime.timedelta(leadtime)
            return startdate + newtd
        except ValueError:
            return None

    def tryparse(x):
        try:
            return parse(x)
        except TypeError:
            return x


    df['DatePlaced'] = df['DatePlaced'].apply(tryparse)
    df['DateExpected'] = df.apply(lambda row: addwks(row['DatePlaced'],row['LeadTime']),axis =1)

    return df

