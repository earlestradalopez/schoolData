import configparser
import hashlib
import sys
import os
import glob
import csv
from xmlrpc.client import DateTime
import mysql.connector
from datetime import datetime
from time import strftime


def createTable(tableName, mydb, readFile, primaryKey):
    mycursor = mydb.cursor()
    mycursor.execute("SHOW TABLES LIKE '" + tableName + "'")
    result = mycursor.fetchone()
    columnString = []
    columnNames = []
    columnCount = []
    columnValue = []
    notInt = []
    hasValue = []
    with open(readFile, "r") as file1:
        reader = csv.reader(file1, delimiter=",")
        for i, line in enumerate(reader):
            if (i != 0):
                #print (columnCount)
                y = 0
                for temp in line:
                    possibleTime = convertDate(temp)
                    if possibleTime != None:
                        if possibleTime != temp:                        
                            if columnNames[y].find(' ') == -1:                                
                                columnNames[y] += ' datetime DEFAULT NULL,'                
                    if len(temp) > columnCount[y]:
                        columnCount[y] = len(temp)
                    if checkInt(temp) == False:
                        notInt[y] = True
                    else:
                        if temp != '':
                            columnValue[y] = int(temp)
                    if temp != '':
                        hasValue[y] = True
                    y += 1
            else:                
                for z in line:                    
                    columnNames.append(z.replace(' ','').replace('-','').replace('(','').replace(')','').replace('ï»¿','').replace('"','').replace('/','').replace(':','').replace('%','').replace('+',''))
                    columnCount.append(0)
                    columnValue.append(0)
                    notInt.append(False)
                    hasValue.append(False)
    catStrings = 'CREATE TABLE ' + tableName + ' ('
    x = 0 
    for temp in columnNames:
        if notInt[x] == False:            
            if temp.lower().find('date') != -1 and hasValue[x] == False:
                catStrings += columnNames[x] + ' datetime DEFAULT NULL,'
                columnString.append(True)
            elif columnValue[x] < 2147483647:                
                catStrings += columnNames[x] + ' int '
                if primaryKey:
                    if temp == 'IntrID' or temp == 'StudentID' or temp == 'LocalID':
                        catStrings += ' PRIMARY KEY'
                catStrings += ',' 
                columnString.append(False)               
            else:
                catStrings += columnNames[x] + ' double DEFAULT NULL,'
                columnString.append(False)
        else:            
            columnString.append(True)
            if temp.find('datetime') == -1:                
                stringCount = columnCount[x] + 45
                catStrings += columnNames[x] + ' VARCHAR(' + str(stringCount) + ') DEFAULT NULL,'                
            else:
                catStrings += columnNames[x]
                
        x += 1

    print (readFile)
    #print (catStrings[:-1] + ')')
    if result:
        print ('# there is a table named ' + tableName)
        return columnString
    mycursor.execute(catStrings[:-1] + ')')
    return columnString
def checkInt(possibleInt):
    try:
        if possibleInt == '':
            return True
        int(possibleInt)
        return True
    except:
        return False
def convertDate(possibleDate):    
    try:
        date_object = datetime.strptime(possibleDate, '%m/%d/%Y').date()
        return date_object
    except:
        if (possibleDate == ''):
            return None
        return possibleDate
def insertData(mydb, sql, val, x):
    mycursor = mydb.cursor()
    #mycursor.execute(sql, val)          
    try:
        mycursor.execute(sql, val)        
    except:
        print ('Error in line : ' + str(x))
    mydb.commit()
def dataAttendance(readFile, mydb):    
    mycursor = mydb.cursor()
    mycursor.execute("SHOW TABLES LIKE 'attendance'")
    result = mycursor.fetchone()
    if result:
        print ('# there is a table named attendance')
    else:
        mycursor.execute('CREATE TABLE attendance (schoodId int NOT NULL PRIMARY KEY, absentCount  int DEFAULT NULL, AttbyDateAbsenceDate varchar(1000) NOT NULL)')
    file1 = open(readFile)
    x = 1
    for line in file1:
        if (x != 1):
            sptLines = line[:-1].replace('"','').split(',')            
            mycursor.execute("SELECT schoodId, absentCount, AttbyDateAbsenceDate FROM attendance where schoodId = " + sptLines[0])
            myresult = mycursor.fetchall()
            for xresult in myresult:                
                if(str(xresult[2]).find(sptLines[1]) == -1):
                    count = int(xresult[1]) + 1
                    temp = sptLines[1]
                    if (count != 1):
                        temp = str(xresult[2]) + ';' + sptLines[1]
                    sql = "UPDATE attendance SET absentCount = " + str(count) + ", AttbyDateAbsenceDate = '" + temp + "' WHERE schoodId = " + sptLines[0]
                    mycursor.execute(sql)
                    mydb.commit()
                    
            if(len(myresult) == 0):
                sql = "INSERT INTO attendance (schoodId, absentCount, AttbyDateAbsenceDate) VALUES (%s, %s, %s)"""
                tempCount = 1
                if (sptLines[1]== ''):
                    tempCount = 0
                val = (sptLines[0], tempCount, sptLines[1])
                insertData(mydb, sql, val, x)
        x += 1
def defaultInsert(readFile, tablename, mydb, primaryKey, columnString):
    mycursor = mydb.cursor()
    myQuery = mydb.cursor()
    pKey = ''
    arrayNumber = 0    
    updateStatementColumn = []
    with open(readFile, "r") as file1:
        reader = csv.reader(file1, delimiter=",")
        for i, line in enumerate(reader):
            if (i != 0):                
                tuppleArray = []
                updateStatementValue = ' SET '
                cCount = 0
                for temp in line: 
                    dataObject = convertDate(temp)                   
                    tuppleArray.append(dataObject)
                    if dataObject != None and primaryKey:
                        updateStatementValue += updateStatementColumn[cCount] + ' = '
                        if columnString[cCount]:
                            updateStatementValue += '"' + str(dataObject)  + '",'
                        else:
                            updateStatementValue += str(dataObject)  + ','                        
                    cCount += 1
                if primaryKey:
                    sqlQuery = "SELECT * FROM " + tablename + " where " + pKey + " = " + str(tuppleArray[arrayNumber])
                    print (sqlQuery)
                    myQuery.execute(sqlQuery)
                    myresult = myQuery.fetchall()
                    if len(myresult) > 0:
                        for xresult in myresult:
                            sqlQuery = "UPDATE " + tablename + updateStatementValue[:-1] + " WHERE " + pKey + " = " + str(tuppleArray[arrayNumber])
                            print (sqlQuery)
                            myQuery.execute(sqlQuery)
                            mydb.commit()
                    else:
                        try:                            
                            mycursor.execute(sql, tuppleArray)                    
                            mydb.commit()
                        except Exception as e:
                            f = open("errorlog.txt", "a")
                            f.write(str(datetime.now()) + ":Error on File -> " + readFile + ' ->' + str(e) + '\n')
                            f.close()

                else:    
                    try:
                        mycursor.execute(sql, tuppleArray)                    
                        mydb.commit()
                    except Exception as e:
                        f = open("errorlog.txt", "a")
                        f.write(str(datetime.now()) + ":Error on File -> " + readFile + ' ->' + str(e) + '\n')
                        f.close()
               
            else:
                fields = ''
                percentS = ''
                arrayCount = 0
                for z in line:
                    tempField = z.replace(' ','').replace('-','').replace('(','').replace(')','').replace('ï»¿','').replace('"','').replace('/','').replace(':','').replace('%','').replace('+','') + ','    
                    if primaryKey:
                        if tempField == 'InstrID,' or tempField == 'StudentID,' or tempField == 'LocalID,' or tempField == 'UserId,':
                            pKey = tempField[:-1]
                            arrayNumber = arrayCount                        
                        updateStatementColumn.append(tempField[:-1])
                    percentS += '%s,'
                    fields += tempField
                    arrayCount += 1                
                sql = "INSERT INTO " + tablename.lower() + " (" + fields[:-1] + ") VALUES (" + percentS[:-1] + ")"""
    file1.close()
    

########## ----- MAIN PROGRAM --------- ############
config = configparser.ConfigParser()
config.read("config.ini")
host=config["MYSQLDB"]["host"]
user=config["MYSQLDB"]["username"]                                                                                                                                                                                                                          
password=config["MYSQLDB"]["password"]
database=config["MYSQLDB"]["database"]
mydb = mysql.connector.connect(
    host=host,
    user=user,                                                                                                                                                                                                                          
    password=password,
    database=database
    )
mycursor = mydb.cursor()
updateTable = str(config["DEFAULT"]["update_data"]).split(';')
currentDirectory = config["DEFAULT"]["directory"]
files = glob.glob(currentDirectory + '/**/*.csv', recursive=True)
for list in files:
    if(list.endswith('.csv')):
        if(list.find('\\Attendance\\') != -1):
            dataAttendance(list, mydb)
        #elif(list.find('\\Teacher_ID') != -1):
        elif(list.find('\\3-6-9') == -1):
            tablename = list.lower().replace(currentDirectory.lower() + "\\",'').replace('.csv', '')
            tablename = tablename[0:tablename.rindex('\\')]
            tablename = tablename.replace('\\','_').replace(' ','_').replace('-','_')
            try:
                tablename = str(int(tablename)) + "_info"
            except:
                print (tablename)            
            try:
                updateData = False
                for tempUpdate in updateTable:
                    if(list.find('\\' + tempUpdate) != -1):
                        updateData = True
                columnString = createTable(tablename, mydb, list, updateData)
                defaultInsert(list, tablename, mydb, updateData, columnString)
            except Exception as e:
                f = open("errorlog.txt", "a")
                f.write(str(datetime.now()) + ":Error on File -> " + list + ' ->' + str(e) + '\n')
                f.close()
########## ----- MAIN PROGRAM --------- ############