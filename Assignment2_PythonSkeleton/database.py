#!/usr/bin/env python3
import psycopg2
from datetime import datetime

#####################################################
##  Database Connection
#####################################################

'''
Connect to the database using the connection string
'''
def openConnection():
    # connection parameters - ENTER YOUR LOGIN AND PASSWORD HERE
    userid = "y23s2c9120_qhua0041"
    passwd = "K6HQub28"
    myHost = "soit-db-pro-2.ucc.usyd.edu.au"

    '''userid = "y23s2c9120_sdin0331"
    passwd = "z1133m"
    myHost = "soit-db-pro-2.ucc.usyd.edu.au"'''

    # Create a connection to the database
    conn = None
    try:
        # Parses the config file and connects using the connect string
        conn = psycopg2.connect(database=userid,
                                    user=userid,
                                    password=passwd,
                                    host=myHost)
    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)
    
    # return the connection to use
    return conn

'''
Validate employee based on username and password
'''
def checkEmployeeCredentials(userName, password):
    conn = openConnection()
    cur = conn.cursor()
    try:
        cur.callproc('CheckLoginCredentials', (userName, password))
        rows = cur.fetchone()
        return rows
    except Exception as e:
        print(f"An error occurred: {e}")
        return False
    finally:
        cur.close()
        conn.close()


'''
List all the associated cars in the database by employee
'''
def findCarsByEmployee(userName):
    conn = openConnection()
    cur = conn.cursor()
    '''sql = "select Car.carID, Car.make || ' ' || Car.model as carinfo, Status.statusName, COALESCE(CarType.carTypeName, '') || '   ' || COALESCE(CarWheel.carWheelName, '') as typewheel, Car.purchaseDate, Employee.firstName || ' ' || Employee.lastName as name, COALESCE(Car.description, '') as description from Car left join Employee on Car.managedBy = Employee.userName left join CarWheel on Car.carWheelID = CarWheel.carWheelID left join CarType on Car.carTypeID = CarType.carTypeID left join Status on Car.statusID = Status.statusID where userName = %s order by Car.purchaseDate asc, Status.statusName desc, Car.description asc"
    input = ([userName])
    cur.execute(sql, input)
    rows = cur.fetchall()
    keys = ['car_id', 'makemodel', 'status', 'typewheel', 'purchasedate', 'employee', 'description']
    dict_car = [dict(zip(keys, row)) for row in rows]
    for i in range(len(dict_car)):
        dict_car[i]['purchasedate'] = dict_car[i]['purchasedate'].strftime('%d/%m/%Y')
    print(dict_car)
    return dict_car'''
    try:
        cur.callproc('findCarsByEmployee', (userName,))
        rows = cur.fetchall()
        keys = ['car_id', 'makemodel', 'status', 'typewheel', 'purchasedate', 'employee', 'description']
        dict_car = [dict(zip(keys, row)) for row in rows]
        for i in range(len(dict_car)):
            dict_car[i]['purchasedate'] = dict_car[i]['purchasedate'].strftime('%d/%m/%Y')
        print(dict_car)
        return dict_car
    except Exception as e:
        print(f"An error occurred: {e}")
        return False
    finally:
        cur.close()
        conn.close()


'''
Find a list of cars based on the searchString provided as parameter
See assignment description for search specification
'''
def findCarsByCriteria(searchString):
    conn = openConnection()
    cur = conn.cursor()
    if len(searchString) > 0:
        print('>0')
        sql = "select Car.carID, Car.make || ' ' || Car.model as carinfo, Status.statusName, COALESCE(CarType.carTypeName, '') || '   ' || COALESCE(CarWheel.carWheelName, '') as typewheel, Car.purchaseDate, COALESCE(Employee.firstName || ' ' || Employee.lastName, '') as name, COALESCE(Car.description, '') from Car left join Employee on Car.managedBy = Employee.userName left join CarWheel on Car.carWheelID = CarWheel.carWheelID left join CarType on Car.carTypeID = CarType.carTypeID left join Status on Car.statusID = Status.statusID where (make ilike %s or  model ilike %s or statusName ilike %s or carTypeName ilike %s or carWheelName ilike %s or firstName ilike %s or lastName ilike %s or description ilike %s) and Car.purchaseDate > CURRENT_DATE - INTERVAL '15 years' order by case when Car.managedBy is NULL then 1 else 2 end, Car.purchaseDate asc, Status.statusName desc"
        cur.execute(sql, ('%' + searchString + '%',) * 8)
    else:
        print('Null')
        sql = "select Car.carID, Car.make || ' ' || Car.model as carinfo, Status.statusName, COALESCE(CarType.carTypeName, '') || '   ' || COALESCE(CarWheel.carWheelName, '') as typewheel, Car.purchaseDate, COALESCE(Employee.firstName || ' ' || Employee.lastName, '') as name, COALESCE(Car.description, '') from Car left join Employee on Car.managedBy = Employee.userName left join CarWheel on Car.carWheelID = CarWheel.carWheelID left join CarType on Car.carTypeID = CarType.carTypeID left join Status on Car.statusID = Status.statusID where Car.purchaseDate > CURRENT_DATE - INTERVAL '15 years' order by case when Car.managedBy is NULL then 1 else 2 end, Car.purchaseDate asc, Status.statusName desc"
        cur.execute(sql)
    rows = cur.fetchall()
    keys = ['car_id', 'makemodel', 'status', 'typewheel', 'purchasedate', 'employee', 'description']
    dict_car = [dict(zip(keys, row)) for row in rows]
    for i in range(len(dict_car)):
        dict_car[i]['purchasedate'] = dict_car[i]['purchasedate'].strftime('%d/%m/%Y')
    print(dict_car)
    return dict_car



'''
Add a new car
'''
def addCar(make, model, type, wheel, purchasedate, description):
    if type == 'Sedan':
        typeid = 1
    elif type == 'sedan':
        typeid = 1
    elif type == 'SUV':
        typeid = 2
    elif type == 'suv':
        typeid = 2
    elif type == 'MPV':
        typeid = 3
    elif type == 'mpv':
        typeid = 3
    else:
        return False
    if wheel == '2WD':
        wheelid = 1
    elif wheel == '2wd':
        wheelid = 1
    elif wheel == '4WD':
        wheelid = 2
    elif wheel == '4wd':
        wheelid = 2
    elif wheel == 'AWD':
        wheelid = 3
    elif wheel == 'awd':
        wheelid = 3
    else:
        wheelid = None
    conn = openConnection()
    cur = conn.cursor()
    sql = "insert into Car (make, model, cartypeid, statusid, carwheelid, purchasedate, description) values (%s, %s, %s, 1, %s, %s, %s)"
    input = ([make, model, typeid, wheelid, purchasedate, description])
    cur.execute(sql,input)
    conn.commit()
    return True


'''
Update an existing car
'''
def updateCar(carid, make, model, status, type, wheel, purchasedate, employee, description):
    if type == 'Sedan':
        typeid = 1
    elif type == 'sedan':
        typeid = 1
    elif type == 'SUV':
        typeid = 2
    elif type == 'suv':
        typeid = 2
    elif type == 'MPV':
        typeid = 3
    elif type == 'mpv':
        typeid = 3
    else:
        return False
    if wheel == '2WD':
        wheelid = 1
    elif wheel == '2wd':
        wheelid = 1
    elif wheel == '4WD':
        wheelid = 2
    elif wheel == '4wd':
        wheelid = 2
    elif wheel == 'AWD':
        wheelid = 3
    elif wheel == 'awd':
        wheelid = 3
    else:
        wheelid = None
    if status == 'New Stock':
        statusid = 1
    elif status == 'Hire Ready':
        statusid = 2
    elif status == 'Hired':
        statusid = 3
    elif status == 'Repair':
        statusid = 4
    elif status == 'Unavailable':
        statusid = 5
    elif status == 'Write Off':
        statusid = 6
    else:
        return False
    print(employee)
    conn = openConnection()
    cur = conn.cursor()
    employee = employee.lower()
    if employee not in ['jswift', 'mchan', 'opalster', 'jkeller', 'ktaylor', 'mmiller', 'jdavis', 'njohnson', 'glenna', 'cbowtel']:
        return False
        '''sql = "update Car set make = %s, model = %s, statusid = %s, cartypeid = %s, carwheelid = %s, purchasedate = %s, description = %s where carid = %s"
        input = ([make, model, statusid, typeid, wheelid, purchasedate, description, carid])
        cur.execute(sql,input)'''
    else:
        sql = "update Car set make = %s, model = %s, statusid = %s, cartypeid = %s, carwheelid = %s, purchasedate = %s, managedby = %s, description = %s where carid = %s"
        input = ([make, model, statusid, typeid, wheelid, purchasedate, employee, description, carid])
        cur.execute(sql,input)
    conn.commit()
    return True
