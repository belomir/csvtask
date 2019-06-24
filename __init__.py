"""csv employee mandays module"""


from os import listdir, path
import csv
import sqlite3


def exec(folder='../../data', database='data.db', projectField='Название проекта', managerField='Руководитель', dateField='Дата сдачи'):
    """
    main function for script

    :param folder: folder url, that contains csv files
    :type folder: str
    :param database: sqlite2 database name
    :type database: str
    :param projectField: name of project name fields in csv files
    :type projectField: str
    :param managerField: manager fields in csv files
    :type managerField: str
    :param dateField: project end's date fields in csv files
    :type dateField: str
    """
    
    fields = [projectField, managerField, dateField] #: csv fields list
    
    connection = sqlite3.connect(folder+'/'+database)
    cursor = connection.cursor()
    cursor.execute('create table if not exists employees (id text primary key)')
    connection.commit()
    cursor.execute('create table if not exists projects (id text primary key, manager text, date string)')
    connection.commit()
    cursor.execute('create table if not exists mandays (project text not null, employee text not null, mandays integer, primary key(project, employee))')
    connection.commit()

    dataFiles = [file for file in listdir(folder) if path.isfile(file)]

    for dataFile in dataFiles:
        with open('./{dir}/{file}'.format(dir=folder, file=dataFile), encoding='utf-8') as file:
            fileiscsv = True
            try:
                reader = csv.DictReader(file)
            except:
                fileiscsv = False
            if fileiscsv:
                employees = {employee for employee in reader.fieldnames if employee not in fields}
                projects = list()
                mandays = list()
                for row in reader:
                    project = row[projectField]
                    projects.append((project, row[managerField], '{2}-{1:0>2}-{0:0>2}'.format(*row[dateField].split('.'))))
                    projectMandays = [(project, employee, int(row[employee].strip())) for employee in employees if row[employee].strip() != '']
                    mandays.extend(projectMandays)
                cursor.executemany('insert or ignore into employees (id) values (?)', [(employee,) for employee in employees])
                connection.commit()
                cursor.executemany('insert or replace into projects (id, manager, date) values (?,?,?)', projects)
                connection.commit()
                cursor.executemany('insert or replace into mandays (project, employee, mandays) values (?,?,?)', mandays)
                connection.commit()

    cursor.execute('select id, manager, strftime("%d.%m.%Y", date) from projects order by date')
    with open('output.csv', 'w', encoding='utf-8') as file:
        writer = csv.writer(file, quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(fields)
        for row in cursor.fetchall():
            writer.writerow(row)

    connection.close()
