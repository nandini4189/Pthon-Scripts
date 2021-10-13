import psycopg2
import csv
conn=psycopg2.connect(database="postgres",user="postgres",
                      host="localhost",password="postgres",
                      port="5432")
cur=conn.cursor()
cur.execute("SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'xamplify_test' AND tablename = 'spam');")
tableExists = cur.fetchone()[0]
if tableExists==True:
    cur.execute("""truncate table xamplify_test.spam cascade""")
    print("table is truncated")

else:
    cur.execute("""create table xamplify_test.spam(
                                                email text,created character varying(255),ip text)""")
with open(r"C:\Users\tkalyan\Desktop\spam.csv",'r') as f:
    #with open("C:\\Users\\tkalyan\\Desktop\\csvfile2.csv",'r') as f:---> we can also this command for mentioning the path
    csv_data = csv.reader(f)
    next(csv_data)
    for line in csv_data:
        new_line = []
        for x in line:
            if x == '':
                new_line.append(None)
            else:
                new_line.append( x )
        cur.execute("""INSERT INTO xamplify_test.spam(email,created,ip)
        VALUES (%s,%s,%s)""",new_line)
conn.commit()
conn.close()
print("Table is loaded from csv file")
