import psycopg2
import csv
conn=psycopg2.connect(database="postgres",user="postgres",
                      host="localhost",password="postgres",
                      port="5432")
cur=conn.cursor()
cur.execute("SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'xamplify_test' AND tablename = 'bounce');")
tableExists = cur.fetchone()[0]
if tableExists==True:
    cur.execute("""truncate table xamplify_test.bounce cascade""")
    print("table is truncated")

else:

    cur.execute("""create table xamplify_test.bounce(status character varying(255),
                                                reason text,email text,created character varying(255))""")
with open(r"C:\Users\tkalyan\Desktop\bounces.csv",'r') as f:
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
        cur.execute("""INSERT INTO xamplify_test.bounce(status,
                                                reason,email,created)
        VALUES (%s,%s,%s,%s)""",new_line)
conn.commit()
conn.close()
print("Table is loaded from csv file")
