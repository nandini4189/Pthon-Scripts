import psycopg2
import sys
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

if len(sys.argv) < 1:
    print("Please provide database name and schema name")
    sys.exit(1)

print(sys.argv)

database = sys.argv[1]
host = sys.argv[2]
user = sys.argv[3]
password = sys.argv[4]

try:
    conn_source = psycopg2.connect(dbname="xamplify_stage", host="107.170.192.65", user="postgres",
                                   password="B!U;X>z9@Dhq$dKT")
    conn_source.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    print('source connected successfully')
except Exception as e:
    print('cannot find the source')

cur_source = conn_source.cursor()
conn_source.commit()

try:
    conn_target = psycopg2.connect(dbname=database, host=host, user=user, password=password)
    conn_target.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    print('target connected successfully')
except Exception as e:
    print('cannot find the target')

cur_target = conn_target.cursor()
cur_target.execute("SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'xamplify_test_load' AND tablename = 'xt_videofiles_d');")
tableExists = cur_target.fetchone()[0]
if tableExists==True:
    cur_target.execute("""truncate table xamplify_test_load.xt_videofiles_d cascade""")
    print("table is truncated")

else:
    cur_target.execute("""CREATE TABLE xamplify_test_load.xt_videofiles_d
(
    id integer NOT NULL,
    customer_id integer,
    title character varying COLLATE pg_catalog."default",
    created_time timestamp without time zone,
    CONSTRAINT xt_videofiles_d_pkey PRIMARY KEY (id),
    CONSTRAINT video_file_fkey FOREIGN KEY (customer_id)
        REFERENCES xamplify_test_load.xt_user_d (user_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
)
    ;
    """)
    print("created table  in xamplify_test_load schema successfully")
cur_source.execute("""select  id ,
   customer_id ,
   title ,
   created_time  from public.xt_video_files 
where customer_id in (select distinct user_id from public.xt_user_profile)
""")
rows=cur_source.fetchall()
for row in rows:
    cur_target.execute("""insert into  xamplify_test_load.xt_videofiles_d(id,
    customer_id,
    title,
    created_time)
                values(%s,%s,%s,%s)""",row)
cur_source.execute("""select count(*) from (select  id ,
   customer_id ,
   title ,
   created_time  from public.xt_video_files 
where customer_id in (select distinct user_id from public.xt_user_profile))e""")
result1 = cur_source.fetchone()
print("Source Rows:",result1[0])
conn_source.commit()
conn_source.close()
cur_target.execute("select count(*) from xamplify_test_load.xt_videofiles_d")
result = cur_target.fetchone()
print("Target Rows:",result[0])
conn_target.commit()
conn_target.close()
