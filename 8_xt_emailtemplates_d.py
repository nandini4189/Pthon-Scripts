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

cur_target.execute("SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'xamplify_test_load' AND tablename = 'xt_emailtemplates_d');")
tableExists = cur_target.fetchone()[0]
if tableExists==True:
    cur_target.execute("""truncate table xamplify_test_load.xt_emailtemplates_d cascade""")
    print("table truncated")

else:
    cur_target.execute("""CREATE TABLE xamplify_test_load.xt_emailtemplates_d
(
    id integer NOT NULL,
    user_id integer,
    name character varying COLLATE pg_catalog."default",
    created_time timestamp without time zone,
    type character varying COLLATE pg_catalog."default",
    CONSTRAINT xt_emailtemplates_d_pkey PRIMARY KEY (id),
    CONSTRAINT email_template_fkey FOREIGN KEY (user_id)
        REFERENCES xamplify_test_load.xt_user_d (user_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
)
    ;
    """)
    print("created table  in xamplify_test_load schema successfully")
cur_source.execute("""select
 et.id ,
 et.user_id ,
 et.name ,
 et.created_time ,
 et.type
from
 public.xt_email_templates et,public.xt_user_profile up where et.user_id=up.user_id""")
rows=cur_source.fetchall()
for row in rows:
    cur_target.execute("""insert into  xamplify_test_load.xt_emailtemplates_d( id,
    user_id,
    name,
    created_time,
    type)
                values(%s,%s,%s,%s,%s)""",row)
cur_source.execute("""select count(*) from (select
 et.id ,
 et.user_id ,
 et.name ,
 et.created_time ,
 et.type
from
 public.xt_email_templates et,public.xt_user_profile up where et.user_id=up.user_id)e""")
result1 = cur_source.fetchone()
print("The no of source table rows are",result1[0])
conn_source.commit()
conn_source.close()
cur_target.execute("select count(*) from xamplify_test_load.xt_emailtemplates_d")
result = cur_target.fetchone()
print("The no of target table rows are",result[0])
conn_target.commit()
conn_target.close()
