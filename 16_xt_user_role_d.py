import psycopg2
import sys
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

if len(sys.argv)<1:
    print("Please provide database name and schema name")
    sys.exit(1)

print(sys.argv)

database=sys.argv[1]
host=sys.argv[2]
user=sys.argv[3]
password=sys.argv[4]

try:
    conn_source = psycopg2.connect(dbname="xamplify_stage",host="107.170.192.65",user="postgres",password="B!U;X>z9@Dhq$dKT")
    conn_source.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    print('source connected successfully')
except Exception as e:
    print('cannot find the source')

cur_source=conn_source.cursor()
conn_source.commit()

try:
    conn_target=psycopg2.connect(dbname=database,host=host,user=user,password=password)
    conn_target.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    print('target connected successfully')
except Exception as e:
    print('cannot find the target')

cur_target=conn_target.cursor()
cur_target.execute("SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'xamplify_test_load' AND tablename = 'xt_user_role_d');")
tableExists = cur_target.fetchone()[0]
if tableExists==True:
    cur_target.execute("""truncate table xamplify_test_load.xt_user_role_d cascade""")
    print("table is truncated")

else:
    cur_target.execute("""CREATE TABLE xamplify_test_load.xt_user_role_d
(
    user_id integer NOT NULL,
    role_id integer,
    CONSTRAINT user_role_fkey FOREIGN KEY (user_id)
        REFERENCES xamplify_test_load.xt_user_d (user_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
)
    """)
    print("created table  in xamplify_test_load schema successfully")
cur_source.execute("""select * from public.xt_user_role ur where ur.user_id in(select user_id from public.xt_user_profile)
 and ur.user_id in(86657,86658,86659,86660);
""")
rows=cur_source.fetchall()
for row in rows:
    cur_target.execute("""insert into xamplify_test_load.xt_user_role_d(user_id,role_id)
values(%s,%s)""",row)

cur_source.execute("""select count(*) from(select * from public.xt_user_role ur where ur.user_id in
(select user_id from public.xt_user_profile)and ur.user_id in(86657,86658,86659,86660)) e
                   """)

result1 = cur_source.fetchone()
print("The no of source table rows are",result1[0])
conn_source.commit()
conn_source.close()

cur_target.execute("select count(*) from xamplify_test_load.xt_user_role_d")
result = cur_target.fetchone()
print("The no of target table rows are",result[0])
conn_target.commit()
conn_target.close()
