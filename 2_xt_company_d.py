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

cur_target.execute("SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'xamplify_test_load' AND tablename = 'xt_company_d');")
tableExists = cur_target.fetchone()[0]
if tableExists==True:
    cur_target.execute("""truncate table xamplify_test_load.xt_company_d cascade""")
    print("table truncated")
else:
    cur_target.execute("""
        CREATE TABLE xamplify_test_load.xt_company_d
    (
        company_id integer NOT NULL,
        company_name character varying(250) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
        email_id character varying(250) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
        phone character varying(50) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
        country character varying(250) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
        CONSTRAINT xa_company_d_pkey PRIMARY KEY (company_id)
    );
        """)
    print("created table  in xamplify_test_load schema successfully")
cur_source.execute("""select
     company_id,
     company_name,
     email_id,
     phone,
     country
    from
     public.xt_company_profile
    """)
rows = cur_source.fetchall()
for row in rows:
    cur_target.execute("""insert into  xamplify_test_load.xt_company_d(company_id,
        company_name,
        email_id,
        phone,
        country)
                    values(%s,%s,%s,%s,%s)""", row)
cur_target.execute("select count(*) from xamplify_test_load.xt_company_d")
result = cur_target.fetchone()
print("Source Rows:", result[0])
conn_target.commit()
conn_target.close()
cur_source.execute("select count(*) from public.xt_company_profile")
result1 = cur_source.fetchone()
print("Target Rows:", result1[0])
conn_source.commit()
conn_source.close()


