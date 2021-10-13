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
cur_target.execute("SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'xamplify_test_load' AND tablename = 'xt_user_d');")
tableExists = cur_target.fetchone()[0]
if tableExists==True:
    cur_target.execute("""truncate table xamplify_test_load.xt_user_d cascade""")
    print("table truncated")
else:
    cur_target.execute("""CREATE TABLE xamplify_test_load.xt_user_d
        (
        user_id integer NOT NULL,
        email_id character varying(255) COLLATE pg_catalog."default",
        firstname character varying(255) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
        lastname character varying(255) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
        status character varying(255) COLLATE pg_catalog."default",
        country character varying(45) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
        city character varying(45) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
        zip character varying(45) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
        datereg timestamp without time zone,
        datelastlogin timestamp without time zone,
        created_time timestamp without time zone,
        updated_time timestamp without time zone,
        updated_by integer,
        mobile_number character varying(45) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
        company_id integer,
        state character varying COLLATE pg_catalog."default",
        datereg_key integer,
        CONSTRAINT xa_user_d_pkey PRIMARY KEY (user_id),
        CONSTRAINT xa_user_d_email_id_key UNIQUE (email_id)
    )
        ;""")

print("created table  in xamplify_test_load schema successfully")
cur_source.execute("""select
     user_id,
     email_id,
     firstname,
     lastname,
     status,
     country,
     city,
     zip,
     datereg,
     datelastlogin,
     created_time,
     updated_time,
     updated_by,
     mobile_number,
     company_id,
     state,
     (split_part(datereg::text , '-',1)||split_part(datereg::text , '-',2)||left(split_part(datereg::text , '-',3),2))::int datereg_key 
    from
     public.xt_user_profile

    """)
rows = cur_source.fetchall()
for row in rows:
    cur_target.execute("""insert into  xamplify_test_load.xt_user_d( user_id,
        email_id,
        firstname,
        lastname,
        status,
        country,
        city,
        zip,
        datereg,
        datelastlogin,
        created_time,
        updated_time,
        updated_by,
        mobile_number,
        company_id,
        state,
        datereg_key
    ) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", row)

cur_source.execute("select count(*) from public.xt_user_profile")
result1 = cur_source.fetchone()
print("Source Rows:", result1[0])
conn_source.commit()
conn_source.close()
cur_target.execute("select count(*) from xamplify_test_load.xt_user_d")
result = cur_target.fetchone()
print("Target Rows:", result[0])
conn_target.commit()
conn_target.close()





