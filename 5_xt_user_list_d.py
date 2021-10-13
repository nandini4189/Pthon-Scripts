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

cur_target.execute("SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'xamplify_test_load' AND tablename = 'xt_user_list_d');")
tableExists = cur_target.fetchone()[0]
if tableExists==True:
    cur_target.execute("""truncate table xamplify_test_load.xt_user_list_d cascade""")
    print("table truncated")
else:
    cur_target.execute("""
        CREATE TABLE xamplify_test_load.xt_user_list_d
    (
        user_list_key serial,
        user_list_id integer NOT NULL,
        user_list_name character varying(256) COLLATE pg_catalog."default",
        customer_id integer,
        updated_by integer,
        created_time timestamp without time zone,
        is_partner_userlist boolean,
        listby_partner_id integer,
        first_name character varying(256) COLLATE pg_catalog."default",
        last_name character varying(256) COLLATE pg_catalog."default",
        contact_company character varying(256) COLLATE pg_catalog."default",
        job_titile character varying(256) COLLATE pg_catalog."default",
        email_id character varying(256) COLLATE pg_catalog."default",
        address character varying(256) COLLATE pg_catalog."default",
        city character varying(256) COLLATE pg_catalog."default",
        state character varying(256) COLLATE pg_catalog."default",
        zip_code character varying(256) COLLATE pg_catalog."default",
        country character varying(256) COLLATE pg_catalog."default",
        mobile_number character varying(256) COLLATE pg_catalog."default",
        company_name character varying(256) COLLATE pg_catalog."default",
        CONSTRAINT xa_user_list_key_pkey PRIMARY KEY (user_list_key)
    )
        ;
        """)
    print("created table  in xamplify_test_load schema successfully")
cur_source.execute("""SELECT
 uul.user_list_id,
 ul.user_list_name,
 up.user_id customer_id,
 ul.updated_by,
 ul.created_time,
 is_partner_userlist,
 uul.user_id AS listby_partner_id,
 uul.firstname,
 uul.lastname,
 uul.contact_company,
 uul.job_title,
 up.email_id,
 uul.address,
 uul.city,
 uul.state,
 uul.zip,
 uul.country,
 uul.mobile_number,
 cp.company_name    
 FROM
 public.xt_user_profile up
 JOIN public.xt_user_list ul ON (up.user_id = ul.customer_id)
 JOIN public.xt_user_userlist uul ON uul.user_list_id = ul.user_list_id
 join public.xt_user_profile up1 on up1.user_id = uul.user_id
 left join public.xt_company_profile cp on up1.company_id = cp.company_id
 ORDER BY user_list_id
 """)
rows = cur_source.fetchall()
for row in rows:
    cur_target.execute("""insert into xamplify_test_load.xt_user_list_d( user_list_id,
        user_list_name,
        customer_id,
        updated_by,
        created_time,
        is_partner_userlist,
        listby_partner_id,
        first_name,
        last_name,
        contact_company,
        job_titile,
        email_id,
        address,
        city,
        state,
        zip_code,
        country,
        mobile_number,
        company_name)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """, row)
cur_source.execute("""select count(*) from (SELECT
 uul.user_list_id,
 ul.user_list_name,
 up.user_id customer_id,
 ul.updated_by,
 ul.created_time,
 is_partner_userlist,
 uul.user_id AS listby_partner_id
FROM
 public.xt_user_profile up
JOIN public.xt_user_list ul ON (up.user_id = ul.customer_id)
JOIN public.xt_user_userlist uul ON uul.user_list_id = ul.user_list_id)e""")

result1 = cur_source.fetchone()
print("Source Rows:", result1[0])
conn_source.commit()
conn_source.close()
cur_target.execute("select count(*) from xamplify_test_load.xt_user_list_d")
result = cur_target.fetchone()
print("Target Rows:", result[0])
conn_target.commit()
conn_target.close()



