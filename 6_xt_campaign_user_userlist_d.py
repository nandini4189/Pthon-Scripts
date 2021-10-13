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
cur_target.execute("SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'xamplify_test_load' AND tablename = 'xt_campaign_user_userlist_d');")
tableExists = cur_target.fetchone()[0]
if tableExists==True:
    cur_target.execute("""truncate table xamplify_test_load.xt_campaign_user_userlist_d cascade""")
    print("table is truncated")

else:
    cur_target.execute("""
    CREATE TABLE xamplify_test_load.xt_campaign_user_userlist_d
(
    id integer NOT NULL,
    campaign_id integer NOT NULL,
    user_list_id integer NOT NULL,
    user_id integer NOT NULL,
    email_id character varying(255) COLLATE pg_catalog."default",
    partner_company_name character varying(255) COLLATE pg_catalog."default",
    partner_first_name character varying(255) COLLATE pg_catalog."default",
    partner_last_name character varying(255) COLLATE pg_catalog."default",
    contact_user_id integer NOT NULL,
    contact_first_name character varying(255),
    contact_last_name character varying(255),
    contact_email_name character varying(255),
    contact_job_title character varying(255),
    contact_address character varying(255),
    contact_city character varying(255),
    contact_state character varying(255),
    contact_zip character varying(255),
    contact_country character varying(255),
    contact_mobile_number character varying(255),
    CONSTRAINT xa_campaign_user_userlist_pkey PRIMARY KEY (id)
)
    ;
    """)
    print("created table  in xamplify_test_load schema successfully")
cur_source.execute("""SELECT DISTINCT cuul.id,cuul.campaign_id,
cuul.user_list_id,
cuul.user_id,
up.email_id,
cp.company_name as partner_company_name,
up.firstname as partner_first_name,
up.lastname partner_last_name,
uul.user_id as contact_user_id,
uul.firstname as contact_first_name,
uul.lastname as contact_last_name,
uul.contact_company as contact_email_name,
uul.job_title contact_job_title,
uul.address as contact_address,
uul.city as contact_city,
uul.state as contact_state,
uul.zip as contact_zip,
uul.country as contact_country,
uul.mobile_number as contact_mobile_number

FROM xt_campaign c
JOIN xt_campaign_user_userlist cuul ON cuul.campaign_id = c.campaign_id
JOIN xt_user_list ul ON cuul.user_list_id = ul.user_list_id
join xt_user_userlist uul on uul.user_list_id = cuul.user_list_id
and uul.user_id = cuul.user_id
JOIN xt_user_profile up ON up.user_id = cuul.user_id
left JOIN xt_company_profile cp ON up.company_id = cp.company_id
ORDER BY cuul.campaign_id
""")
rows=cur_source.fetchall()
for row in rows:
    cur_target.execute("""insert into xamplify_test_load.xt_campaign_user_userlist_d(id,
    campaign_id,
    user_list_id,
    user_id,
    email_id,
    partner_company_name,
    partner_first_name,
    partner_last_name,
    contact_user_id,
    contact_first_name,
    contact_last_name,
    contact_email_name,
    contact_job_title,
    contact_address,
    contact_city,
    contact_state,
    contact_zip,
    contact_country,
    contact_mobile_number)
                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",row)
cur_source.execute("""select count(*) from (SELECT DISTINCT cuul.id,cuul.campaign_id,
cuul.user_list_id,
cuul.user_id,
up.email_id,
cp.company_name as partner_company_name,
up.firstname as partner_first_name,
up.lastname partner_last_name,
uul.user_id as contact_user_id,
uul.firstname as contact_first_name,
uul.lastname as contact_last_name,
uul.contact_company as contact_email_name,
uul.job_title contact_job_title,
uul.address as contact_address,
uul.city as contact_city,
uul.state as contact_state,
uul.zip as contact_zip,
uul.country as contact_country,
uul.mobile_number as contact_mobile_number

FROM xt_campaign c
JOIN xt_campaign_user_userlist cuul ON cuul.campaign_id = c.campaign_id
JOIN xt_user_list ul ON cuul.user_list_id = ul.user_list_id
join xt_user_userlist uul on uul.user_list_id = cuul.user_list_id
and uul.user_id = cuul.user_id
JOIN xt_user_profile up ON up.user_id = cuul.user_id
left JOIN xt_company_profile cp ON up.company_id = cp.company_id
ORDER BY cuul.campaign_id)e""")
result1 = cur_source.fetchone()
print("The no of source table rows are",result1[0])
conn_source.commit()
conn_source.close()
cur_target.execute("select count(*) from xamplify_test_load.xt_campaign_user_userlist_d")
result = cur_target.fetchone()
print("The no of target table rows are",result[0])
conn_target.commit()
conn_target.close()
