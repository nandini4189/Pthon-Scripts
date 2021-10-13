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
cur_target.execute("SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'xamplify_test_load' AND tablename = 'xt_campaign_deal_registration_d');")
tableExists = cur_target.fetchone()[0]
if tableExists==True:
    cur_target.execute("""truncate table xamplify_test_load.xt_campaign_deal_registration_d cascade""")
    print("table is truncated")

else:
    cur_target.execute("""CREATE TABLE xamplify_test_load.xt_campaign_deal_registration_d
    (
    id integer NOT NULL,
    created_by integer,
    campaign_id integer NOT NULL,
    lead_id integer,
    email character varying(100) COLLATE pg_catalog."default",
    company character varying(255) COLLATE pg_catalog."default",
    first_name character varying(255) COLLATE pg_catalog."default",
    last_name character varying(255) COLLATE pg_catalog."default",
    lead_street character varying(255) COLLATE pg_catalog."default",
    lead_city character varying(255) COLLATE pg_catalog."default",
    lead_state character varying(255) COLLATE pg_catalog."default",
    postal_code character varying(255) COLLATE pg_catalog."default",
    lead_country character varying(255) COLLATE pg_catalog."default",
    title character varying(100) COLLATE pg_catalog."default",
    phone character varying(100) COLLATE pg_catalog."default",
    deal_type character varying(255) COLLATE pg_catalog."default",
    website character varying(255) COLLATE pg_catalog."default",
    created_time timestamp without time zone,
    updated_time timestamp without time zone,
    estimated_closed_date timestamp without time zone,
    opportunity_amount double precision,
    parent_campaign_id integer,
    partner_company_id integer,
    partner_company_name character varying(255) COLLATE pg_catalog."default",
    is_deal boolean,
    opportunity_role character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT deal_reg_pkey PRIMARY KEY (id)
);
    """)
    print("created table  in xamplify_test_load schema successfully")
cur_source.execute("""SELECT id,created_by,campaign_id,lead_id,email,company,first_name,last_name,
lead_street,lead_city,lead_state,postal_code,lead_country,
title,cd.phone,deal_type,cd.website,created_time,updated_time,estimated_closed_date,opportunity_amount,
parent_campaign_id,partner_company_id,cp.company_name AS partner_company_name,is_deal, opportunity_role
FROM public.xt_campaign_deal_registration cd
JOIN public.xt_company_profile cp ON (cd.partner_company_id = cp.company_id)
""")
rows=cur_source.fetchall()
for row in rows:
    cur_target.execute("""insert into xamplify_test_load.xt_campaign_deal_registration_d(id,
 created_by,
 campaign_id,
 lead_id,
 email,
 company,
 first_name,
 last_name,
 lead_street,
 lead_city,
 lead_state,
 postal_code,
 lead_country,
 title,
 phone,
 deal_type,
 website, 
 created_time,
 updated_time,
 estimated_closed_date,
 opportunity_amount,
 parent_campaign_id,
 partner_company_id,
 partner_company_name, 
 is_deal,
 opportunity_role)
                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",row)

cur_source.execute("""select count(*) from(SELECT id,created_by,campaign_id,lead_id,email,company,first_name,last_name,
lead_street,lead_city,lead_state,postal_code,lead_country,
title,cd.phone,deal_type,cd.website,created_time,updated_time,estimated_closed_date,opportunity_amount,
parent_campaign_id,partner_company_id,cp.company_name AS partner_company_name,is_deal, opportunity_role
FROM public.xt_campaign_deal_registration cd
JOIN public.xt_company_profile cp ON (cd.partner_company_id = cp.company_id)) e
                   """)

result1 = cur_source.fetchone()
print("Source Rows:",result1[0])
conn_source.commit()
conn_source.close()

cur_target.execute("select count(*) from xamplify_test_load.xt_campaign_deal_registration_d")
result = cur_target.fetchone()
print("Target Rows:",result[0])
conn_target.commit()
conn_target.close()
