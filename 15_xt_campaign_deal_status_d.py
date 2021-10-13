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

cur_target.execute("SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'xamplify_test_load' AND tablename = 'xt_campaign_deal_status_d');")
tableExists = cur_target.fetchone()[0]
if tableExists==True:
    cur_target.execute("""truncate table xamplify_test_load.xt_campaign_deal_status_d cascade""")
    print("table is truncated")

else:
    cur_target.execute("""CREATE TABLE xamplify_test_load.xt_campaign_deal_status_d
    (
    id integer,
    deal_id integer,
    deal_status character varying(100) COLLATE pg_catalog."default",
    created_time timestamp without time zone,
    updated_time timestamp without time zone,
    created_by integer,
    updated_by integer,
    CONSTRAINT deal_id_fkey FOREIGN KEY (deal_id)
        REFERENCES xamplify_test_load.xt_campaign_deal_registration_d (id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
    """)
    print("created table  in xamplify_test_load schema successfully")
cur_source.execute("""SELECT *
FROM public.xt_campaign_deal_status 
where deal_id in ((select distinct id from public.xt_campaign_deal_registration))
;
""")
rows=cur_source.fetchall()
for row in rows:
    cur_target.execute("""insert into xamplify_test_load.xt_campaign_deal_status_d(id,
    deal_id,
    deal_status,
    created_time,
    updated_time,
    created_by,
    updated_by)
                values(%s,%s,%s,%s,%s,%s,%s)""",row)

cur_source.execute("""select count(*) from(SELECT *
FROM public.xt_campaign_deal_status 
where deal_id in (select distinct id from public.xt_campaign_deal_registration)) e
                   """)

result1 = cur_source.fetchone()
print("Source Rows:",result1[0])
conn_source.commit()
conn_source.close()

cur_target.execute("select count(*) from xamplify_test_load.xt_campaign_deal_status_d")
result = cur_target.fetchone()
print("Target Rows:",result[0])
conn_target.commit()
conn_target.close()
