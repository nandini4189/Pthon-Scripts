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
    conn_source = psycopg2.connect(dbname="postgres",host="localhost",user="postgres",password="janu@8878")
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
cur_target.execute("SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'xamplify_test_load' AND tablename = 'xt_user_profile_f');")
tableExists = cur_target.fetchone()[0]
if tableExists==True:
    cur_target.execute("""truncate table xamplify_test_load.xt_user_profile_f cascade""")
    print("table is truncated")

else:
    cur_target.execute("""CREATE TABLE xamplify_test_load.xt_user_profile_f
	(
    user_id integer,
    campaign_id integer,
    company_id integer,
    role_id integer,
    socialconn_id integer,
    date_key integer,
    CONSTRAINT f_campaign_fkey FOREIGN KEY (campaign_id)
        REFERENCES xamplify_test_load.xt_campaign_d (campaign_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT f_company_id_fkey FOREIGN KEY (company_id)
        REFERENCES xamplify_test_load.xt_company_d (company_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT f_role_id_fkey FOREIGN KEY (role_id)
        REFERENCES xamplify_test_load.xt_role_d (role_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT f_social_conn_fkey FOREIGN KEY (socialconn_id)
        REFERENCES xamplify_test_load.xt_socialconn_d (id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT f_user_id_fkey FOREIGN KEY (user_id)
        REFERENCES xamplify_test_load.xt_user_d (user_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
	)
    """)
    print("created table  in xamplify_test_load schema successfully")
cur_source.execute("""SELECT a.user_id,campaign_id,company_id,role_id,social_conn_id,date_key
FROM 
(
SELECT ud1.user_id,c.campaign_id,cd.company_id,sd.id AS social_conn_id,ud1.datereg_key AS date_key
FROM xamplify_test_load.xt_user_d ud1 FULL JOIN xamplify_test_load.xt_company_d cd ON (cd.company_id = ud1.company_id)
LEFT JOIN  xamplify_test_load.xt_campaign_d c ON (c.customer_id = ud1.user_id)
LEFT JOIN  xamplify_test_load.xt_socialconn_d sd ON (sd.user_id = ud1.user_id)
)a
LEFT JOIN (
SELECT ud.user_id,ur.role_id
FROM xamplify_test_load.xt_user_role_d ur
LEFT JOIN xamplify_test_load.xt_user_d ud ON (ur.user_id = ud.user_id)
JOIN xamplify_test_load.xt_role_d r ON (r.role_id = ur.role_id)
) b ON (a.user_id = b.user_id)""")
rows=cur_source.fetchall()
for row in rows:
    cur_target.execute("""insert into xamplify_test_load.xt_user_profile_f(user_id,
    campaign_id,
    company_id,
    role_id,
    socialconn_id ,
    date_key)
                values(%s,%s,%s,%s,%s,%s)""",row)

cur_source.execute("""select count(*) from
(SELECT a.user_id,campaign_id,company_id,role_id,social_conn_id,date_key
FROM 
(
SELECT ud1.user_id,c.campaign_id,cd.company_id,sd.id AS social_conn_id,ud1.datereg_key AS date_key
FROM xamplify_test_load.xt_user_d ud1 FULL JOIN xamplify_test_load.xt_company_d cd ON (cd.company_id = ud1.company_id)
LEFT JOIN  xamplify_test_load.xt_campaign_d c ON (c.customer_id = ud1.user_id)
LEFT JOIN  xamplify_test_load.xt_socialconn_d sd ON (sd.user_id = ud1.user_id)
)a
LEFT JOIN (
SELECT ud.user_id,ur.role_id
FROM xamplify_test_load.xt_user_role_d ur
LEFT JOIN xamplify_test_load.xt_user_d ud ON (ur.user_id = ud.user_id)
JOIN xamplify_test_load.xt_role_d r ON (r.role_id = ur.role_id)
) b ON (a.user_id = b.user_id)) e
                   """)

result1 = cur_source.fetchone()
print("The no of source table rows are",result1[0])
conn_source.commit()
conn_source.close()

cur_target.execute("select count(*) from xamplify_test_load.xt_user_profile_f")
result = cur_target.fetchone()
print("The no of target table rows are",result[0])
conn_target.commit()
conn_target.close()
