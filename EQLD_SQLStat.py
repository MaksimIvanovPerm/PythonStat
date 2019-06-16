import pandas as pd
import numpy as np
import cx_Oracle as cxo
import os
import sys
import configparser
import ast

os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"
#####################################################################################################
def getparameter(p_config_section,p_paramname,p_paramtype="string"):
	ret_val={'exit_code':0}
	if p_config_section=="" or p_paramname== "":
		ret_val['exit_code']=1
		return ret_val
	try:
		if p_paramtype=="string":
			ret_val['p_value']=str( ast.literal_eval( config.get(p_config_section,p_paramname) ) )
		if p_paramtype=="int":
			ret_val['p_value']=ast.literal_eval( config.getint(p_config_section,p_paramname) )
		if p_paramtype=="float":
			ret_val['p_value']=ast.literal_eval( config.getfloat(p_config_section,p_paramname) )
		if p_paramtype=="boolean":
			ret_val['p_value']=ast.literal_eval( config.getboolean(p_config_section,p_paramname) )
	except Exception:
			ret_val['exit_code']=1
			return ret_val
	return ret_val

def euqlid_distance(p1,p2,p_normalize=0):
	if p_normalize>0:
		min=p1.min()
		max=p1.max()
		if min==max:
			p1=1
		else:
			p1=(p1-min)/(max-min)
		min=p2.min()
		max=p2.max()
		if min==max:
			p2=1
		else:
			p2=(p2-min)/(max-min)
	d=np.sqrt(np.sum((p1-p2)**2))
	return float(d)
#####################################################################################################
conf_file="dbaccess.conf"
config = configparser.ConfigParser()
try:
	config.read(conf_file)
except Exception:
	print("Unexpected error: %s" %(sys.exc_info()[0]))

v_rtcfg={}

x=getparameter('CONNECTION','TNS_ALIAS')
if x['exit_code']==0:
	v_rtcfg['wheretoconnect']=x['p_value']

if wheretoconnect=="":
	x=getparameter('CONNECTION','HOST')
	if x['exit_code']==0:
		v_rtcfg['dbhost']=x['p_value']
	x=getparameter('CONNECTION','PORT')
	if x['exit_code']==0:
		v_rtcfg['dbport']=x['p_value']
#
	if v_rtcfg['dbhost']=="" or v_rtcfg['dbport']=="":
		print("Well, you don't provide tnsalias either host/port where to connect to; Due to it any next activity is meaningless;")
		sys.exit(1)
#
	x=getparameter('CONNECTION','SID')
	if x['exit_code']==0:
		v_rtcfg['v_name']=x['p_value']
		v_rtcfg['wheretoconnect'] = cxo.makedsn(dbhost, dbport, v_name)
	if v_rtcfg['v_name']=="":
		x=getparameter('CONNECTION','SERVICE_NAME')
		if x['exit_code']==0:
			v_rtcfg['v_name']=x['p_value']
			v_rtcfg['wheretoconnect'] = cxo.makedsn(dbhost, dbport, service_name=v_name)
	if v_rtcfg['v_name']=="":
		print("You don't provide SID nor SERVICE_NAME of the database; Due to it any next activity is meaningless;")
		sys.exit(2)

# Well ok, try to get out logopass
x=getparameter('CONNECTION','USERNAME')
if x['exit_code']==0:
	v_rtcfg['login']=x['p_value']

x=getparameter('CONNECTION','PASSWORD')
if x['exit_code']==0:
	v_rtcfg['pwd']=x['p_value']

if v_rtcfg['login']=="" or v_rtcfg['pwd']=="":
	print("You don't provide logopass;")
	sys.exit(3)

x=getparameter('DB','DBID')
if x['exit_code']==0:
	v_rtcfg['v_dbid']=x['p_value']

x=getparameter('DB','BEGIN_SNAP')
if x['exit_code']==0:
	v_rtcfg['v_bsnap']=x['p_value']

x=getparameter('DB','END_SNAP')
if x['exit_code']==0:
	v_rtcfg['v_esnap']=x['p_value']

x=getparameter('DB','statname')
if x['exit_code']==0:
	v_rtcfg['statname']=x['p_value']

x=getparameter('CSV','csvdirectory')
if x['exit_code']==0:
	v_rtcfg['csvdirectory']=x['p_value']

x=getparameter('CSV','resultfilename')
if x['exit_code']==0:
	v_rtcfg['resultfilename']=x['p_value']

x=getparameter('CSV','sepchar')
if x['exit_code']==0:
	v_rtcfg['sepchar']=x['p_value']

x=getparameter('CSV','decimalsep')
if x['exit_code']==0:
	v_rtcfg['decimalsep']=x['p_value']

# Well, what do we have here
for i in sorted(v_rtcfg.keys()):
	if i!="pwd":
		print("%s\t%s" %(i, v_rtcfg[i]))
	else:
		print("%s\t%s" %(i, "..."))
#
# Connect as user "hr" with password "welcome" to the "oraclepdb" service running on this computer.
try:
	connection = cxo.connect(v_rtcfg['login']+"/"+v_rtcfg['pwd']+"@"+v_rtcfg['wheretoconnect'])
except Exception:
	print("some error: %s" %(sys.exc_info()[0]))
	sys.exit(4)

print("database version: %s" %(connection.version))

v_sql="""select T.SNAP_ID as snap_id, sum("""+v_rtcfg['statname']+""") as stat_value 
from SYS.WRH$_SQLSTAT t 
where T.dbid="""+v_rtcfg['v_dbid']+"""
  and T.INSTANCE_NUMBER=1 
  and T.SNAP_ID between """+v_rtcfg['v_bsnap']+""" and """+v_rtcfg['v_esnap']+"""
group by t.snap_id 
order by t.snap_id"""

cursor = connection.cursor()
# https://www.oracle.com/technetwork/prez-python-queries-101587.html
cursor.execute(v_sql)
v_rows=[]
v_clmns=[]
v_cdesc={}
for i in cursor.description:
	v_cdesc['name']=i[0]
	if i[1] is cxo.NUMBER:
		v_cdesc['type']="int"
	if i[1] is cxo.STRING or i[1] is cxo.FIXED_CHAR:
		v_cdesc['type']="str"
	if i[1] is cxo.DATETIME or i[1] is cxo.TIMESTAMP:
		v_cdesc['type']="datetime.datetime"
	if i[1] is cxo.CLOB or i[1] is cxo.BLOB:
		v_cdesc['type']="cx_Oracle.LOB"
	v_clmns.append(v_cdesc)
	v_cdesc={}

v_cdesc=[]
for i in v_clmns:
	v_cdesc.append((i['name'],i['type']))

for i in cursor:
	v_rows.append(i)

cursor.close()

#y=np.array(v_rows, dtype=v_cdesc)
dbstat=pd.DataFrame.from_records(v_rows, columns=[i[0] for i in v_cdesc])
#dbstat.head(5)
#dbstat.columns
#dbstat.shape

v_sql="""SELECT distinct t.sql_id as item_id 
FROM sys.wrh$_sqltext t
WHERE t.dbid="""+v_rtcfg['v_dbid']
cursor = connection.cursor()
cursor.execute(v_sql)
v_rows=[]
v_clmns=[]
v_cdesc={}
for i in cursor.description:
	v_cdesc['name']=i[0]
	if i[1] is cxo.NUMBER:
		v_cdesc['type']="int"
	if i[1] is cxo.STRING or i[1] is cxo.FIXED_CHAR:
		v_cdesc['type']="str"
	if i[1] is cxo.DATETIME or i[1] is cxo.TIMESTAMP:
		v_cdesc['type']="datetime.datetime"
	if i[1] is cxo.CLOB or i[1] is cxo.BLOB:
		v_cdesc['type']="cx_Oracle.LOB"
	v_clmns.append(v_cdesc)
	v_cdesc={}

v_cdesc=[]
for i in v_clmns:
	v_cdesc.append((i['name'],i['type']))

for i in cursor:
	v_rows.append(i)

cursor.close()
items=pd.DataFrame.from_records(v_rows, columns=[i[0] for i in v_cdesc])

result=[]
for j in items['ITEM_ID']:
	#j="038df7udb5bz7"
	#print("%s" %(j))
	v_sql="""select  T.SNAP_ID as snap_id, t."""+v_rtcfg['statname']+""" as item_stat
from SYS.WRH$_SQLSTAT t
where T.dbid="""+v_rtcfg['v_dbid']+"""
  and T.INSTANCE_NUMBER=1
  and T.SNAP_ID between """+v_rtcfg['v_bsnap']+""" and """+v_rtcfg['v_esnap']+"""
  and t.sql_id='"""+j+"""'"""
#	print("%s" %(v_sql))
	cursor = connection.cursor()
	cursor.execute(v_sql)
	v_rows=[]
	v_clmns=[]
	v_cdesc={}
	for i in cursor.description:
		v_cdesc['name']=i[0]
		if i[1] is cxo.NUMBER:
			v_cdesc['type']="int"
		if i[1] is cxo.STRING or i[1] is cxo.FIXED_CHAR:
			v_cdesc['type']="str"
		if i[1] is cxo.DATETIME or i[1] is cxo.TIMESTAMP:
			v_cdesc['type']="datetime.datetime"
		if i[1] is cxo.CLOB or i[1] is cxo.BLOB:
			v_cdesc['type']="cx_Oracle.LOB"
		v_clmns.append(v_cdesc)
		v_cdesc={}
	v_cdesc=[]
	for i in v_clmns:
		v_cdesc.append((i['name'],i['type']))
	for i in cursor:
		v_rows.append(i)
	cursor.close()
	item_stat=pd.DataFrame.from_records(v_rows, columns=[i[0] for i in v_cdesc])
	if item_stat['ITEM_STAT'].var() > 0:
		print("%s\t%d\tOK" %(j, item_stat.shape[0]))
		m=item_stat.merge(dbstat,how='right',on='SNAP_ID')
		m['ITEM_STAT']=m['ITEM_STAT'].fillna(value=0, method=None)
		x=euqlid_distance(m['STAT_VALUE'].values, m['ITEM_STAT'].values, 1)
		result.append((j,x))
	else:
		print("%s\t%d\tSKIP" %(j, item_stat.shape[0]))

item_stat=pd.DataFrame(result,columns=['item_id','metric'])
#item_stat.head(5)
item_stat.to_csv(os.path.join(v_rtcfg['csvdirectory'],v_rtcfg['resultfilename']), sep=v_rtcfg['sepchar'], header=True, index=False, decimal=v_rtcfg['decimalsep'])

connection.close()


