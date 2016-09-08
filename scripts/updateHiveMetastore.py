import os,utils, logging, MySQLdb, sys, ctologger

## loggings ##
logger = ctologger.getCdhLogger()

def postgress_hive_metastore_creation():
	try:
		logger.info('Inside Postgres Hive Metastore Creation.')
		# reading HIVE Metastore db properties
		HIVE_DB_HOST = utils.getXmlValue("hive","hive-service-config","hive_metastore_database_host")
		HIVE_DB_NAME = utils.getXmlValue("hive","hive-service-config","hive_metastore_database_name")
		HIVE_DB_PWD = utils.getXmlValue("hive","hive-service-config","hive_metastore_database_password")
		HIVE_DB_PORT = utils.getXmlValue("hive","hive-service-config","hive_metastore_database_port")

		logger.info("Hive Metastore Host "+ str(HIVE_DB_HOST))
		logger.info("Hive Metastore Name  "+ str(HIVE_DB_NAME))
		#logger.info("Hive Metastore Password "+ str(HIVE_DB_PWD))
		logger.info("Hive Metastore Port "+ str(HIVE_DB_PORT))
		
		#install postgres client
		utils.run_command("yum -y install postgresql91.x86_64")
		# create postgres user to login
		os.system("adduser postgres -p postgres")
		command="sudo -u postgres psql -h "+HIVE_DB_HOST+" -p "+HIVE_DB_PORT+" --command=\"CREATE DATABASE "+HIVE_DB_NAME+";\""
		logger.debug("Database create cmd:" + command)
		utils.run_command(command)
		command=" cd /opt/cloudera/parcels/CDH/lib/hive/scripts/metastore/upgrade/postgres/; "+"sudo -u postgres psql -h "+ HIVE_DB_HOST +" -p "+HIVE_DB_PORT+" -d "+HIVE_DB_NAME+" -f /opt/cloudera/parcels/CDH/lib/hive/scripts/metastore/upgrade/postgres/hive-schema-1.1.0.postgres.sql"
		logger.debug("Execute cmd:"+command)
		utils.run_command(command)
		command="sudo -u postgres psql -h "+HIVE_DB_HOST+" --command=\"CREATE USER hive WITH PASSWORD '"+HIVE_DB_PWD+"';\""
		logger.debug("Create Hive user cmd: CREATE USER hive")
		#logger.debug("Create Hive user cmd: CREATE USER hive WITH PASSWORD "+HIVE_DB_PWD.encode('base64','strict'))
		utils.run_command(command)
		command="sudo -u postgres psql -h "+ HIVE_DB_HOST +" -p "+HIVE_DB_PORT+" -d "+HIVE_DB_NAME+" --output=/tmp/grant-privs -t -c \"SELECT 'GRANT SELECT,INSERT,UPDATE,DELETE ON \\\"'  || schemaname || '\\\". \\\"' ||tablename ||'\\\" TO hive ;' FROM pg_tables WHERE tableowner = CURRENT_USER and schemaname = 'public';\""
		logger.debug("Create grant-privs script cmd:"+ command)
		utils.run_command(command)
		command="sudo -u postgres psql -h "+ HIVE_DB_HOST +" -p "+HIVE_DB_PORT+" -d "+HIVE_DB_NAME+" -f /tmp/grant-privs"
		logger.debug("Execute grant-privs cmd:"+ command)
		utils.run_command(command)
		logger.info('Postgres Hive Metastore created!!! PLEASE RESTART HIVE SERVICE from CM UI')
		return 0
	except Exception, e:
		logger.error("Error installing postgress Hive metastore " + str(e) )
		logger.exception(e)
		raise Exception(e)
		
		
  ## Create metastore database in mariadb/mysql required for Hive and its respective tables##
def mysql_hive_metastore_creation():
	try:
		logger.info('Deploy mysql hive metastore')
		# reading HIVE Metastore db properties
		HIVE_DB_HOST = utils.getXmlValue("hive","hive-service-config","hive_metastore_database_host")
		HIVE_DB_NAME = utils.getXmlValue("hive","hive-service-config","hive_metastore_database_name")
		HIVE_DB_PWD = utils.getXmlValue("hive","hive-service-config","hive_metastore_database_password")
		HIVE_DB_PORT = utils.getXmlValue("hive","hive-service-config","hive_metastore_database_port")
		cmdb_root_pwd=utils.getXmlValue("manager","cmdb_root_password")

		logger.info("Hive Metastore Host "+ str(HIVE_DB_HOST))
		logger.info("Hive Metastore Name  "+ str(HIVE_DB_NAME))
		#logger.info("Hive Metastore Password "+ str(HIVE_DB_PWD))
		logger.info("Hive Metastore Port "+ str(HIVE_DB_PORT))
		
		connection = MySQLdb.connect(host=HIVE_DB_HOST, port=int(HIVE_DB_PORT), user="root", passwd=cmdb_root_pwd)
		cur = connection.cursor()
		command="SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME='"+HIVE_DB_NAME+"'"
		cur.execute(command)
		data=cur.fetchall()
		if len(data)==0:
			command= "cd /opt/cloudera/parcels/CDH/lib/hive/scripts/metastore/upgrade/mysql;" + "mysql -uroot -proot123 -h "+HIVE_DB_HOST+" -P "+HIVE_DB_PORT+" --execute=\"CREATE DATABASE IF NOT EXISTS "+HIVE_DB_NAME+"; USE "+HIVE_DB_NAME+"; SOURCE  /opt/cloudera/parcels/CDH/lib/hive/scripts/metastore/upgrade/mysql/hive-schema-1.1.0.mysql.sql;\""
			logger.debug("Creating Database and metastore scripts ")
			utils.run_command(command)
			command = "mysql -uroot -p"+cmdb_root_pwd+" -h "+HIVE_DB_HOST+" -P "+HIVE_DB_PORT+" --execute=\"GRANT SELECT,INSERT,UPDATE,DELETE,LOCK TABLES,EXECUTE ON "+HIVE_DB_NAME+".* TO 'hive'@'"+ HIVE_DB_HOST +"' IDENTIFIED BY '" + HIVE_DB_PWD+ "';\""
			utils.run_command(command)
			# grant hive user to access database from other hosts
			command = "mysql -uroot -p"+cmdb_root_pwd+" -h "+HIVE_DB_HOST+" -P "+HIVE_DB_PORT+" --execute=\"GRANT SELECT,INSERT,UPDATE,DELETE,LOCK TABLES,EXECUTE ON "+HIVE_DB_NAME+".* TO 'hive'@'%' IDENTIFIED BY '" + HIVE_DB_PWD+ "';\""
			utils.run_command(command)
			command = "mysql -uroot -p"+cmdb_root_pwd+" -h "+HIVE_DB_HOST+" -P "+HIVE_DB_PORT+" --execute=\"GRANT SELECT,INSERT,UPDATE,DELETE,LOCK TABLES,EXECUTE ON "+HIVE_DB_NAME+".* TO 'hive'@'localhost' IDENTIFIED BY '" + HIVE_DB_PWD+ "';\""
			utils.run_command(command)
			
			command = "mysql -uroot -p"+cmdb_root_pwd+" -h "+HIVE_DB_HOST+" -P "+HIVE_DB_PORT+" --execute='FLUSH PRIVILEGES;'"
			utils.run_command(command)
			logger.info('Mysql Hive Metastore created!!! PLEASE RESTART HIVE SERVICE from CM UI')
		else:
			logger.info('Mysql Hive Metastore database already exists')
			
		connection.close()
		return 0		
	except Exception, e:
		logger.error("Error installing mysql Hive metastore " + str(e) )
		logger.exception(e)
		raise Exception(e)

def main():
	try:
		logger.info("Update Hive Metastore` Start !!!!!!")
		HIVE_DB_TYPE = utils.getXmlValue("hive","hive-service-config","hive_metastore_database_type")
		logger.info("Hive Metastore Type "+ str(HIVE_DB_TYPE))
		if HIVE_DB_TYPE == "postgresql":
			res = postgress_hive_metastore_creation()	
			return res
		elif HIVE_DB_TYPE == "mysql":
			res = mysql_hive_metastore_creation()
			return res
		else:
			logger.error("Invalid database type " + str(HIVE_DB_TYPE) + " Valid database types are postgresql and mysql")
			raise Exception() 
	except Exception, e:
		logger.error("Error in Updating Hive Metastore :"+ str(e))
		logger.exception(e)
		raise Exception(e)

if __name__ == "__main__":
   main() 	

		