import utils, time, socket, sys, re, os, logging, ctologger
from subprocess import Popen, PIPE, STDOUT 
CLUSTER_NAME = utils.getXmlHostlist("distribution", "cluster_name")
cm_server_host=utils.getXmlValue("manager","host")
#Function for installing and starting MySQL DB

logger = ctologger.getCdhLogger()
def mysql_Install():
	try:	
		mysql_host = utils.getXmlHostlist("cm_mysqldb","hosts")
		logger.info("Installing Cloudera Manager Database!!! Processing.....!!")
		if len(mysql_host[0]) > 0:
			for host in mysql_host:
				logger.info("Installing Database on: "+host)
				command="ssh  " + host + " yum -y install MariaDB-Galera-server MariaDB-client galera"
				utils.run_command(command)
				command = "ssh  " + host + " service mysql start"
				utils.run_command(command)
				
	except Exception as e:
		logger.error("Error while installing mySQL Database on Nodes : "+ str(e))
		logger.exception(e)
		raise Exception(e)

#########################################################################################Config File Creation#########################################
def creating_Config_File(host):
	hostm = utils.getXmlValue("cm_mysqldb","hosts")
	try:
		command = "ssh "+host+ " \" echo transaction-isolation = READ-COMMITTED >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)	
		command = "ssh "+host+ " \" echo binlog_format=ROW >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		
		command = "ssh "+host+ " \" echo key_buffer = 16M >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command = "ssh "+host+ " \" echo key_buffer_size = 32M >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command = "ssh "+host+ " \" echo max_allowed_packet = 32M >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command = "ssh "+host+ " \" echo thread_stack = 256K >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command = "ssh "+host+ " \" echo thread_cache_size = 64 >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command = "ssh "+host+ " \" echo query_cache_limit = 8M >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command = "ssh "+host+ " \" echo max_connections = 550 >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command = "ssh "+host+ " \" echo read_buffer_size = 2M >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command = "ssh "+host+ " \" echo read_rnd_buffer_size = 16M >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command = "ssh "+host+ " \" echo sort_buffer_size = 8M >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command = "ssh "+host+ " \" echo join_buffer_size = 8M >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		
		command = "ssh "+host+ " \" echo default-storage-engine=innodb >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command = "ssh "+host+ " \" echo innodb_autoinc_lock_mode=2 >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command = "ssh "+host+ " \" echo innodb_locks_unsafe_for_binlog=1 >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command = "ssh "+host+ " \" echo query_cache_size=64M >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command ="ssh "+host+ " \" echo query_cache_type=1 >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command ="ssh "+host+ " \" echo bind-address=0.0.0.0 >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command ="ssh "+host+ " \" echo datadir=/var/lib/mysql >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command ="ssh "+host+ " \" echo innodb_log_file_size=100M >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command ="ssh "+host+ " \" echo innodb_file_per_table=1 >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command ="ssh "+host+ " \" echo innodb_log_buffer_size=64M >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command ="ssh "+host+ " \" echo innodb_buffer_pool_size=4G >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command ="ssh "+host+ " \" echo innodb_thread_concurrency=8 >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command ="ssh "+host+ " \" echo innodb_flush_method=O_DIRECT >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command ="ssh "+host+ " \" echo innodb_flush_log_at_trx_commit=2 >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command ="ssh "+host+ " \" echo wsrep_provider=/usr/lib64/galera/libgalera_smm.so >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command ="ssh "+host+ " \" echo wsrep_cluster_address=\\\'gcomm://"+hostm+"\\\' >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command ="ssh "+host+ " \" echo wsrep_cluster_name=\\\'galera_cluster\\\' >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command ="ssh "+host+ " \" echo wsrep_node_address=\\\'"+host+"\\\' >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command ="ssh "+host+ " \" echo wsrep_node_name=\\\'cemod1\\\' >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command ="ssh "+host+ " \" echo wsrep_sst_method=rsync >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		command ="ssh "+host+ " \" echo wsrep_sst_auth=root:root123 >> /etc/my.cnf.d/server.cnf \""
		utils.run_command(command)
		
	except Exception as e:
		logger.error("Error while Creating configuration file : "+ str(e))
        	logger.exception(e)
	        raise Exception(e)
##################################################Copying Configuration file on all nodes###################################
		
def starting_Mysql_Services():
	try:
		mysql_host = utils.getXmlHostlist("cm_mysqldb","hosts")
		mhost = mysql_host[0]
		for host in mysql_host:
			#if (host == mhost and active):
			if (host == mhost):
				command = "ssh "+host+ " service mysql stop"
				utils.run_command(command)
				command = "ssh "+host+ " \"/etc/init.d/mysql start --wsrep-new-cluster\""
				utils.run_command(command)
				#setup CM accounts and databases
				
				# Make sure DNS is set up properly so all nodes can find all other nodes
			else:
				command = "ssh "+host+ " service mysql stop"
				utils.run_command(command)
				command = "ssh " +host+ " service mysql start"
				utils.run_command(command)
				command = "ssh " +host+ " chkconfig mysql on"
				utils.run_command(command)
	except Exception as e:
		logger.error("Error while Restarting database on first Node : "+ str(e))
		logger.exception(e)
		raise Exception(e)
		

def configureCMUser(host):
	logger.info("Creating CM User on.....!!!!!: " +host)	
	result = os.system("mysql -uroot -proot123 -h "+host+" -e 'show databases'  >> /dev/null 2>&1")
	if result != 0:	
		command="ssh " +host+" ""\" mysql  -e  "    "\\\"grant all privileges on *.* to \'root\'@\'localhost\' identified by \'root123\' WITH GRANT OPTION;flush privileges;" +"\\" +"\""+"\""
		utils.run_command(command)
	command="ssh " +host+" ""\" mysql -u root --password=\'root123\' -e  "    "\\\"grant all privileges on *.* to \'root\'@\'"+host+"\' identified by \'root123\' WITH GRANT OPTION;flush privileges;" +"\\" +"\""+"\""
	utils.run_command(command)
	command="ssh "+host+ " \" mysql -u root --password=\'root123\' -e ""\\\"grant all privileges on *.* to \'root\'@\'"+cm_server_host+"\' identified by \'root123\' WITH GRANT OPTION;" +"\\" +"\""+"\""
	utils.run_command(command)
	command="ssh "+host+ " \"mysql -u root --password=\'root123\' -e \\\"grant all privileges on *.* to \'root\'@\'"+cm_server_host+"\' identified by 'root123' WITH GRANT OPTION;flush privileges;\\\"\""
	utils.run_command(command)
	command="ssh "+host+ " \"mysql -u root --password=\'root123\' -e \\\"grant all privileges on *.* to \'scm\'@\'"+cm_server_host+"\' identified by 'scm' WITH GRANT OPTION;flush privileges;\\\"\""
	utils.run_command(command)
	command="ssh "+host+ " \"mysql -u root --password=\'root123\' -e \\\"grant all privileges on scm.* to \'scm\'@\'%\' identified by 'scm';\\\"\""
	utils.run_command(command)
	command="ssh "+host+ " \"mysql -u root --password=\'root123\' -e " ""+""+"\\\"UPDATE mysql.user SET Password=PASSWORD(\'root123\') WHERE User=\'root\';" +"" +""+"\"\\\""
	utils.run_command(command)
	command="ssh "+host+ " \"mysql -uroot -proot123  -e   "  "\\\"grant all privileges on *.* to \'root\'@\'%\' identified by \'root123\';\\\"\""
	utils.run_command(command)	
	command="ssh "+host+ " \" mysql -u root --password=\'root123\'  -e \\\"create database if not exists amon DEFAULT CHARACTER SET utf8; grant all on amon.* TO \'amon\'@\'%\' IDENTIFIED BY \'amon_password\';grant all on amon.* TO \'amon\'@\'"+ host +"\' IDENTIFIED BY \'amon_password\' WITH GRANT OPTION;\\\"\""
	utils.run_command(command)
	command="ssh "+host+ " \" mysql -u root --password=\'root123\'  -e \\\"create database if not exists rman DEFAULT CHARACTER SET utf8; grant all on rman.* TO \'rman\'@\'%\' IDENTIFIED BY \'rman_password\';grant all on rman.* TO \'rman\'@\'"+ host +"\' IDENTIFIED BY \'rman_password\' WITH GRANT OPTION;\\\"\""
	utils.run_command(command)
	command="ssh "+host+ " \" mysql -u root --password=\'root123\' -e \\\"create database if not exists nav DEFAULT CHARACTER SET utf8; grant all on nav.* TO \'nav\'@\'%\' IDENTIFIED BY \'nav_password\';grant all on nav.* TO \'nav\'@\'"+ host +"\' IDENTIFIED BY \'nav_password\' WITH GRANT OPTION;\\\"\""
	utils.run_command(command)
	command="ssh "+host+ " \" mysql -u root --password=\'root123\'  -e \\\"create database if not exists navms DEFAULT CHARACTER SET utf8; grant all on navms.* TO \'navms\'@\'%\' IDENTIFIED BY \'navms_password\';grant all on navms.* TO \'navms\'@\'"+ host +"\' IDENTIFIED BY \'navms_password\' WITH GRANT OPTION;\\\"\""
	utils.run_command(command)
	command ="ssh "+host+" \" mysql -u root --password=\'root123\'  -e \\\"GRANT PROCESS ON *.* TO \'clustercheckuser\'@\'localhost\' IDENTIFIED BY \'clustercheckpassword\!\';\\\"\""
	utils.run_command(command)
	MYSQL_HA =  utils.getXmlHostlist("cm_mysqldb","mysql_vip_port")
	for element in MYSQL_HA:
		MYSQL_HA_PROXY = element.split(':')
		command  = "ssh "+host+" \" mysql -u root --password=\'root123\' -e \\\" delete from mysql.user where Host=\'"+MYSQL_HA_PROXY[0]+"\' ;\\\" \""
		#command = "ssh "+host+" \" mysql -u root --password=\'root123\' -e \\\" delete from mysql.user where Host=\'"+MYSQL_HA_PROXY[0]+"\' and EXISTS(SELECT 1 FROM mysql.user WHERE Host = \'"+MYSQL_HA_PROXY[0]+"\' LIMIT 1);  \\\" \""
		utils.run_command(command)
		command="ssh "+host+" \" mysql -u root --password=\'root123\' -e \\\"insert into mysql.user (Host,User) values(\'"+MYSQL_HA_PROXY[0]+"\',\'haproxy\' );\\\"\""
		utils.run_command(command)
	
				
def keepalived_setup(vip,mysql_host,priority):
	try:
		logger.info("Enabling keepalived on VIP "+ vip)
		#   get host ip 
		logger.info("Enabling keepalived on host "+mysql_host)
		hostip_cmd = ["ssh "+mysql_host+" hostname -i"]
		hostip_result = Popen(hostip_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True).stdout.read()
		hostip = hostip_result.strip()
		logger.debug("host ip of "+ mysql_host + " is "+ hostip)
		
		ethif_cmd = ["ssh "+mysql_host+" ifconfig -a |grep -B1 "+ hostip+ " |head -n1 | awk '{print $1}'"]
		ethif_result = Popen(ethif_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True).stdout.read()
		ethif = ethif_result.strip()
		logger.debug("host "+ mysql_host + " is connected to eth interface "+ ethif)
		
		# priority for each keepalived server is increased by 1
		
		with open("/opt/cto/conf/keepalived_default.conf", "r") as sources:
			lines = sources.readlines()

		with open("/opt/cto/conf/keepalived.conf","w") as writesource:
			for line in lines:
				if re.search("interface <ethif>", line):
					writesource.write(re.sub("<ethif>",ethif,line))	
				elif re.search("priority <priority>", line):
					writesource.write(re.sub("<priority>",str(priority),line))
				elif re.search("<virtualip>", line):
					writesource.write(re.sub("<virtualip>",vip,line))				
				else:
					writesource.write(line)		

			writesource.close()
			
		#install and start keepalived service demon 
		logger.info("Installing keepalived on host " + mysql_host)
		command="ssh " + mysql_host + " yum -y install keepalived"
		utils.run_command(command)
		
		#copying keepalived.conf to haproxy machine
		logger.info("Copying keepalived.conf file to "+mysql_host)
		command = "scp /opt/cto/conf/keepalived.conf "+ mysql_host +":/etc/keepalived/"
		utils.run_command(command)
		# starting keepalived service
		logger.info("Starting keepalived service on "+ mysql_host)
		command="ssh " + mysql_host + " service keepalived restart"
		utils.run_command(command)
		
		# delete the keepalived.conf file in /opt/cto/conf
		command="rm -rf /opt/cto/conf/keepalived.conf"
		utils.run_command(command)
		
		logger.info("Successfully enabled keepalived on host "+ mysql_host)
		return 0
	except Exception as e:
		logger.error("Error setting up keepalived service :" + str(e) )
		logger.exception(e)
		raise Exception(e)
		
############################################################################################################################3
		
def haproxy_setup(proxy_port,host):
	listhost = utils.getXmlHostlist("cm_mysqldb","hosts")
	try:
		#configuring haproxy.cfg file
		logger.info("Configuring haproxy on proxy port "+ proxy_port)
		cnt=1
		with open("/opt/cto/conf/maria_haproxy_default.cfg", "r") as sources:
			lines = sources.readlines()

		with open("/opt/cto/conf/haproxy.cfg","w") as writesource:
			for line in lines:
				if re.match("^listen",line):
					writesource.write(re.sub("<proxy_port>",proxy_port, line))
				elif re.search("<mysqlserver1>", line):
					for h in listhost:
						writesource.write(re.sub("<mysqlserver1>",h,re.sub("mysqlser<no>","mysqlser"+str(cnt),line)))	
						cnt=cnt+1	
				else:	
					writesource.write(line)		

			writesource.close()	
		# configure haproxy on each HiveServer2 hosts
		
		#install and start haproxy
		logger.info("Installing haproxy on host " + host + " Port:"+proxy_port)
		command="ssh " + host + " yum -y install haproxy"
		utils.run_command(command)

		#copying haproxy.cfg to haproxy machine
		logger.debug("Copying haproxy.cfg file to "+host)
		command = "scp /opt/cto/conf/haproxy.cfg "+ host +":/etc/haproxy/"
		# starting haproxy service
		utils.run_command(command)
		logger.info("Starting haproxy service on "+ host)
		command="ssh " + host + " service haproxy restart"
		utils.run_command(command)
		command = "ssh " + host + " chkconfig haproxy on"
		utils.run_command(command)
		#delete  haproxy.cfg file in /opt/cto/conf
		del_cmd = "rm -rf /opt/cto/conf/haproxy.cfg"
		utils.run_command(del_cmd)
		logger.info("Haproxy successfully installed on port ")
		return 0
	except Exception as e:
		logger.error("Error setting up haproxy : "+ str(e))
		logger.exception(e)
		raise Exception(e)
	

###################################################################################################################################

'''def configuringDatabaseAccounts(hosts):
	try:
		
	except:
		logger.error("Error while configuring datbase for CM : "+ str(e))
		logger.exception(e)
		raise Exception(e)'''

def enableXinietd(host):
	logger.info("Enabling Xinited for MySQL on: "+host)
	try:
		command = "ssh "+host+" \"sed -e \'/9294/ s/^#*/#/\' -i /etc/services\""
		utils.run_command(command)
		command = "ssh "+host+" \"echo \\\"mysqlchk            9294/tcp\\\" >> /etc/services\""
		utils.run_command(command)
		command = "ssh "+host+" yum -y install xinetd"
		utils.run_command(command)
		command  = "chmod 777 /opt/cto/scripts/clustercheck "
		utils.run_command(command)
		command  = "scp /opt/cto/scripts/clustercheck "+host+":/usr/bin/"
		utils.run_command(command)
		command  = "ssh "+host+" chmod 777 /usr/bin/clustercheck"
		utils.run_command(command)
		command  = "chmod 777 /opt/cto/conf/mysqlchk "
		utils.run_command(command)
		command  = "scp /opt/cto/conf/mysqlchk "+host+":/etc/xinetd.d/"
		utils.run_command(command)
		command  = "ssh "+host+" chmod 777 /etc/xinetd.d/mysqlchk"
		utils.run_command(command)
		command = "ssh "+host+" service xinetd restart"
		utils.run_command(command)
		
	except Exception as e:
		logger.error("Error while enabling Xinited " + str(e))
		logger.exception(e)
		raise Exception(e)


	
def enableMySQLServerHA(host,priority):
	try:
		logger.info("Enabling HA for MySQL ")
		## HA related 
		MYSQL_HA =  utils.getXmlValue("cm_mysqldb","mysql_vip_port").split(':')
		print MYSQL_HA[0]
		print MYSQL_HA[1]
		keepalived_setup(MYSQL_HA[0],host,priority)
		#configure and enable Haproxy
		haproxy_setup(MYSQL_HA[1],host)
		
		#configure and enable keepalived for HA of Haproxy
		enableXinietd(host)
		logger.info("Successfully enabled HA for MySQL")
	except Exception as e:
		logger.error("Error enabling HA for MySQL " + str(e))
		logger.exception(e)
		raise Exception(e)
		
##############################################################################################################################
def main():
	try:
		mysql_Install()
		creating_Config_File()
		
		hosts = utils.getXmlHostlist("cm_mysqldb","hosts")
		configureCMUser(hosts)
		if len(hosts) > 1:
			priority = 101
			for host in hosts:
				enableMySQLServerHA(host,priority)
				priority -= 1
		#configuringDatabaseAccounts(hosts)
		
		starting_Mysql_Services()		
	except Exception, e:
		logger.error("Error setting HA :" + str(e) )
		logger.exception(e)
		raise Exception(e)

###############################################################################################################################

if __name__ == "__main__":
   main()


