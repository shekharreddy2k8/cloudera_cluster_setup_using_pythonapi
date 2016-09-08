import cm_utils, ctologger, re, utils, subprocess
from subprocess import Popen, PIPE, STDOUT 

logger = ctologger.getCdhLogger()

# CM account info
CM_HOST = utils.getXmlValue("manager", "host")
CLUSTER_NAME = utils.getXmlValue("distribution", "cluster_name")


# Deploys Hive - hive metastore, hiveserver2, 
def deploy_hive(cluster):
	first_run=True
	try:
		### Hive configuration ###
		HIVE_SERVICE_NAME = utils.getXmlRole("hive")
		HIVE_SERVICE_CONFIG = utils.getXmlConf("hive","hive-service-config")
		HIVE_HMS_CONFIG = utils.getXmlConf("hive","hive-metastore-config")
		HIVE_HS2_CONFIG = utils.getXmlConf("hive","hive-server2-config")
		HIVE_HS2_HOST = utils.getXmlHostlist("hive", "hosts")
		HIVE_GW_CONFIG = utils.getXmlConf("hive","hive-gateway-config")

		HIVE_METASTORE_PWD = utils.getXmlValue("hive", "hive-service-config" ,"hive_metastore_database_password")
		HIVE_DB_TYPE = utils.getXmlValue("hive","hive-service-config","hive_metastore_database_type")
		
		if len(HIVE_HS2_HOST) > 0:
			logger.info('Deploying Hive Service...')
			hive_service = cm_utils.getServiceObject(HIVE_SERVICE_NAME)
			if hive_service is not None:
				logger.info("HIVE service already exists")
				first_run=False				
			else:
				# wait for other commands if any to complete before creating hive service
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
				## checking if hdfs is running, exit if not running
				if (cm_utils.getServiceStatus(CLUSTER_NAME,"HDFS") == 1):
					logger.error("HDFS service is not running. Exiting from deploying HIVE ")
					raise Exception("Error : HDFS service is not running. Exiting from deploying HIVE")
				
				logger.info("Creating Hive Service...")
				hive_service = cluster.create_service(HIVE_SERVICE_NAME, HIVE_SERVICE_NAME)
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
				hive_service.update_config(HIVE_SERVICE_CONFIG)
				
				hms = hive_service.get_role_config_group("{0}-HIVEMETASTORE-BASE".format(HIVE_SERVICE_NAME))
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
				hms.update_config(HIVE_HMS_CONFIG)
				
				hs2 = hive_service.get_role_config_group("{0}-HIVESERVER2-BASE".format(HIVE_SERVICE_NAME))
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
				hs2.update_config(HIVE_HS2_CONFIG)
				
				#gw = hive_service.get_role_config_group("{0}-GATEWAY-BASE".format(HIVE_SERVICE_NAME))
				#gw.update_config(HIVE_GW_CONFIG)

			HIVE_DEPLOY_HOSTS = cm_utils.getNewHosts(CM_HOST,CLUSTER_NAME,"hive", "hosts")
			
			hiveserver = len(HIVE_HS2_HOST)-len(HIVE_DEPLOY_HOSTS)
			for host in HIVE_DEPLOY_HOSTS:
				hiveserver += 1
				# create hivemetastore role
				logger.debug("Creating HiveMetastore role...")
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
				hive_service.create_role("{0}-hms-".format(HIVE_SERVICE_NAME) + str(hiveserver), "HIVEMETASTORE", host)
				
				# create hiveserver2 role
				logger.debug("Creating HiveServer2 role...")
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
				hive_service.create_role("{0}-hs2-".format(HIVE_SERVICE_NAME) +str(hiveserver), "HIVESERVER2", host)
				
				# create hive gateway role
				#cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)				
				#hive_service.create_role("{0}-gw-".format(HIVE_SERVICE_NAME) + str(hiveserver), "GATEWAY", host)
				
				logger.info("Deployed Hive service " + HIVE_SERVICE_NAME + " using HiveMetastoreServer on " + str(host) + " and HiveServer2 on " + str(host))
				# copy jdbc connector jars to /usr/share/java and create soft link
				copyJdbcConnectorJars(host,HIVE_DB_TYPE)

			
			HIVE_GW_HOSTS = utils.getXmlHostlist("hive", "gateway")
			if len(HIVE_GW_HOSTS) > 0:
				HIVE_GW_DEPLOY_HOSTS = cm_utils.getNewHosts(CM_HOST,CLUSTER_NAME,"hive", "gateway")
				gateway = len(HIVE_GW_HOSTS)-len(HIVE_GW_DEPLOY_HOSTS)
					
				gw = hive_service.get_role_config_group("{0}-GATEWAY-BASE".format(HIVE_SERVICE_NAME))
				gw.update_config(HIVE_GW_CONFIG)
				
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
				for host in HIVE_GW_DEPLOY_HOSTS:
					gateway += 1
					hive_service.create_role("{0}-gw-".format(HIVE_SERVICE_NAME) + str(gateway), "GATEWAY", host)
			
			if first_run==True:	
				init_hive(hive_service,CM_HOST,CLUSTER_NAME)
			
			# enable HA only if there are more than one hosts
			if len(HIVE_HS2_HOST) > 1 and len(HIVE_DEPLOY_HOSTS) > 0:
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
				enableHiveServerHA(HIVE_DEPLOY_HOSTS)
			
			cm_utils.startService(cluster,HIVE_SERVICE_NAME)
			
			logger.info("Hive service deployed successfully")
			return hive_service
	except Exception, e:   
		logger.error("Error in deploy Hive. Exception:" + str(e))
		logger.exception(e)
		raise Exception(e)

def init_hive(hive_service,cm_host,clustername):
	try:
		logger.info("Inside init hive.. Creating Hive warehouse directory")
		cm_utils.waitForRunningCommand(cm_host,clustername)
		cmd=hive_service.create_hive_warehouse()
		cm_utils.waitForRunningCommand(cm_host,clustername)
		cmd = cmd.fetch()
		if cmd.success == True:
			logger.info("Hive warehouse created successfully...")
		else:
			logger.info("Hive warehouse creation failed...")
		return cmd.success
	except Exception, e:
		logger.error("Exception in Creating hive warehouse directory:"+str(e))
		logger.exception(e)
		
def enableHiveServerHA(hive_hosts):
	try:
		logger.info("Enabling HA for Hive ")
		## HA related 
		HIVE_HA_PROXY =  utils.getXmlValue("hive","hive-server2-config","hiverserver2_load_balancer").split(':')
		if len(HIVE_HA_PROXY) == 2:
			HIVE_HA_VIRTUAL_IP = HIVE_HA_PROXY[0]
			HIVE_HA_PROXY_PORT = HIVE_HA_PROXY[1]
			
			#configure and enable Haproxy
			haproxyRunning = isHaProxyRunningOnAllHosts(hive_hosts)
			if(haproxyRunning == True):
			# append haproxy cong to existing configuration
				append_haproxy(HIVE_HA_PROXY_PORT,hive_hosts)
			else:
			# set up haproxy by installing haproxy and configurations
				haproxy_setup(HIVE_HA_PROXY_PORT,hive_hosts)
			
			
			#configure and enable keepalived for HA of Haproxy	
			keepalivedRunning = isKeepalivedRunningOnAllHosts(hive_hosts)
			if(keepalivedRunning == True):
				logger.info("Keepalived is already running on all hosts. So using existing setup")
			else:
				keepalived_setup(HIVE_HA_VIRTUAL_IP,hive_hosts)
			
			logger.info("Successfully enabled HA for Hive")
			return 0
			
		else:
			logger.error("Wrong arguments for hiverserver2_load_balancer property. Excepted format is vip:port")
			raise Exception("Wrong arguments for hiverserver2_load_balancer property. Excepted format is vip:port")

	except Exception, e:
		logger.error("Error enabling HA for Hive Server " + str(e))
		logger.exception(e)
		raise Exception(e)
	
def	copyJdbcConnectorJars(host,HIVE_DB_TYPE):
	try:
		logger.info("Copying connector jars to "+ host)
		if HIVE_DB_TYPE == "mysql":
			command="mkdir -p /tmp/cto/; tar -zxvf /opt/cto/distribution/mysql-connector-java-5.1.39.tar.gz -C /tmp/cto/;"
			utils.run_command(command)
			command="scp /tmp/cto/mysql-connector-java-5.1.39/mysql-connector-java-5.1.39-bin.jar "+host+":/usr/share/java/"
			utils.run_command(command)
			command="ssh "+host+" ln -sf /usr/share/java/mysql-connector-java-5.1.39-bin.jar /usr/share/java/mysql-connector-java.jar"
			utils.run_command(command)
			command="rm -rf /tmp/cto"
			utils.run_command(command)
		elif HIVE_DB_TYPE == "postgresql":
			command="scp /opt/cto/distribution/postgresql-9.2-1004.jdbc4.jar "+host+":/usr/share/java/"
			utils.run_command(command)
			command="ssh "+host+" ln -sf /usr/share/java/postgresql-9.2-1004.jdbc4.jar /usr/share/java/postgresql.jar"
			utils.run_command(command)		
		else:
			logger.error("Invalid database type " + str(HIVE_DB_TYPE) + " Valid database types are postgresql and mysql. jdbc connector jars not copied")
	except Exception, e:
		logger.error("Error copying jdbc connector jars "+ str(e))	
		logger.exception(e)
		raise Exception(e)
		
def keepalived_setup(vip,hive_hosts):
	try:
		priority = 100
		logger.info("Enabling keepalived on VIP "+ vip)
		for host in hive_hosts:
		#   get host ip 
			logger.info("Enabling keepalived on host "+host)
			hostip_cmd = ["ssh "+host+" hostname -i"]
			hostip_result = Popen(hostip_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True).stdout.read()
			hostip = hostip_result.strip()
			logger.debug("host ip of "+ host + " is "+ hostip)
			
			ethif_cmd = ["ssh "+host+" ifconfig -a |grep -B1 "+ hostip+ " |head -n1 | awk '{print $1}'"]
			ethif_result = Popen(ethif_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True).stdout.read()
			ethif = ethif_result.strip()
			logger.debug("host "+ host + " is connected to eth interface "+ ethif)
			
			# priority for each keepalived server is increased by 1
			priority = priority+1
			with open("/opt/cto/conf/keepalived_default.conf", "r") as sources:
				lines = sources.readlines()

			with open("/tmp/keepalived.conf","w") as writesource:
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
			logger.info("Installing keepalived on host " + host)
			command="ssh " + host + " yum -y install keepalived"
			utils.run_command(command)
			
			#copying keepalived.conf to haproxy machine
			logger.info("Copying keepalived.conf file to "+host)
			command = "scp /tmp/keepalived.conf "+ host +":/etc/keepalived/"
			utils.run_command(command)
			# starting keepalived service
			logger.info("Starting keepalived service on "+ host)
			command="ssh " + host + " service keepalived restart"
			utils.run_command(command)
			
			logger.info("Starting keepalived service on reboot "+ host)
			command="ssh " + host + " chkconfig keepalived on"
			utils.run_command(command)
			
			# delete the keepalived.conf file in /tmp
			command="rm -rf /tmp/keepalived.conf"
			utils.run_command(command)
			
			logger.info("Successfully enabled keepalived on host "+ host)
		return 0
	except Exception, e:
		logger.error("Error setting up keepalived service :" + str(e) )
		logger.exception(e)
		raise Exception(e)
		
def haproxy_setup(proxy_port,hive_hosts):
	try:
		#configuring haproxy.cfg file
		logger.info("Configuring haproxy on servers "+ str(hive_hosts) + " with proxy port "+ proxy_port)
		cnt=1
		with open("/opt/cto/conf/hive_haproxy_default.cfg", "r") as sources:
			lines = sources.readlines()

		with open("/tmp/haproxy.cfg","w") as writesource:
			for line in lines:
				if re.match("^listen",line):
					writesource.write(re.sub("<proxy_port>",proxy_port, line))
				elif re.search("<hive_server2_host>", line):
					for host in hive_hosts:
						writesource.write(re.sub("<hive_server2_host>",host,re.sub("hiveser<no>","hiveser"+str(cnt),line)))	
						cnt=cnt+1	
				else:	
					writesource.write(line)		

			writesource.close()
	
		# configure haproxy on each HiveServer2 hosts
		for host in hive_hosts:
			#install and start haproxy
			logger.info("Installing haproxy on host " + host + " Port:"+proxy_port)
			command="ssh " + host + " yum -y install haproxy"
			utils.run_command(command)

			#copying haproxy.cfg to haproxy machine
			logger.debug("Copying haproxy.cfg file to "+host)
			command = "scp /tmp/haproxy.cfg "+ host +":/etc/haproxy/"
			# starting haproxy service
			utils.run_command(command)
			logger.info("Starting haproxy service on "+ host)
			command="ssh " + host + " service haproxy restart"
			utils.run_command(command)
			
			logger.info("Enabling haproxy service startup on reboot "+ host)
			command="ssh " + host + " chkconfig haproxy on"
			utils.run_command(command)
		
		#delete  haproxy.cfg file in /tmp
		del_cmd = "rm -rf /tmp/haproxy.cfg"
		utils.run_command(del_cmd)
		logger.info("Haproxy successfully installed on port ")
		return 0
	except Exception, e:
		logger.error("Error setting up haproxy : "+ str(e))
		logger.exception(e)
		raise Exception(e)

def isHaProxyRunningOnAllHosts(hosts):
	try:
		logger.info("Checking if Haproxy is running on "+str(hosts))
		noOfHostRunning = 0
		for host in hosts:
			cmd = subprocess.Popen(['ssh', host ,'service', 'haproxy', 'status'], stdout=subprocess.PIPE)
			string = cmd.stdout.read()
			string = string.rstrip()
			
			if re.search("is running",string):
				noOfHostRunning = noOfHostRunning + 1;
				logger.info("HAProxy is running on " + host)		
			else:
				logger.info("HAProxy is not running on " + host)
				
		if noOfHostRunning == len(hosts):
			logger.info("HAProxy is running on all hosts")
			return True
		elif noOfHostRunning == 0:
			logger.info( "HAProxy is not running on any hosts" )
			return False
		else:
			logger.error("HAProxy is not running on all.. Wrong hosts configuration")
			raise Exception("HAProxy is not running on all.. Wrong hosts configuration")		
			
	except Exception, e:
		logger.error("Exception in isHaProxyRunningOnAllHosts while checking on hosts "+ str(host) + " Error is :"+ str(e))
		logger.exception(e)
		raise Exception(e)

def isKeepalivedRunningOnAllHosts(hosts):
	try:
		logger.info("Checking if keepalived is running on "+str(hosts))
		noOfHostRunning = 0
		for host in hosts:
			cmd = subprocess.Popen(['ssh', host ,'service', 'keepalived', 'status'], stdout=subprocess.PIPE)
			string = cmd.stdout.read()
			string = string.rstrip()
			
			if re.search("is running",string):
				noOfHostRunning = noOfHostRunning + 1;
				logger.info("Keepalived is running on " + host)		
			else:
				logger.info("Keepalived is not running on " + host)
				
		if noOfHostRunning == len(hosts):
			logger.info("Keepalived is running on all hosts")
			return True
		elif noOfHostRunning == 0:
			logger.info( "Keepalived is not running on any hosts" )
			return False
		else:
			logger.error("Keepalived is not running on all.. Wrong hosts configuration")
			raise Exception("Keepalived is not running on all.. Wrong hosts configuration")	
			
	except Exception, e:
		logger.error("Exception in isKeepalivedRunningOnAllHosts while checking on host "+ str(host) + " Error is :"+ str(e))
		logger.exception(e)
		raise Exception(e)
		
def append_haproxy(proxy_port,hive_hosts):
	try:
		logger.info("Appending haproxy on servers "+ str(hive_hosts) + " with proxy port "+ proxy_port)
		for host in hive_hosts:
			#copy haproxy.cfg file to append the configurations for hive
			command="scp " + host + ":/etc/haproxy/haproxy.cfg /tmp"
			utils.run_command(command)
			logger.debug("haproxy.cfg copy command : " + command)
			
			with open("/tmp/haproxy.cfg","r") as existing_source:
				existing_lines = existing_source.readlines()
			existing_source.close()
			
			# check if hiveserver2 related listen configuration already exist
			hive_config = False
			for existing_line in existing_lines:
				if re.match("listen hive_proxy",existing_line):
					hive_config = True
					# remove the conf file copied in tmp
					command="rm -f /tmp/haproxy.cfg"
					utils.run_command(command)
					logger.info("hiveserver2 related haproxy listen configuration already exists on host "+ host)
			
			# append config only if hive config does not exist
			if(not hive_config):
				# read the default hive_haproxy_default file
				with open("/opt/cto/conf/hive_haproxy_default.cfg", "r") as sources:
					lines = sources.readlines()
				sources.close()
			
				hive_config_string ="\n\n"
				
				hive_listen_config = 0
				cnt=1
				# create hive related haproxy string which will be added to haproxy.cfg file
				for line in lines:
					if re.match("^listen hive_proxy",line):
						hive_config_string = hive_config_string + re.sub("<proxy_port>",proxy_port, line)
						hive_listen_config = 1
					elif re.search("<hive_server2_host>", line):
						for host1 in hive_hosts:
							hive_config_string = hive_config_string + re.sub("<hive_server2_host>",host1,re.sub("hiveser<no>","hiveser"+str(cnt),line))
							cnt=cnt+1
					elif hive_listen_config == 1:
						hive_config_string = hive_config_string + line
				
				# add the hive listen haproxy to cfg file
				logger.debug("Appending hive related haproxy conf to haproxy.cfg file. Listen String added is: "+ hive_config_string)
				with open("/tmp/haproxy.cfg","a") as writesource:
					writesource.write(hive_config_string)
				writesource.close()
				
				# copy back the configuration file to the server
				command="scp /tmp/haproxy.cfg " + host + ":/etc/haproxy/" #scp overwrites by default
				utils.run_command(command)
				logger.info("Copied the hive related haproxy configurations to server "+ host)
					
				logger.info("Re-Starting haproxy service on "+ host)
				command="ssh " + host + " service haproxy restart"
				utils.run_command(command)
								
				#delete  haproxy.cfg file in /tmp
				del_cmd = "rm -rf /tmp/haproxy.cfg"
				utils.run_command(del_cmd)			
				
	except Exception, e:
		logger.error("Error while appending haproxy conf for hiveserver2. Exception is "+ str(e))
		logger.exception(e)
		raise Exception(e)
	
