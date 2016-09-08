from cm_api.api_client import ApiResource
import os, sys, utils, time, ctologger

logger = ctologger.getCdhLogger()

# Map for service name role name mapping to XML name mappings

serviceRoleMap = {
	"hdfs-Master" : "NAMENODE",
	"hdfs-Slave" : "DATANODE",
	"yarn-Master" : "RESOURCEMANAGER",
	"yarn-Slave" : "NODEMANAGER",
	"yarn-gateway" : "GATEWAY",
	"hbase-Master" : "MASTER",
	"hbase-Slave" : "REGIONSERVER",
	"hive-hosts" : "HIVESERVER2",
	"zookeeper-hosts" : "SERVER",
	"spark-hosts" : "SPARK_YARN_HISTORY_SERVER",
	"hive-gateway" : "GATEWAY",
	"hbase-gateway" : "GATEWAY",
	"sparkthrift-hosts" : "SPARK_THRIFT"
	}

def getclusterObject(cm_server_host, clustername):
	try:
		cm_username = utils.getXmlValue("manager", "cm_user")
		cm_password = utils.getXmlValue("manager", "cm_pass")
		resource = ApiResource(cm_server_host,username=cm_username, password=cm_password)
		cluster = resource.get_cluster(clustername)
		logger.info("using existing cluster object")
	except Exception as e:
		if "not found" in e.message:
			cluster = resource.create_cluster(clustername,"CDH5","5.7.0")
			logger.info("created the newcluster")
	else:
		logger.error("Something went wrong....!!" + str(e.message))

	return cluster

# Method to fetch new hosts not in cluster yet but in XML.
def getNonClusterHosts(cm_server_host, clustername):
  
	if utils.pingHost(cm_server_host) != 0:
		logger.error("CM not reachable ", str(cm_host))
		return
	cm_username = utils.getXmlValue("manager", "cm_user")
	cm_password = utils.getXmlValue("manager", "cm_pass")
	resource = ApiResource(cm_server_host,username=cm_username, password=cm_password)
	try:
		cluster = resource.get_cluster(clustername)
		reflist = cluster.list_hosts()
	except Exception as e:
		if "not found" in e.message:
			logger.error("Object Not Found, Cannot retrieve the Host List from Cluster "+ str(clustername) + " !!")
			return [ ]
  
	exHosts = [ ]
	for r in reflist:
		host = resource.get_host(r.hostId)
		exHosts.append(host.hostname)
  
	nodelist = [ ]
	for key, value in serviceRoleMap.items():
		key = key.split("-")
		nodestr = utils.getXmlValue(key[0],key[1])
		if nodestr != None:
			list = nodestr.split(",")
			for node in list:
				if node not in nodelist and len(node) > 0:
					nodelist.append(node)

	newnode = [ ]
	for node in nodelist:
		if node not in exHosts and len(node) > 0:
			newnode.append(node)
	
	break_loop = False
	while break_loop == False:
		break_loop = True
		logger.info("Looking for all hosts to be registered/listed with Cloudera manager")
		allhosts = resource.get_all_hosts()
		listedhosts = []
		for host in allhosts:
			listedhosts.append(host.hostname)
		for host in newnode:
			if host not in listedhosts:
				break_loop = False
		time.sleep(5)
	
	return newnode


# Method to fetch new hosts in any service any role
def getNewHosts(cm_server_host, clustername, servicename, rolename):
  
	if utils.pingHost(cm_server_host) != 0:
		logger.error("CM not reachable ", str(cm_host))
		return

	nodestr = utils.getXmlValue(servicename,rolename)
	if len(nodestr) < 1:
		logger.error("No Hosts from XML to be added into the Cluster!! ")
		return [ ]
	
	xmllist = nodestr.split(",")

	rolename = serviceRoleMap[servicename+"-"+rolename]
	servicename = utils.getXmlRole(servicename)
 
	try:
		cm_username = utils.getXmlValue("manager", "cm_user")
		cm_password = utils.getXmlValue("manager", "cm_pass")
		resource = ApiResource(cm_server_host,username=cm_username, password=cm_password)
		cluster = resource.get_cluster(clustername)
		service = cluster.get_service(servicename)
		reflist = service.get_roles_by_type(rolename)
	except Exception as e:
		if "not found" in e.message:
			logger.error(str(servicename) + "Object Not Found, Cannot retrieve the "+ str(rolename) + "Role List!!")
			return [ ]
  
	exHosts = [ ]
	for r in reflist:
		host = resource.get_host(r.hostRef.hostId)
		exHosts.append(host.hostname)

	newHosts = [ ]
	for h in xmllist:
		if h not in exHosts and len(h) > 0:
			newHosts.append(h)
  
	return newHosts

# Method to hostId with hostname or IpAddress
def getHostId( cm_host, host_ip):
	if utils.pingHost(cm_host) != 0:
		logger.error("CM not reachable ", str(cm_host))
		return
	cm_username = utils.getXmlValue("manager", "cm_user")
	cm_password = utils.getXmlValue("manager", "cm_pass")
	resource = ApiResource(cm_host, username=cm_username, password=cm_password)
	hostId = ''
	for h in  resource.get_all_hosts():
		if host_ip == h.ipAddress or host_ip == h.hostname:
			hostId=h.hostId
			break
	return hostId

# startService method starts the service, checks the status of the service for attempt times and returns 0 if started, 1 if could not started
def startService(cluster, servicename):
	cnt=1
	cm_server_host = utils.getXmlValue("manager","host")
	if cluster.get_service(servicename).serviceState == "STARTED":
		res = startRole(cluster, servicename)
		return res
	else:
		logger.info("Starting " + str(servicename) + " service...")
		waitForRunningCommand(cm_server_host,cluster.name)
		cmd = cluster.get_service(servicename).start()
		waitForRunningCommand(cm_server_host,cluster.name)
		cmd = cmd.fetch()
		
		if cmd.success == True:	
			logger.info(str(servicename) + " service started successfully...")
			return cmd.success
		else:
			logger.info(str(servicename) + " Failed to start.... ")
			return cmd.success		

def startRole(cluster, servicename):
	result = True
	cm_server_host = utils.getXmlValue("manager","host")
	for r in cluster.get_service(servicename).get_all_roles():
		if r.roleState != "STARTED":
			logger.info("Starting " + str(r.name) + " role...")
			waitForRunningCommand(cm_server_host,cluster.name)
			cmd = cluster.get_service(servicename).start_roles(r.name)
			waitForRunningCommand(cm_server_host,cluster.name)
			if len(cmd) > 0:
				cmd1 = cmd[0].fetch()
				
				if cmd1.success == True:
					logger.info(str(r.name) + " role started successfully...")
				else:
					logger.info(str(r.name) + " Failed to start....")
					result = False
	
	return result
					
def waitForRunningCommand(cm_server_host,clustername):
	cm_username = utils.getXmlValue("manager", "cm_user")
	cm_password = utils.getXmlValue("manager", "cm_pass")
	resource = ApiResource(cm_server_host,username=cm_username, password=cm_password)
	while True:
		cluster = resource.get_cluster(clustername)
		cmdcnt = len(cluster.get_commands())
		for service in cluster.get_all_services():
			cmdcnt += len(service.get_commands())
			for role in service.get_all_roles():
					cmdcnt += len(role.get_commands())
		if cmdcnt > 0:
			time.sleep(2)
		else:	
			return 0

def getServiceStatus(clustername,servicename):
	try:
		cm_server_host = utils.getXmlValue("manager","host")
		cm_username = utils.getXmlValue("manager", "cm_user")
		cm_password = utils.getXmlValue("manager", "cm_pass")
		resource = ApiResource(cm_server_host,username=cm_username, password=cm_password)
		cluster = resource.get_cluster(clustername)
		service = cluster.get_service(servicename)
		if service.serviceState == "STARTED":
			return 0
		else:
			return 1
	except Exception as e:
		logger.error("Failed to retrieve the service object "+str(e))
		return 1

def getServiceObject(servicename):
	cm_server_host = utils.getXmlValue("manager","host")
	clustername = utils.getXmlValue("distribution","cluster_name")
	cm_username = utils.getXmlValue("manager", "cm_user")
	cm_password = utils.getXmlValue("manager", "cm_pass")
	try:
		resource = ApiResource(cm_server_host,username=cm_username, password=cm_password)
		cluster = resource.get_cluster(clustername)
		try:
			service = cluster.get_service(servicename)
			return service
		except Exception as e:
			logger.debug("No service object available!! Create New!!")
			if "not found" in str(e):
				return None
	except Exception as e:
		logger.error("Failed to retrieve the cluster object "+str(e))
		if "not found" in str(e):
			return None
			