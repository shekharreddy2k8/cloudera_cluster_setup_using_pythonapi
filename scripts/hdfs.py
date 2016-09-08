import utils, cm_utils, ctologger, time

logger = ctologger.getCdhLogger()

### CM ###
CM_HOST = utils.getXmlValue("manager", "host")
CLUSTER_NAME = utils.getXmlValue("distribution", "cluster_name")
   
# Deploys HDFS - NN, DNs, SNN .

def deploy_hdfs(cluster):
	HDFS_SERVICE_NAME = utils.getXmlRole("hdfs")
	HDFS_SERVICE_CONFIG = utils.getXmlConf("hdfs","hdfs-service-config")
	HDFS_NAMENODE_SERVICE_NAME = "nn"
	HDFS_NAMENODE_HOST = utils.getXmlHostlist("hdfs", "Master")
	HDFS_NAMENODE_CONFIG = utils.getXmlConf("hdfs","hdfs-namenode-config")

	HDFS_SECONDARY_NAMENODE_CONFIG = utils.getXmlConf("hdfs","hdfs-secondarynamenode-config")
	HDFS_DATANODE_HOSTS = utils.getXmlHostlist("hdfs", "Slave")
	HDFS_DATANODE_CONFIG = utils.getXmlConf("hdfs","hdfs-datanode-config")

	try:
		if len(HDFS_NAMENODE_HOST[0]) > 0:
			logger.info('Deploying HDFS Service....')
			hdfs_service = None
			first_run = True
			hdfs_service = cm_utils.getServiceObject(HDFS_SERVICE_NAME)
			if hdfs_service is not None:
				logger.info("HDFS service already exists")
				first_run = False
			else:
				logger.info("Creating HDFS service")
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
				hdfs_service = cluster.create_service(HDFS_SERVICE_NAME, HDFS_SERVICE_NAME)
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
				hdfs_service.update_config(HDFS_SERVICE_CONFIG)

				nn_role_group = hdfs_service.get_role_config_group("{0}-NAMENODE-BASE".format(HDFS_SERVICE_NAME))
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)				
				nn_role_group.update_config(HDFS_NAMENODE_CONFIG)
				logger.info("Updated HDFS NAME NODE config")
				nn_service_pattern = "{0}-" + HDFS_NAMENODE_SERVICE_NAME
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)				
				hdfs_service.create_role(nn_service_pattern.format(HDFS_SERVICE_NAME), "NAMENODE", HDFS_NAMENODE_HOST[0])
				logger.info("Created HDFS NAME NODE role with name "+ str(nn_service_pattern))

				snn_role_group = hdfs_service.get_role_config_group("{0}-SECONDARYNAMENODE-BASE".format(HDFS_SERVICE_NAME))
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)				
				snn_role_group.update_config(HDFS_SECONDARY_NAMENODE_CONFIG)
				hdfs_service.create_role("{0}-snn".format(HDFS_SERVICE_NAME), "SECONDARYNAMENODE", HDFS_NAMENODE_HOST[0])
				logger.info("Created HDFS SECONDARY NAME NODE role with name "+ HDFS_SERVICE_NAME +"-snn")

				dn_role_group = hdfs_service.get_role_config_group("{0}-DATANODE-BASE".format(HDFS_SERVICE_NAME))
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
				dn_role_group.update_config(HDFS_DATANODE_CONFIG)
				logger.info("Updated HDFS DATA NODE config")
			
			HDFS_DATANODE_DEPLOY_HOSTS = cm_utils.getNewHosts(CM_HOST,CLUSTER_NAME,"hdfs", "Slave")
			datanode=len(HDFS_DATANODE_HOSTS)-len(HDFS_DATANODE_DEPLOY_HOSTS)
			for host in HDFS_DATANODE_DEPLOY_HOSTS:
				datanode += 1
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)			
				hdfs_service.create_role("{0}-dn-".format(HDFS_SERVICE_NAME) + str(datanode), "DATANODE", host)
				logger.info("Created HDFS DATA NODE role with name "+ HDFS_SERVICE_NAME +"-dn-" + str(datanode))
			
			if first_run == True:
				init_hdfs(hdfs_service, HDFS_SERVICE_NAME)
				logger.info("Initialized HDFS service")

			cm_utils.startService(cluster,HDFS_SERVICE_NAME)
			
			post_startup_hdfs(hdfs_service)
			if first_run == True and len(HDFS_NAMENODE_HOST) == 2:
				enable_hdfs_ha(hdfs_service,HDFS_NAMENODE_HOST[1],HDFS_SERVICE_NAME)
			
			logger.info("Deployed HDFS service " + HDFS_SERVICE_NAME + " using NameNode on " + str(HDFS_NAMENODE_HOST) + " and DataNodes running on: " + str(HDFS_DATANODE_DEPLOY_HOSTS))
			
			return hdfs_service
	except Exception, e:
		logger.error("Error in deploy HDFS. Exception:" + str(e))
		logger.exception(e)
		raise Exception(e)
		
		
# Initializes HDFS - format the file system
def init_hdfs(hdfs_service, hdfs_name):
	logger.info('Inside init_hdfs...Formatting NameNode')
	cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)	
   	cmd = hdfs_service.format_hdfs("{0}-nn".format(hdfs_name))[0]
	cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
	cmd = cmd.fetch()

	if cmd.success != True:
		logger.error( "Failed to format HDFS, attempting to continue with the setup "+ str(cmd.resultMessage))
		return cmd.success
	
	logger.info("Successfully formatted HDFS..!!")
	return cmd.success


# Executes steps that need to be done after the final startup once everything is deployed and running.
def post_startup_hdfs(hdfs_service):
	#Create HDFS temp dir
	cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
	cmd=hdfs_service.create_hdfs_tmp()
	cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
	cmd = cmd.fetch()
	
	if cmd.success == True:
		logger.info("Successfully created temp directory..!!")
	else:
		logger.error("Failed to create the temp directory..!!")
	
	return cmd.success

def enable_hdfs_ha(hdfs_service,standbynamenode,hdfs_servicename):
	try:
		HDFS_JOURNAL_HOSTS = utils.getXmlHostlist("hdfs", "journal")
		
		jns = []
		jnnode=0
		logger.info("Enabling HA for HDFS..!!")
		for JNODE in HDFS_JOURNAL_HOSTS:
			jnnode+=1
			jndetails = { }
			jndetails['jnHostId']= cm_utils.getHostId(CM_HOST,JNODE)
			jndetails['jnName']= "{0}-jn".format(hdfs_servicename)+str(jnnode)
			jndetails['jnEditsDir']= utils.getXmlValue("hdfs", "hdfs-journal-config","dfs_journalnode_edits_dir") 
			jns.append(jndetails)
			
			utils.run_command("mkdir -p "+jndetails['jnEditsDir'])
			utils.run_command("chown hdfs:hdfs "+jndetails['jnEditsDir'])
		
		standbyId = cm_utils.getHostId(CM_HOST,standbynamenode)
		clustername = utils.getXmlValue("hdfs","hdfs-namenode-config","dfs_federation_namenode_nameservice")
		if cm_utils.getServiceStatus(CLUSTER_NAME,"ZOOKEEPER") == 1:
			logger.error("Failed to enable HA, ZOOKEEPER not in Running..!!")
			raise Exception("Failed to enable HA,ZOOKEEPER not in Running..!!")
			
		cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
		cmd = hdfs_service.enable_nn_ha("{0}-nn".format(hdfs_servicename),standbyId,clustername,jns,zk_service_name="ZOOKEEPER")
		while cmd.success is None:
			time.sleep(5)
			cmd = cmd.fetch()
		
		if cmd.success == True:
			logger.info("Successfully Enabled HA for HDFS..!!")
		else:
			logger.error("Failed to enable HA for HDFS..!!")
			raise Exception("Failed to enable HA for HDFS..!!")
	except Exception, e:
		logger.error("Exception in enable HA for HDFS")
		logger.exception(e)
		raise Exception(e)