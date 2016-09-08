import ctologger,fieldsUtil,cm_utils


logger = ctologger.getCdhLogger()

# Deploys spark 
def deploy_spark(cluster):

	logger.info('Inside deploy_spark.')
	yarn_service=None

	CM_HOST = fieldsUtil.getCMHost()
	CLUSTER_NAME = fieldsUtil.getClusterName()

	### YARN ###
	YARN_SERVICE_NAME = fieldsUtil.getYarnServiceName()

	### SPARK ###
	SPARK_SERVICE_NAME = fieldsUtil.getSparkServiceName()
	SPARK_HOSTS = fieldsUtil.getSparkHosts()
	SPARK_SERVICE_CONFIG = fieldsUtil.getSparkServiceConfig()
	SPARK_HS_CONFIG = fieldsUtil.getSparkHSConfig()

	try:
		if len(SPARK_HOSTS) > 0:
			spark_service = None
			
			yarn_service=cm_utils.getServiceObject(YARN_SERVICE_NAME)       
			if yarn_service is None:
				logger.error('YARN not installed....')
				raise Exception('YARN not installed....')

			if yarn_service.serviceState != "STARTED":
				logger.warn('YARN not started')

			spark_service=cm_utils.getServiceObject(SPARK_SERVICE_NAME)
			if spark_service is not None:
				logger.info(SPARK_SERVICE_NAME+" service already exists")
			else:
				spark_service = cluster.create_service(SPARK_SERVICE_NAME, SPARK_SERVICE_NAME)
				spark_service.update_config(SPARK_SERVICE_CONFIG)
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)

				res = spark_service.service_command_by_name("CreateSparkUserDirCommand")
				while res.success != None:
						sleep(5)
						res = res.fetch()

				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)			        					
				res = spark_service.service_command_by_name("CreateSparkHistoryDirCommand")
				while res.success != None:
						sleep(5)
						res = res.fetch() 

			SPARK_HS_DEPLOY_HOSTS = cm_utils.getNewHosts(CM_HOST,CLUSTER_NAME,"spark", "hosts")

			historyserver = len(SPARK_HOSTS)-len(SPARK_HS_DEPLOY_HOSTS)

			cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
			for host in SPARK_HS_DEPLOY_HOSTS:
				historyserver += 1
				spark_service.create_role("{0}-rs-".format(SPARK_SERVICE_NAME) + str(historyserver), "SPARK_YARN_HISTORY_SERVER", host)

			cm_utils.startService(cluster,SPARK_SERVICE_NAME)
				
			logger.info("Deployed Spark service " +SPARK_SERVICE_NAME + " using History server on " + str(SPARK_HOSTS))
			return spark_service
	except Exception, e:
		logger.error("Error in deploy SPARK. Execption:" + str(e))
		raise Exception(e)
