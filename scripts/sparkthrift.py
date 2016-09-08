import ctologger,fieldsUtil,cm_utils,clustersetup,enable_ha,time


logger = ctologger.getCdhLogger()

# Deploys spark 
def deploy_sparkthrift(cluster):

	logger.info('Inside deploy_sparkthrift.')
	yarn_service=None

	CM_HOST = fieldsUtil.getCMHost()
	CLUSTER_NAME = fieldsUtil.getClusterName()

	### YARN ###
	YARN_SERVICE_NAME = fieldsUtil.getYarnServiceName()

	### SPARK ###
	SPARK_SERVICE_NAME = fieldsUtil.getSparkServiceName()

	### SPARK THRIFT###
	SPARK_THRIFT_SERVICE_NAME = fieldsUtil.getSparkThriftServiceName()
	SPARK_THRIFT_HOSTS = fieldsUtil.getSparkthriftHosts()
	SPARK_THRIFT_SERVICE_CONFIG = fieldsUtil.getSparkThriftConfig()


	try:
		if len(SPARK_THRIFT_HOSTS) > 0:
			spark_thrift_service = None
			
			spark_service=cm_utils.getServiceObject(SPARK_SERVICE_NAME)       
			if spark_service is None:
				logger.error('SPARK not installed....')
				raise Exception('SPARK not installed....')
			
			spark_thrift_service=cm_utils.getServiceObject(SPARK_THRIFT_SERVICE_NAME)
			if spark_thrift_service is not None:
				logger.info(SPARK_THRIFT_SERVICE_NAME+" service already exists")
			else:
				spark_thrift_service = cluster.create_service(SPARK_THRIFT_SERVICE_NAME, SPARK_THRIFT_SERVICE_NAME)
				spark_thrift_service.update_config(SPARK_THRIFT_SERVICE_CONFIG)
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)

			SPARK_HS_DEPLOY_HOSTS = cm_utils.getNewHosts(CM_HOST,CLUSTER_NAME,"sparkthrift", "hosts")

			historyserver = len(SPARK_THRIFT_HOSTS)-len(SPARK_HS_DEPLOY_HOSTS)

			cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
			for host in SPARK_HS_DEPLOY_HOSTS:
				historyserver += 1
				#command = "ssh "+ host + " " + "yum -y install NSN-CTO-thirdparty-libs"
				#utils.run_command(command)
				spark_thrift_service.create_role("{0}".format(SPARK_THRIFT_SERVICE_NAME) + str(historyserver), "SPARK_THRIFT", host)

		 	if len(SPARK_THRIFT_HOSTS) > 1 and len(SPARK_HS_DEPLOY_HOSTS) > 0:
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
				enable_ha.enableHA(SPARK_HS_DEPLOY_HOSTS,"sparkthrift","thrift","thrift_proxy")

			time.sleep(10)
			cm_utils.startService(cluster,SPARK_THRIFT_SERVICE_NAME)
				
			logger.info("Deployed Spark service " +SPARK_THRIFT_SERVICE_NAME + " using History server on " + str(SPARK_THRIFT_HOSTS))
			return spark_thrift_service
	except Exception, e:
		logger.error("Error in deploy SPARK THRIFT. Execption:" + str(e))
		raise Exception(e)

if __name__ == "__main__":
	CLUSTER  = clustersetup.init_cluster()
	deploy_sparkthrift(CLUSTER)		
