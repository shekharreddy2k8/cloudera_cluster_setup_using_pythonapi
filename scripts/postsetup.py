import ctologger
from cm_api.api_client import ApiResource

CMD_TIMEOUT = 180
logger = ctologger.getCdhLogger()

def deploy_client_config(cluster, hdfs_service, yarn_service, hive_service, hbase_service, spark_service):
	try:
		# Deploy client configs to all necessary hosts
		cmd = cluster.deploy_client_config()
		if not cmd.wait(CMD_TIMEOUT).success:
			logger.error( "Failed to deploy client configs ")
			
		# Deploy client configs to all necessary hosts
		cmd = hdfs_service.deploy_client_config()
		if not cmd.wait(CMD_TIMEOUT).success:
			logger.error( "Failed to deploy client configs for hdfs_service")
			
		cmd = yarn_service.deploy_client_config()
		if not cmd.wait(CMD_TIMEOUT).success:
			logger.error( "Failed to deploy client configs for yarn_service")

		cmd = hive_service.deploy_client_config()
		if not cmd.wait(CMD_TIMEOUT).success:
			logger.error( "Failed to deploy client configs for hive_service")

		cmd = hbase_service.deploy_client_config()
		if not cmd.wait(CMD_TIMEOUT).success:
			logger.error( "Failed to deploy client configs for hbase_service")
		
		cmd = spark_service.deploy_client_config()
		if not cmd.wait(CMD_TIMEOUT).success:
			logger.error( "Failed to deploy client configs for spark_service")
	except Exception, e:
		logger.error("Failed to deploy client config . Exception : " + str(e))
		logger.exception(e)
		raise Exception(e)
		
def restartCluster(cluster):
	logger.info("About to restart cluster")
	time.sleep(15)
	logger.info("Restart cluster...................")
	cluster.stop().wait()
	cluster.start().wait()
	logger.info("Done restarting cluster")
