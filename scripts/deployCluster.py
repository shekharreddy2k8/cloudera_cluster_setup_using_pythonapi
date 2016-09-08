import utils, pdb, cm_utils, ctologger,sparkthrift
import zookeeper, parcels, clustersetup, hdfs, yarn, hbase, hive, postsetup, kerberos_setup,spark,thrift_server_ha

## loggings ##
logger = ctologger.getCdhLogger()
logger.info("Installation Start !!!!!!")

### Set up environment-specific vars ###
CM_HOST = utils.getXmlValue("manager", "host")
CLUSTER_NAME = utils.getXmlValue("distribution", "cluster_name")
KDC_STATUS = utils.getXmlValue("kerberos", "enable")

   
### Main function ###
def main():
	try:
		logger.info('Started deploying cloudera cluster....')
		CLUSTER  = clustersetup.init_cluster()
		parcels.deploy_parcels(CLUSTER)
		zookeeper_service = zookeeper.deploy_zookeeper(CLUSTER)
		hdfs_service = hdfs.deploy_hdfs(CLUSTER)
		yarn_service = yarn.deploy_yarn(CLUSTER)
		hbase_service = hbase.deploy_hbase(CLUSTER)
		hive_service = hive.deploy_hive(CLUSTER)
		spark_service=spark.deploy_spark(CLUSTER)
		sparkthrift.deploy_sparkthrift(CLUSTER)
		
		#thrift_server_ha.deploySparkThrift()
		cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
		CLUSTER.deploy_client_config()
		#postsetup.deploy_client_config(CLUSTER, hdfs_service, yarn_service, hive_service, hbase_service, spark_service)
		logger.info("Finished deploying Cloudera cluster. Go to http://" + CM_HOST + ":7180 to administer the cluster.")
		
		if KDC_STATUS.lower() == "yes":
			logger.info("Starting Kerberos setting")
#			command="sudo python kerberos_setup.py"
#			utils.run_os_command(command)
			kerberos_setup.main()
			logger.info("Finished Kerberos setting")
			
		return 0
	except Exception, e:
		logger.error("Error in deploy cluster!!!" + str(e))
		logger.exception(e)
		raise Exception(e)
		return 1

if __name__ == "__main__":
   main()   
