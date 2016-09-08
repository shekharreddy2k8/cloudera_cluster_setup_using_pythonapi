import cm_utils, ctologger, utils

logger = ctologger.getCdhLogger()

def check_services():
	try:
		logger.info("POST CLUSTER SETUP VALIDATION FOR SERVICES")
		services=['ZOOKEEPER','HDFS','YARN']
	
		CLUSTER_NAME = utils.getXmlValue("distribution", "cluster_name")
		CM_SERVER_HOST=utils.getXmlValue("manager","host")

		cm_utils.waitForRunningCommand(CM_SERVER_HOST,CLUSTER_NAME)
	
		for service in services:
			status = cm_utils.getServiceStatus(CLUSTER_NAME,service)
			if(status == 0):
				logger.info(service + " service is started")
			else:
				logger.error(service + " service is not started") 
				raise Exception()
	except Exception,e:
		logger.error("Error in postClusterSetupValidation "+ str(e))
		logger.exception(e)
		raise Exception(e)


if __name__ == "__main__":
    check_services()
