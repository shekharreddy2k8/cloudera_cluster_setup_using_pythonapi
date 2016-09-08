import socket, sys, time, csv, pprint, urllib2, os, utils, cm_utils, ctologger
from subprocess import Popen, PIPE, STDOUT
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiServiceSetupInfo

logger = ctologger.getCdhLogger()

### ZooKeeper ###
# ZK quorum will be the first three hosts 






# Deploys and initializes ZooKeeper
def deploy_zookeeper(cluster):
	first_run=True
	try:
		ZOOKEEPER_HOSTS = utils.getXmlHostlist("zookeeper", "hosts")
		ZOOKEEPER_SERVICE_NAME = utils.getXmlRole("zookeeper")
		ZOOKEEPER_SERVICE_CONFIG = utils.getXmlConf("zookeeper","zookeeper-service-config")
		ZOOKEEPER_ROLE_CONFIG = utils.getXmlConf("zookeeper","zookeeper-server-config")
		CM_HOST = utils.getXmlValue("manager", "host")
		CLUSTER_NAME = utils.getXmlValue("distribution", "cluster_name")
		cm_server_host=utils.getXmlValue("manager","host")
		if len(ZOOKEEPER_HOSTS) > 0:
			logger.info('Inside deploy_zookeeper.')
			zk = None
			for s in cluster.get_all_services():
			#for s in cm_utils.getServiceObject(ZOOKEEPER_SERVICE_NAME):
				if s.type == ZOOKEEPER_SERVICE_NAME:
					zk = s
			if zk is not None:
				logger.info("Zookeeper service already exists")
				first_run=False
			else:
				cm_utils.waitForRunningCommand(cm_server_host,CLUSTER_NAME)
				zk = cluster.create_service(ZOOKEEPER_SERVICE_NAME, "ZOOKEEPER")
				cm_utils.waitForRunningCommand(cm_server_host,CLUSTER_NAME)
				zk.update_config(ZOOKEEPER_SERVICE_CONFIG)
				
			
			ZOOKEEPER_DEPLOY_HOSTS = cm_utils.getNewHosts(CM_HOST,CLUSTER_NAME,"zookeeper", "hosts")
			zk_id=len(ZOOKEEPER_HOSTS)-len(ZOOKEEPER_DEPLOY_HOSTS)
			for zk_host in ZOOKEEPER_DEPLOY_HOSTS:
				zk_id += 1
				ZOOKEEPER_ROLE_CONFIG['serverId'] = zk_id
				cm_utils.waitForRunningCommand(cm_server_host,CLUSTER_NAME)
				role = zk.create_role(ZOOKEEPER_SERVICE_NAME + "-" + str(zk_id), "SERVER", zk_host)
			
			#wait for roles to get created.
			cm_utils.waitForRunningCommand(cm_server_host,CLUSTER_NAME)
			if first_run==True:
				cmd=zk.init_zookeeper()
				while cmd.success==None:
					cm_utils.waitForRunningCommand(cm_server_host,CLUSTER_NAME)
					cmd=cmd.fetch()
			
			cm_utils.startService(cluster,ZOOKEEPER_SERVICE_NAME)
			
			logger.info("Deployed ZooKeeper " + ZOOKEEPER_SERVICE_NAME + " to run on: " + str(ZOOKEEPER_DEPLOY_HOSTS))
			return zk
	except Exception, e:
		logger.error("Error in deploy Zookeeper. Exception:" + str(e))
		logger.exception(e)
		raise Exception(e)
