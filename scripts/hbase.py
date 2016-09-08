import utils, cm_utils, ctologger
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiServiceSetupInfo

logger = ctologger.getCdhLogger()

try:
	### HBase ###
	HBASE_SERVICE_NAME = utils.getXmlRole("hbase")

	CM_HOST = utils.getXmlValue("manager", "host")
	CLUSTER_NAME = utils.getXmlValue("distribution", "cluster_name")
except Exception, e:
	logger.error("Error in parsing platform template xml!!! Exception:", str(e))
	logger.exception(e)
	raise Exception(e)
   
# Deploys HBase - HMaster, RSes
def deploy_hbase(cluster):
	try:
		HBASE_HM_HOSTS = utils.getXmlHostlist("hbase", "Master")
		HBASE_RS_HOSTS = utils.getXmlHostlist("hbase", "Slave")
		first_run=True
		
		if len(HBASE_HM_HOSTS) > 0:
			logger.info('Deploying Hbase....')
			hbase_service=cm_utils.getServiceObject(HBASE_SERVICE_NAME)
			if hbase_service is not None:
				first_run=False
				logger.info("HBase service already exists.")
			else:
				hbase_service = createServices(cluster)
				
			createMaster(hbase_service,HBASE_HM_HOSTS)
			createRegionServer(hbase_service,HBASE_RS_HOSTS)
			createGateway(hbase_service)
						
			if first_run==True:
				hbase_service.create_hbase_root()
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
				logger.info("Deployed HBase service " + HBASE_SERVICE_NAME + " using HMaster on " + str(HBASE_HM_HOSTS) + " and RegionServers on " + str(HBASE_RS_HOSTS))
			
			cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
			cm_utils.startService(cluster,HBASE_SERVICE_NAME)
				
			return hbase_service
	except Exception, e:
		logger.error("Error in deploy HBase!!! Exception:", str(e))
		logger.exception(e)
		raise Exception(e)

def createServices(cluster):
	try:
		HBASE_SERVICE_CONFIG = utils.getXmlConf("hbase","hbase-service-config")
		HBASE_HM_CONFIG = utils.getXmlConf("hbase","hbase-master-config")
		HBASE_RS_CONFIG = utils.getXmlConf("hbase","hbase-regionserver-config")
		
		cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
		hbase_service = cluster.create_service(HBASE_SERVICE_NAME, HBASE_SERVICE_NAME)
		hbase_service.update_config(HBASE_SERVICE_CONFIG)
		
		cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
		hm = hbase_service.get_role_config_group("{0}-MASTER-BASE".format(HBASE_SERVICE_NAME))
		hm.update_config(HBASE_HM_CONFIG)
		
		cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
		rs = hbase_service.get_role_config_group("{0}-REGIONSERVER-BASE".format(HBASE_SERVICE_NAME))
		rs.update_config(HBASE_RS_CONFIG)
		logger.debug("Created HBase service " + HBASE_SERVICE_NAME)
		
		return hbase_service
	except Exception, e:
		logger.error("Failed to creating HBase services. Exception : " + str(e))
		logger.exception(e)
		raise Exception(e)

		
def createMaster(hbase_service,hbase_hm_hosts):
	try:
		HBASE_HM_DEPLOY_HOSTS = cm_utils.getNewHosts(CM_HOST,CLUSTER_NAME,"hbase", "Master")
		hm_id = len(hbase_hm_hosts)-len(HBASE_HM_DEPLOY_HOSTS)
		for host in HBASE_HM_DEPLOY_HOSTS:
			hm_id += 1
			cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
			hbase_service.create_role("{0}-hm-".format(HBASE_SERVICE_NAME)+ str(hm_id), "MASTER", host)
			logger.debug("Created HBase master role on " +host +" with the name "+ str(HBASE_SERVICE_NAME)+"-hm-"+str(hm_id))
	except Exception, e:
		logger.error("Failed to creating HBase Mater role. Exception : " + str(e))
		logger.exception(e)
		raise Exception(e)

		
def createRegionServer(hbase_service,hbase_rs_hosts):
	try:
		HBASE_RS_DEPLOY_HOSTS = cm_utils.getNewHosts(CM_HOST,CLUSTER_NAME,"hbase", "Slave")
		regionserver = len(hbase_rs_hosts)-len(HBASE_RS_DEPLOY_HOSTS)
		for host in HBASE_RS_DEPLOY_HOSTS:
			regionserver += 1
			cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
			hbase_service.create_role("{0}-rs-".format(HBASE_SERVICE_NAME) + str(regionserver), "REGIONSERVER", host)
			logger.debug("Created HBase regionserver role on " +host +" with the name "+ str(HBASE_SERVICE_NAME)+"-rs-"+str(regionserver))
	except Exception, e:
		logger.error("Failed to creating HBase regionServer role. Exception : " + str(e))
		logger.exception(e)
		raise Exception(e)

def createGateway(hbase_service):
	try:
		HBASE_GW_HOSTS = utils.getXmlHostlist("hbase", "gateway")
		if len(HBASE_GW_HOSTS) > 0:
			HBASE_GW_CONFIG=utils.getXmlConf("hbase","hbase-gateway-config")
			HBASE_GW_DEPLOY_HOSTS = cm_utils.getNewHosts(CM_HOST,CLUSTER_NAME,"hbase", "gateway")
			gateway = len(HBASE_GW_HOSTS)-len(HBASE_GW_DEPLOY_HOSTS)
			gw = hbase_service.get_role_config_group("{0}-GATEWAY-BASE".format(HBASE_SERVICE_NAME))
			gw.update_config(HBASE_GW_CONFIG)
		   
			cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
			logger.info("adding HBASE gateways")
			for host in HBASE_GW_DEPLOY_HOSTS:
				gateway += 1
				hbase_service.create_role("{0}-gw-".format(HBASE_SERVICE_NAME) + str(gateway), "GATEWAY", host)
				logger.debug("Created HBase gateway role on " +host +" with the name "+ str(HBASE_SERVICE_NAME)+"-gw-"+str(gateway))
	except Exception, e:
		logger.error("Failed to creating HBase gateway role. Exception : " + str(e))
		logger.exception(e)
		raise Exception(e)