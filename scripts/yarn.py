import utils, cm_utils, ctologger, time
from subprocess import Popen, PIPE, STDOUT
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiServiceSetupInfo	  

logger = ctologger.getCdhLogger()

try:
	### YARN ###
	YARN_SERVICE_NAME = utils.getXmlRole("yarn")
	CM_HOST = utils.getXmlValue("manager", "host")
	CLUSTER_NAME = utils.getXmlValue("distribution", "cluster_name")
except Exception, e:
	logger.error("Error in parsing platform template xml!!! Exception:", str(e))
	logger.exception(e)
	raise Exception(e)

# Deploys YARN - RM, JobHistoryServer, NMs
# This shouldn't be run if MapReduce is deployed.
def deploy_yarn(cluster):
	try:
		YARN_RM_HOSTS = utils.getXmlHostlist("yarn", "Master")
		YARN_NM_HOSTS = utils.getXmlHostlist("yarn", "Slave")
		if len(YARN_RM_HOSTS) > 0:
			logger.info('Deploying Yarn....')
			first_run = True
			yarn_service=cm_utils.getServiceObject(YARN_SERVICE_NAME)
			if yarn_service is not None:
				first_run=False
				logger.info("YARN service already exists.")
			else:
				yarn_service=createServices(cluster)
				createResourceManager(yarn_service,YARN_RM_HOSTS)
				
			createNodeManager(yarn_service,YARN_NM_HOSTS)
			createGateway(yarn_service)
				
			if first_run==True:
				logger.info("Deployed YARN service " + YARN_SERVICE_NAME + " using ResourceManager on " + str(YARN_RM_HOSTS) + ", JobHistoryServer on " + str(YARN_RM_HOSTS[0]) + ", and NodeManagers on " + str(YARN_NM_HOSTS))
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
				post_startup_yarn()
			
			cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
			cm_utils.startService(cluster,YARN_SERVICE_NAME)
			
			enableYarnHA(yarn_service,first_run,YARN_RM_HOSTS)
			return yarn_service
	except Exception, e:
		logger.error("Error in deploy YARN!!! Exception:", str(e))
		logger.exception(e)
		raise Exception(e)

def createServices(cluster):
	try:
		YARN_SERVICE_CONFIG = utils.getXmlConf("yarn","yarn-service-config")
		YARN_RM_CONFIG = utils.getXmlConf("yarn","yarn-rm-config")
		YARN_JHS_CONFIG = utils.getXmlConf("yarn","yarn-jhs-config")
		YARN_NM_CONFIG = utils.getXmlConf("yarn","yarn-nm-config")
		
		cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
		yarn_service = cluster.create_service(YARN_SERVICE_NAME, YARN_SERVICE_NAME)
		yarn_service.update_config(YARN_SERVICE_CONFIG)
		
		cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
		rm = yarn_service.get_role_config_group("{0}-RESOURCEMANAGER-BASE".format(YARN_SERVICE_NAME))
		rm.update_config(YARN_RM_CONFIG)
		
		cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
		jhs = yarn_service.get_role_config_group("{0}-JOBHISTORY-BASE".format(YARN_SERVICE_NAME))
		jhs.update_config(YARN_JHS_CONFIG)
		
		cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
		nm = yarn_service.get_role_config_group("{0}-NODEMANAGER-BASE".format(YARN_SERVICE_NAME))
		nm.update_config(YARN_NM_CONFIG)
		logger.debug("Created Yarn service " + YARN_SERVICE_NAME)
		
		return yarn_service
	except Exception, e:
		logger.error("Failed to creating Yarn services. Exception : " + str(e))
		logger.exception(e)
		raise Exception(e)

def createResourceManager(yarn_service,yarn_rm_hosts):
	try:
		YARN_RM_DEPLOY_HOSTS = cm_utils.getNewHosts(CM_HOST,CLUSTER_NAME,"yarn", "Master")
		rm_id = len(yarn_rm_hosts)-len(YARN_RM_DEPLOY_HOSTS)
		cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
		yarn_service.create_role("{0}-rm-".format(YARN_SERVICE_NAME)+ str(rm_id), "RESOURCEMANAGER", yarn_rm_hosts[0])
		logger.debug("Created Yarn resource manager role on " +yarn_rm_hosts[0] +" with the name of "+ str(YARN_SERVICE_NAME)+"-rm-"+str(rm_id))
		cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
		yarn_service.create_role("{0}-jhs-".format(YARN_SERVICE_NAME)+ str(rm_id), "JOBHISTORY", yarn_rm_hosts[0])
		logger.debug("Created Yarn job history role on " +yarn_rm_hosts[0] +" with the name "+ str(YARN_SERVICE_NAME)+"-jhs-"+str(rm_id))
	except Exception, e:
		logger.error("Failed to creating Yarn Resource Manager role. Exception : " + str(e))
		logger.exception(e)
		raise Exception(e)
		
def createNodeManager(yarn_service,yarn_nm_hosts):
	try:
		YARN_NM_DEPLOY_HOSTS = cm_utils.getNewHosts(CM_HOST,CLUSTER_NAME,"yarn", "Slave")
		nodemanager = len(yarn_nm_hosts)-len(YARN_NM_DEPLOY_HOSTS)
		for host in YARN_NM_DEPLOY_HOSTS:
			nodemanager += 1
			cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
			yarn_service.create_role("{0}-nm-".format(YARN_SERVICE_NAME) + str(nodemanager), "NODEMANAGER", host)
			logger.debug("Created Yarn nodemanager role on " +host +" with the name "+ str(YARN_SERVICE_NAME)+"-nm-"+str(nodemanager))
	except Exception, e:
		logger.error("Failed to creating Yarn Node Manager role. Exception : " + str(e))
		logger.exception(e)
		raise Exception(e)

		
def createGateway(yarn_service):
	try:
		YARN_GW_HOSTS = utils.getXmlHostlist("yarn", "gateway")
		if len(YARN_GW_HOSTS) > 0:
			YARN_GW_CONFIG = utils.getXmlConf("yarn", "yarn-gw-config")
			gw = yarn_service.get_role_config_group("{0}-GATEWAY-BASE".format(YARN_SERVICE_NAME))
			gw.update_config(YARN_GW_CONFIG)
			
			YARN_GW_DEPLOY_HOSTS = cm_utils.getNewHosts(CM_HOST,CLUSTER_NAME,"yarn", "gateway")
			gateway = len(YARN_GW_HOSTS)-len(YARN_GW_DEPLOY_HOSTS)
			for host in YARN_GW_DEPLOY_HOSTS:
				gateway += 1
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
				yarn_service.create_role("{0}-gw-".format(YARN_SERVICE_NAME) + str(gateway), "GATEWAY", host)
				logger.debug("Created Yarn gateway role on " +host +" with the name "+ str(YARN_SERVICE_NAME)+"-gw-"+str(gateway))
	except Exception, e:
		logger.error("Failed to creating Yarn gateway role. Exception : " + str(e))
		logger.exception(e)
		raise Exception(e)
		
def enableYarnHA(yarn_service,first_run,yarn_rm_hosts):
	try:
		ZOOKEEPER_SERVICE_NAME = utils.getXmlRole("zookeeper")
		if first_run == True and len(yarn_rm_hosts) == 2:
			stdrm = cm_utils.getHostId(CM_HOST,yarn_rm_hosts[1])
			cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
			if cm_utils.getServiceStatus(CLUSTER_NAME,ZOOKEEPER_SERVICE_NAME) == 1:
				logger.error("Failed to enable Yarn HA, ZOOKEEPER not in Running..!!!")
				raise Exception("Failed to enable Yarn HA, ZOOKEEPER not in Running..!!")
			logger.info("Enabling yarn resource manager HA !!!")
			yarn_service.enable_rm_ha(stdrm,zk_service_name=ZOOKEEPER_SERVICE_NAME)
			cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
	except Exception, e:
		logger.error("Failed to enable Yarn HA!!!. Exception : " + str(e))
		logger.exception(e)
		raise Exception(e)		
		
	
# Executes steps that need to be done after the final startup once everything is deployed and running.
def post_startup_yarn():
	
	# Noe change permissions on the /user dir so YARN will work
	shell_command = ['sudo -u hdfs hadoop fs -chmod 775 /user']
	user_chmod_output = Popen(shell_command, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True).stdout.read()
