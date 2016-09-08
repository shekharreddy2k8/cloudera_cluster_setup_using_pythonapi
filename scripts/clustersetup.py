import socket, sys, time, csv, pprint, urllib2, os, utils, pdb, MySQLdb, cm_utils, ctologger
import zookeeper, parcels
from subprocess import Popen, PIPE, STDOUT
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiServiceSetupInfo
from cm_api.endpoints.cms import ClouderaManager

logger = ctologger.getCdhLogger()

# This is the host that the Cloudera Manager server is running on
CM_HOST = utils.getXmlValue("manager", "host")

# CM admin account info
ADMIN_USER = utils.getXmlValue("manager", "cm_user")
ADMIN_PASS = utils.getXmlValue("manager", "cm_pass")

### CM Definition ###
CM_CONFIG = utils.getXmlConf("manager","manager-config")

#### Cluster Definition #####
CLUSTER_NAME = utils.getXmlValue("distribution", "cluster_name")
CDH_VERSION = utils.getXmlValue("distribution", "distribution")
CDH_FULL_VERSION = utils.getXmlValue("distribution", "version")

### Management Services ###
# If using the embedded postgresql database, the database passwords can be found in /etc/cloudera-scm-server/db.mgmt.properties.
# The values change every time the cloudera-scm-server-db process is restarted. 

AMON_ROLENAME = utils.getXmlRole("manager","amon-config")
AMON_ROLE_CONFIG = utils.getXmlConf("manager","amon-config")
APUB_ROLENAME = utils.getXmlRole("manager","apub-config")
APUB_ROLE_CONFIG = utils.getXmlConf("manager","apub-config")
ESERV_ROLENAME = utils.getXmlRole("manager","eserv-config")
ESERV_ROLE_CONFIG =  utils.getXmlConf("manager","eserv-config")
HMON_ROLENAME = utils.getXmlRole("manager","hmon-config")
HMON_ROLE_CONFIG = utils.getXmlConf("manager","hmon-config")
SMON_ROLENAME = utils.getXmlRole("manager","smon-config")
SMON_ROLE_CONFIG = utils.getXmlConf("manager","smon-config")
NAV_ROLENAME = utils.getXmlRole("manager","nav-config")
NAV_ROLE_CONFIG = utils.getXmlConf("manager","nav-config")
NAVMS_ROLENAME = utils.getXmlRole("manager","navms-config")
NAVMS_ROLE_CONFIG = utils.getXmlConf("manager","navms-config")
RMAN_ROLENAME = utils.getXmlRole("manager","rman-config")
RMAN_ROLE_CONFIG = utils.getXmlConf("manager","rman-config")

### Deployment/Initialization Functions ###

# Creates the cluster and adds hosts
def init_cluster():
	
	API = ApiResource(CM_HOST, username=ADMIN_USER, password=ADMIN_PASS)
	MANAGER = API.get_cloudera_manager()
	MANAGER.update_config(CM_CONFIG)
	logger.info("Connected to CM host on " + CM_HOST)
	logger.info('Inside init_cluster.')
	
	try:
		cluster = API.get_cluster(CLUSTER_NAME)
		logger.info("Using existing cluster object")
	except Exception as e:
		if "not found" in e.message:
			cluster = API.create_cluster(CLUSTER_NAME,CDH_VERSION,CDH_FULL_VERSION)
			logger.info("Created the new cluster")
    	
	# Add the CM host to the list of hosts to add in the cluster so it can run the management services
	HOSTS = cm_utils.getNonClusterHosts(CM_HOST,CLUSTER_NAME)
	if len(HOSTS) > 0:
		addhost_tocluster(cluster,HOSTS)
		logger.info("Initialized cluster " + CLUSTER_NAME + " which uses CDH version " + CDH_VERSION)

	try:
		MANAGER.get_service()
		logger.info("Management service already existing in cluster. ")
	except Exception as e:
		if "Cannot find" in e.message:
			deploy_management(MANAGER)
			logger.info("Created the new management service")
			# get the CM instance
			#cm = ClouderaManager(API)
			# activate the CM trial license
			#cm.begin_trial()
	return cluster

# Add the CM host to the list of hosts to add in the cluster so it can run the management services
def addhost_tocluster(cluster,cluster_hosts):
	all_hosts = list(cluster_hosts)
	cluster.add_hosts(all_hosts)
	
# Deploys management services. Not all of these are currently turned on because some require a license.
# This function also starts the services.
def deploy_management(manager):
	try: 
		logger.info('Inside deploy_management.')
		mgmt = manager.create_mgmt_service(ApiServiceSetupInfo())

		# create roles. Note that host id may be different from host name (especially in CM 5). Look it it up in /api/v5/hosts
		mgmt.create_role(AMON_ROLENAME + "-1", "ACTIVITYMONITOR", CM_HOST)
		mgmt.create_role(APUB_ROLENAME + "-1", "ALERTPUBLISHER", CM_HOST)
		mgmt.create_role(ESERV_ROLENAME + "-1", "EVENTSERVER", CM_HOST)
		mgmt.create_role(HMON_ROLENAME + "-1", "HOSTMONITOR", CM_HOST)
		mgmt.create_role(SMON_ROLENAME + "-1", "SERVICEMONITOR", CM_HOST)
		#mgmt.create_role(NAV_ROLENAME + "-1", "NAVIGATOR", CM_HOST)
		#mgmt.create_role(NAVMS_ROLENAME + "-1", "NAVIGATORMETADATASERVER", CM_HOST)
		#mgmt.create_role(RMAN_ROLENAME + "-1", "REPORTSMANAGER", CM_HOST)

		# now configure each role   
		for group in mgmt.get_all_role_config_groups():
			if group.roleType == "ACTIVITYMONITOR":
				group.update_config(AMON_ROLE_CONFIG)
			elif group.roleType == "ALERTPUBLISHER":
				group.update_config(APUB_ROLE_CONFIG)
			elif group.roleType == "EVENTSERVER":
				group.update_config(ESERV_ROLE_CONFIG)
			elif group.roleType == "HOSTMONITOR":
				group.update_config(HMON_ROLE_CONFIG)
			elif group.roleType == "SERVICEMONITOR":
				group.update_config(SMON_ROLE_CONFIG)
		#   elif group.roleType == "NAVIGATOR":
		#       group.update_config(NAV_ROLE_CONFIG)
		#   elif group.roleType == "NAVIGATORMETADATASERVER":
		#       group.update_config(NAVMS_ROLE_CONFIG)
		#   elif group.roleType == "REPORTSMANAGER":
		#       group.update_config(RMAN_ROLE_CONFIG
			# now start the management service
		mgmt.start().wait()
		logger.info("Deployed CM management service to run on " + CM_HOST)
		return mgmt
	except Exception, e:
		logger.error("Error in deploy Cloudera Management. Exception:" + str(e))
		logger.exception(e)
		raise Exception(e)