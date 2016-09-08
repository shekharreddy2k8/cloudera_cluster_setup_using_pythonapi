import os, utils, cm_utils, logging, ctologger
from cm_api.api_client import ApiResource
from collections import namedtuple
from cm_api.api_client import ApiResource
from cm_api.api_client import API_CURRENT_VERSION
from cm_api.endpoints.types import ApiCommand
from threading import Thread
from urlparse import urlparse
logger = ctologger.getCdhLogger()
# This is the host that the Cloudera Manager server is running on
CM_HOST = utils.getXmlValue("manager", "host")

# CM admin account info
ADMIN_USER = utils.getXmlValue("manager", "cm_user")
ADMIN_PASS = utils.getXmlValue("manager", "cm_pass")
### CM Definition ###
CM_CONFIG = utils.getXmlConf("manager","manager-config")
CLUSTER_NAME = utils.getXmlValue("distribution", "cluster_name")

### KDC Definition ###
KDC_HOST = utils.getXmlValue("kerberos", "KDC_HOST")
KDC_STATUS = utils.getXmlValue("kerberos", "enable")
KDC_REALM = utils.getXmlValue("kerberos", "SECURITY_REALM")
KDC_ADMIN_USER = utils.getXmlValue("kerberos", "admin_username")
KDC_ADMIN_PWD = utils.getXmlValue("kerberos", "admin_password")

#Wait for Command to complete
class CommandWait(Thread):
    def __init__(self, command):
        Thread.__init__(self)
        self.command = command

    def run(self):
        self.command.wait()

#Check if Principals had already created for Kerberos
def verify_cloudera_manager_has_kerberos_principal(cloudera_manager):
        try:
                cm_configs = cloudera_manager.get_config()

                # If the KDC host and security realm are set, this is a good indicator that the Kerberos
                # adminstrative principal has been imported.
                if 'KDC_HOST' in cm_configs and 'SECURITY_REALM' in cm_configs:
                        return True

                return False
        except Exception, e:
                logger.error("Error in veryfying Cloudera Manager Kerberos Principal. Exception:" + str(e))
                logger.exception(e)
                raise Exception(e)

#Check whether KDC host is accessible
def check_ping():
        try:
                response = os.system("ping -c 1 " +KDC_HOST)
                # and then check the response...
                if response == 0:
                        pingstatus = "Network Active"
                else:
                        pingstatus = "Network Error"

                return pingstatus
        except Exception, e:
                logger.error("Error in veryfying KDC HOST. Exception:" + str(e))
                logger.exception(e)
                raise Exception(e)




#Configure all services for Kerberos Setup
def enable_configure_services(cluster,logger):

    services = cluster.get_all_services()

    for service in services:
        service_type = service.type
        if service_type == 'HDFS':
            logger.info("Configuring HDFS for Kerberos.")
			
	    try:
	        service.update_config(
		    {'hadoop_security_authentication': 'kerberos',
		    'hadoop_security_authorization': 'true'}
		)
	    except Exception, e:
		logger.error("Error in Configuring Kerberos for HDFS. Exception:" + str(e))
		logger.exception(e)
		raise Exception(e)
				
        role_cfgs = service.get_all_role_config_groups()

        for role_cfg in role_cfgs:
            if role_cfg.roleType == 'DATANODE':
		try:
			role_cfg.update_config(
				{'dfs_datanode_port': '1004',
				'dfs_datanode_http_port': '1006',
				'dfs_datanode_data_dir_perm': '700'}
			)
		except Exception, e:
			logger.error("Error in in Configuring HDFS for Zookeeper. Exception:" + str(e))
			logger.exception(e)
			raise Exception(e)
						
						
            elif service_type == 'HBASE':
            		logger.info("Configuring HBase for Kerberos.")
			try:
            		    service.update_config(
                		    {'hbase_security_authentication': 'kerberos',
                 		    'hbase_security_authorization': 'true'}
            		    )
			except Exception, e:
			    logger.error("Error in veryfying Kerberos HOST. Exception:" + str(e))
			    logger.exception(e)
			    raise Exception(e)
            elif service_type == 'ZOOKEEPER':
			logger.info("Configuring ZooKeeper for Kerberos.")
			try:
			    service.update_config(
			        {'enableSecurity': 'true'}
			    )
			except Exception, e:
			    logger.error("Error in Configuring Kerberos for Zookeeper. Exception:" + str(e))
			    logger.exception(e)
			    raise Exception(e)	
			role_cfgs = service.get_all_role_config_groups()
			
			for role_cfg in role_cfgs:
				if role_cfg.roleType == 'SERVER':
					try:
					    role_cfg.update_config(
						    {'zookeeper_config_safety_valve': 'jaasLoginRenew=3600000',
						    'zk_server_java_opts': '-Djava.security.auth.login.config=jaas.conf'}
					    )
					except Exception, e:
					    logger.error("Error in Configuring Kerberos for Zookeeper. Exception:" + str(e))
					    logger.exception(e)
					    raise Exception(e)

#Disable Kerberos
def disable_configure_services(cluster,logger):

    services = cluster.get_all_services()

    for service in services:
        service_type = service.type
        if service_type == 'HDFS':
            logger.info("Disabling Kerberos Configuring for HDFS.")
            service.update_config(
                {'hadoop_security_authentication': 'simple',
                 'hadoop_security_authorization': 'false'}
            )

            role_cfgs = service.get_all_role_config_groups()

            for role_cfg in role_cfgs:
                if role_cfg.roleType == 'DATANODE':
                    role_cfg.update_config(
                        {'dfs_datanode_port': '50010',
                         'dfs_datanode_http_port': '50075',
                         'dfs_datanode_data_dir_perm': '755'}
                    )
        elif service_type == 'HBASE':
            logger.info("Disabling Kerberos Configuring for HBase")
            service.update_config(
                {'hbase_security_authentication': 'simple',
                 'hbase_security_authorization': 'false'}
            )
        elif service_type == 'ZOOKEEPER':
			logger.info("Disabling Kerberos Configuring for ZooKeeper.")
			service.update_config(
				{'enableSecurity': 'false'}
			)

			role_cfgs = service.get_all_role_config_groups()

			for role_cfg in role_cfgs:
				if role_cfg.roleType == 'SERVER':
					role_cfg.update_config(
						{'zookeeper_config_safety_valve': '',
						'zk_server_java_opts': '-Dzookeeper.skipACL=yes'}
					)

#Funtion to generate Credentials			
def wait_for_generate_credentials(cloudera_manager,logger):
    try:
        generate_commands = None
        num_tries = 3

        for i in range(0, num_tries):
            generate_commands = find_command_by_name(cloudera_manager, 'GenerateCredentials')

            # If the list is full
            if generate_commands:
                break

            # Couldn't find the command, so sleep 5 seconds and try again
            time.sleep(5)

        # It's possible that multiple GenerateCredentials commands are generated during
        # service configuration. We should wait for all of them.
        if generate_commands:
            for generate_command in generate_commands:
                wait_for_command('Waiting for Generate Credentials', generate_command,logger)
    except Exception, e:
        logger.error("Error in Generating Credentials. Exception:" + str(e))
        logger.exception(e)
        raise Exception(e)


def find_command_by_name(cloudera_manager, name):
    commands = cloudera_manager.get_commands('full')
    found_commands = [command for command in commands if command.name == name]

    return found_commands


def wait_for_command(msg, command,logger):
    logger.info(msg)

    cmd_wait = CommandWait(command)
    cmd_wait.start()

    while cmd_wait.is_alive():
        cmd_wait.join(5.0)

    logger.info(" Done.\n")

#Configuring JAAS Configuration
def create_zookeeper_jaas(hostname):
    try:
        jaas_conf = open("/etc/zookeeper/conf/jaas.conf","w")
	jaas_conf.writelines(["Server {\n","  com.sun.security.auth.module.Krb5LoginModule required\n","  useKeyTab=true\n","  keyTab=\"zookeeper.keytab\"\n","  storeKey=true\n", "  useTicketCache=false\n", "  principal=\"zookeeper/"+hostname+"@"+KDC_REALM+"\";\n","};\n"])
	jaas_conf.close()
    except Exception, e:
        logger.error("Error in Configuring Zookeeper JAAS. Exception:" + str(e))
        logger.exception(e)
        raise Exception(e)

#Configuring JAAS Configuration for HBASE
def create_hbase_jaas(hostname):
	    try:
	        jaas_conf = open("/etc/hbase/conf/zk-jaas.conf","w")
	        jaas_conf.writelines(["Client {\n","  com.sun.security.auth.module.Krb5LoginModule required\n","  useKeyTab=true\n","  keyTab=\"hbase.keytab\"\n","  storeKey=true\n", "  useTicketCache=false\n", "  principal=\"hbase/"+hostname+"@"+KDC_REALM+"\";\n","};\n"])
	        jaas_conf.close()
	    except Exception, e:
	        logger.error("Error in Configuring HBASE JAAS. Exception:" + str(e))
	        logger.exception(e)
	        raise Exception(e)

#Creating Cloudera Prinicipal
def creating_cloudera_principal():
    try:
        os.system("ssh "+KDC_HOST+" \"echo \"addprinc -randkey cloudera-scm/admin@NOKIA.COM\" | /usr/sbin/kadmin.local> /dev/null\"")
	os.system("ssh "+KDC_HOST+" \"echo \"xst -k /root/cmf.keytab cloudera-scm/admin@NOKIA.COM\" | /usr/sbin/kadmin.local> /dev/null\"")
    except Exception, e:
        logger.error("Error in Creating Cloudera Principal. Exception:" + str(e))
        logger.exception(e)
        raise Exception(e)
	
#Update KDC Configuration by copying the file
def update_kdc_config(cluster,api,logger):
	logger.info("Copying krb5 file to CM host")
	os.system("scp root@"+KDC_HOST+":/etc/krb5.conf /etc/")
	os.system("chown cloudera-scm:cloudera-scm /etc/krb5.conf")	
	os.system("scp root@"+KDC_HOST+":/var/kerberos/krb5kdc/kdc.conf /var/kerberos/krb5kdc/")
	os.system("chown cloudera-scm:cloudera-scm /var/kerberos/krb5kdc/kdc.conf")
	logger.info("getting Keytab file from KDC")

	os.system("scp root@"+KDC_HOST+":/root/cmf.keytab /etc/cloudera-scm-server/")
	os.system("chown cloudera-scm:cloudera-scm /etc/cloudera-scm-server/cmf.keytab")
	if (os.system("/usr/bin/kadmin -k -p cloudera-scm/admin@"+KDC_REALM+" -t /etc/cloudera-scm-server/cmf.keytab -q exit") == 0):
	    os.system("echo cloudera-scm/admin@"+KDC_REALM+" > /etc/cloudera-scm-server/cmf.principal")
	    os.system("chown cloudera-scm:cloudera-scm /etc/cloudera-scm-server/cmf.principal")
	    logger.info("Making Entries for services in ACL")
            with open("/var/kerberos/krb5kdc/kadm5.acl", "w") as f:
	        f.write("*/admin@"+KDC_REALM+" *\n")
	        f.write("cloudera-scm/admin@"+KDC_REALM+" *\n")
	        f.write("cloudera-scm@"+KDC_REALM+" * hbase/*@"+KDC_REALM+"\n")
                f.write("cloudera-scm@"+KDC_REALM+" * hdfs/*@"+KDC_REALM+"\n")
	        f.write("cloudera-scm@"+KDC_REALM+" * hive/*@"+KDC_REALM+"\n")
	        f.write("cloudera-scm@"+KDC_REALM+" * HTTP/*@"+KDC_REALM+"\n")
	        f.write("cloudera-scm@"+KDC_REALM+" * hue/*@"+KDC_REALM+"\n")
                f.write("cloudera-scm@"+KDC_REALM+" * mapred/*@"+KDC_REALM+"\n")
	        f.write("cloudera-scm@"+KDC_REALM+" * yarn/*@"+KDC_REALM+"\n")
	        f.write("cloudera-scm@"+KDC_REALM+" * spark/*@"+KDC_REALM+"\n")
                f.write("cloudera-scm@"+KDC_REALM+" * zookeeper/*@"+KDC_REALM+"\n")
				
                cm_config = api.get_cloudera_manager().update_config({'SECURITY_REALM' : KDC_REALM})
	        cm_config = api.get_cloudera_manager().update_config({'KDC_HOST' : KDC_HOST})
	else:
	    logger.error("Failed to authenticate cloudera cmf.keytab...")
	    #raise Exception("Authentication Failed")
		
	

#Copying file to all Nodes in the cluster	
def upload_kdc_config_file_to_allnode():
	hosts = utils.getAllHostsXml()
	for host in hosts:
		if host != CM_HOST:
			os.system("scp /etc/krb5.conf root@"+host+":/etc/")
			os.system("ssh "+host+" chown cloudera-scm:cloudera-scm /etc/krb5.conf")
			
			os.system("scp /var/kerberos/krb5kdc/kdc.conf root@"+host+":/var/kerberos/krb5kdc/")
			os.system("ssh "+host+" chown cloudera-scm:cloudera-scm /var/kerberos/krb5kdc/kdc.conf")
			
			os.system("scp /var/kerberos/krb5kdc/kadm5.acl root@"+host+":/var/kerberos/krb5kdc/")
			os.system("ssh "+host+" chown cloudera-scm:cloudera-scm /var/kerberos/krb5kdc/kadm5.acl")
		
def main():
    try:
	
   		
		cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
		if(check_ping()=="Network Error"):
			logger.error("Error in deploy Kerberos.")
			raise Exception("Error in deploy Kerberos.")
		
   		else:	
	#if (check_ping()=="Network Error"):
	#	ctologger.createCDHLogger() 
	#	logger = logging.getLogger("CDHLOGGER")
	#	logger.error("Check /etc/hosts for "+KDC_HOST)
		
	#else:
			
			#logger = logging.getLogger("CDHLOGGER")
			
			logger.info('Inside Kerberos Main')
			api = ApiResource(CM_HOST, username=ADMIN_USER, password=ADMIN_PASS)
			cluster = api.get_cluster(CLUSTER_NAME)
	
			if KDC_STATUS.lower() == "yes":
				logger.info("Enabling Kerberos")
				ZOOKEEPER_HOSTS = utils.getXmlValue("zookeeper", "hosts").split(',')
				create_zookeeper_jaas(ZOOKEEPER_HOSTS[0])
				HBASE_HM_HOSTS = utils.getXmlValue("hbase", "Master").split(',')
				create_hbase_jaas(HBASE_HM_HOSTS[0])
				creating_cloudera_principal()
				cm_utils.waitForRunningCommand(CM_HOST,CLUSTER_NAME)
				update_kdc_config(cluster,api,logger)
				upload_kdc_config_file_to_allnode()
				cloudera_manager = api.get_cloudera_manager()
				mgmt_service = cloudera_manager.get_service()

				if verify_cloudera_manager_has_kerberos_principal(cloudera_manager):
					wait_for_command('Stopping the cluster', cluster.stop(),logger)
					wait_for_command('Stopping MGMT services', mgmt_service.stop(),logger)
					enable_configure_services(cluster,logger)
					logger.info("Configuring Kerberos.....!!")
					cluster.configure_for_kerberos()
					wait_for_generate_credentials(cloudera_manager,logger)
					wait_for_command('Deploying client configs.', cluster.deploy_client_config(),logger)
					##Not doing because we are manually copying the file.....!!!
					##wait_for_command('Deploying cluster client configs', cluster.deploy_cluster_client_config(),logger)
					wait_for_command('Starting MGMT services', mgmt_service.start(),logger)
					wait_for_command('Starting the cluster', cluster.start(),logger)
				else:
					logger.info("Cluster does not have Kerberos admin credentials.  Exiting!")

			else:
				logger.info("Disabling Kerberos")
				cloudera_manager = api.get_cloudera_manager()
				mgmt_service = cloudera_manager.get_service()
				wait_for_command('Stopping the cluster', cluster.stop(),logger)
				wait_for_command('Stopping MGMT services', mgmt_service.stop(),logger)
				disable_configure_services(cluster,logger)
				wait_for_command('Deploying client configs.', cluster.deploy_client_config(),logger)
				wait_for_command('Deploying cluster client configs', cluster.deploy_cluster_client_config(),logger)
				wait_for_command('Starting MGMT services', mgmt_service.start(),logger)
				wait_for_command('Starting the cluster', cluster.start(),logger)
    except Exception, e:
        logger.error("Error in verifying Kerberos HOST. Exception:" + str(e))
        logger.exception(e)
        raise Exception(e)

if __name__ == "__main__":
   main()



