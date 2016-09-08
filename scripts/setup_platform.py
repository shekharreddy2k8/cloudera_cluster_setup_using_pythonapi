import utils, time, socket, sys, re, os, logging, ctologger
import ValidateEnv
import xmlcomparator

def main():
	try:
		## loggings ##
		logger = ctologger.getCdhLogger()
		
		logger.info("*******VALIDATION OF RPMS******")
		check_rpms=ValidateEnv.check_rpm()
		
		logger.info("*******VALIDATION OF CONFIGURATIONS******")
		check_configs=ValidateEnv.check_config()
		
		logger.info("*******VALIDATION OF TEMPLATE XML FILE******")
		status_xmlcomp=xmlcomparator.main()

		logger.info("*******INSTALLATION OF PLATFORM BEGIN******")

		cm_server_host=utils.getXmlValue("manager","host")
		logger.info("Cloudera Manager from XML "+cm_server_host)
		cmdb_root_pwd=utils.getXmlValue("manager","cmdb_root_password")
		ntp_server=utils.getXmlValue("manager","ntpserver")
		logger.info("NTP Server from XML "+ntp_server)
		repo_url = "http://"+cm_server_host+"/cm/"

		#setup the repo on CM
		logger.info("Setting repo files to Cloudera Manager")
		utils.setupRepo()

		# Prep Cloudera repo
		logger.info("creating repo file to be placed on Cloudera Manager")
		repo_file = open("/etc/yum.repos.d/cloudera-manager.repo","w")
		repo_file.writelines(["[cloudera-manager]\n","name=cloudera-manager\n","baseurl="+repo_url+"\n","enabled=1\n","gpgcheck=0\n"])
		repo_file.close()
		
		#if socket.gethostname() == cm_server_host or socket.gethostbyname(socket.gethostname) == cm_server_host:
		#utils.run_command("sudo cp /tmp/cto/cloudera-manager.repo /etc/yum.repos.d/")
			
		# Turn off firewall
		logger.info("Turning off firewall on the server")
		utils.run_command("sudo service iptables stop")
		utils.run_command("sudo service ip6tables stop")

		# Turn off SELINUX
		logger.info("Turning off selinux on the server")
		utils.run_command("echo 0 | sudo tee /selinux/enforce > /dev/null")

		# Set up NTP
		logger.info("Enabling NTP on the server")
		utils.run_command("sudo yum -y install ntp")
		utils.run_command("sudo chkconfig ntpd on")

		#setup NTPServer config
		has_entry = False
		with open("/etc/ntp.conf", "r") as sources:
		  lines = sources.readlines()

		with open("/etc/ntp.conf", "w") as sources:
			for line in lines:
				if "server " in line:
					if re.match(r"^server "+ntp_server,line):
						has_entry = True
						sources.write(line)
					else:
						sources.write(re.sub(r"^server ", "#server ", line))
				else:
					sources.write(line)
		  
			if has_entry is False:
				sources.write("server " + ntp_server+"\n")
			
			sources.close()
			
		utils.run_command("sudo service ntpd start")

		# Set up python
		logger.info("Installing Python-Pip and CM API  on the server")
		command = "sudo yum -y install python-pip python-argparse"
		utils.run_command(command)
		command = "mkdir -p /opt/cto/tmp; wget "+repo_url+"/cm_api-11.0.0.tar.gz -P /opt/cto/tmp/"
		utils.run_command(command)
		command = "sudo pip install /opt/cto/tmp/cm_api-11.0.0.tar.gz"
		utils.run_command(command)

		# Set up MySQL
		logger.info("Installing Cloudera Manager Database!!! Processing.....!!")
		command="sudo yum -y install perl-DBI MariaDB-server"
		utils.run_command(command)
		command="sudo cp -r /opt/cto/conf/my.cnf /etc/my.cnf;"
		utils.run_command(command)

		result = os.system("sudo service mysql status")
		if result != 0:	
			result = os.system("sudo service mysql start")

		logger.info("Installing Database Connector")
		command="wget "+repo_url+"mysql-connector-java-5.1.39.tar.gz -P /opt/cto/tmp"
		utils.run_command(command)
		command="tar -zxvf /opt/cto/tmp/mysql-connector-java-5.1.39.tar.gz -C /opt/cto/tmp"
		utils.run_command(command)
		command="cp /opt/cto/tmp/mysql-connector-java-5.1.39/mysql-connector-java-5.1.39-bin.jar /usr/share/java/"
		utils.run_command(command)
		command="ln -s -f /usr/share/java/mysql-connector-java-5.1.39-bin.jar /usr/share/java/mysql-connector-java.jar"
		utils.run_command(command)
		command="rm -rf /opt/cto/tmp"
		utils.run_command(command)


		logger.info("Setting up Cloudera Manager Database")

		command = "mysql -uroot -p"+cmdb_root_pwd+" -e 'show databases' >> /dev/null 2>&1"
		result = os.system(command)
		if result != 0:	
		  command="sleep 15;mysql -u root -e \"grant all privileges on *.* to root@'localhost' identified by '"+cmdb_root_pwd+"';flush privileges;\""
		  utils.run_os_command(command)

		command="mysql -u root -p"+cmdb_root_pwd+" -e \"grant all privileges on *.* to root@'\""+cm_server_host+"\"' identified by '"+cmdb_root_pwd+"';flush privileges;\""
		utils.run_os_command(command)
		command="mysql -u root --password='"+cmdb_root_pwd+"' -e \"UPDATE mysql.user SET Password=PASSWORD('"+cmdb_root_pwd+"') WHERE User='root';FLUSH PRIVILEGES;\""
		utils.run_os_command(command)
		command="mysql -u root  -p"+cmdb_root_pwd+" -e \"grant all privileges on *.* to 'root'@'%' identified by '"+cmdb_root_pwd+"';flush privileges;\""
		utils.run_os_command(command)

		#setup CM accounts and databases
		cm_amondb_pwd=utils.getXmlValue("manager","amon-config","firehose_database_password")
		command="mysql -u root --password='"+cmdb_root_pwd+"' -e \"create database if not exists amon DEFAULT CHARACTER SET utf8; grant all on amon.* TO 'amon'@'%' IDENTIFIED BY '"+cm_amondb_pwd+"';grant all on amon.* TO 'amon'@'"+ cm_server_host +"' IDENTIFIED BY '"+cm_amondb_pwd+"';\""
		utils.run_os_command(command)
		cm_rmandb_pwd=utils.getXmlValue("manager","rman-config","headlamp_database_password")
		command="mysql -u root --password='"+cmdb_root_pwd+"' -e \"create database if not exists rman DEFAULT CHARACTER SET utf8; grant all on rman.* TO 'rman'@'%' IDENTIFIED BY '"+cm_rmandb_pwd+"';grant all on rman.* TO 'rman'@'"+ cm_server_host +"' IDENTIFIED BY '"+cm_rmandb_pwd+"';\""
		utils.run_os_command(command)
		cm_navdb_pwd=utils.getXmlValue("manager","nav-config","navigator_database_password")
		command="mysql -u root --password='"+cmdb_root_pwd+"' -e \"create database if not exists nav DEFAULT CHARACTER SET utf8; grant all on nav.* TO 'nav'@'%' IDENTIFIED BY '"+cm_navdb_pwd+"';grant all on nav.* TO 'nav'@'"+ cm_server_host +"' IDENTIFIED BY '"+cm_navdb_pwd+"';\""
		utils.run_os_command(command)
		command="mysql -u root --password='"+cmdb_root_pwd+"' -e \"create database if not exists navms DEFAULT CHARACTER SET utf8; grant all on navms.* TO 'navms'@'%' IDENTIFIED BY 'navms_password';grant all on navms.* TO 'navms'@'"+ cm_server_host +"' IDENTIFIED BY 'navms_password';\""
		utils.run_os_command(command)
		# Make sure DNS is set up properly so all nodes can find all other nodes

		# For master
		logger.info("Installing Cloudera Manager!!! Processing.....!!")
		command="sudo yum -y install cloudera-manager-agent cloudera-manager-daemons cloudera-manager-server"
		utils.run_command(command)

		command="mysql -uroot -p"+cmdb_root_pwd+" -s -e 'use scm' >> /dev/null 2>&1"
		result = os.system(command)
		if result != 0:	
			logger.info("Creating Cloudera Manager Schema!!! Processing.....!!")
			command="/usr/share/cmf/schema/scm_prepare_database.sh mysql -u root -p"+cmdb_root_pwd+" -P 3306 scm scm scm"
			utils.run_command(command)
			
		#copy the parcel to the cloudera parcel-repo location
		pack_dir = utils.getXmlValue("distribution","package_path")
		parcel_ver = utils.getXmlValue("distribution","parcel_version")
		if os.path.isfile("/opt/cloudera/parcel-repo/CDH-"+parcel_ver+"-el6.parcel") is False:
			logger.info("Copying Cloudera Parcel file.....!!")
			command="cp "+pack_dir+"/CDH-"+parcel_ver+"-el6.parcel /opt/cloudera/parcel-repo/"
			utils.run_command(command)
			command="cp "+pack_dir+"/CDH-"+parcel_ver+"-el6.parcel.sha /opt/cloudera/parcel-repo/"
			utils.run_command(command)
		
		logger.info("Starting Cloudera Manager .....!!")
		command="sudo service cloudera-scm-server start"
		utils.run_os_command(command)

		with open("/etc/cloudera-scm-agent/config.ini", "r") as sources:
		  lines = sources.readlines()
		  
		with open("/etc/cloudera-scm-agent/config.ini", "w") as sources:
		  for line in lines:
			sources.write(re.sub(r"server_host=.*", "server_host="+cm_server_host, line))
			
		logger.info("Starting Cloudera Agent .....!!")
		command="sudo service cloudera-scm-agent start"
		utils.run_command(command)

		#setup Cloudera Manager Agents on all the Nodes
		utils.setupCMAgents()

		#check for Webservice of Manager to be up
		while 1:
		  result = os.system("wget http://" + cm_server_host +":7180/ -q")
		  if result == 0:
			break
		  logger.info("Waiting for Cloudera Manager UI to come up .....!!")
		  time.sleep(10)
		 
		# Execute script to deploy Cloudera cluster
		if len(utils.getAllHostsXml()) > 0:
			logger.info("Started deploying cloudera cluster....")
			result = os.system("python /opt/cto/scripts/deployCluster.py")
			if result != 0:
				logger.info("Failed deploying cloudera cluster.")
				raise Exception()
			logger.info("Finished deploying cloudera cluster.")
			
			# post cluster validation script
			os.system("python /opt/cto/scripts/postClusterSetupValidation.py")
						
		logger.info("Configuring Backup .....!!")
		os.system("ssh "+cm_server_host+" 'echo \"0      22     *       *       * /usr/bin/python /opt/cto/scripts/dbbackup.py > /opt/cto/log/backup.log 2>&1\" | /usr/bin/crontab'")
		
		
		
		return 0
	except Exception, e:
		logger.error("Exception in setup_platform. Exception is "+ str(e))
		logger.exception(e)
		raise Exception(e)	
		#sys.exit(1)
		
if __name__ == "__main__":
	main() 		
