import utils, time, socket, sys, re, os, logging, ctologger, deployCluster, mysqlHA

def main():
	try:
		## loggings ##
		logger = ctologger.getCdhLogger()
                logger.info("*******VALIDATION OF ENVIRONMENT BEGIN******")
                #os.system("python /opt/cto/scripts/validate.py")
		logger.info("*******INSTALLATION OF PLATFORM BEGIN******")

		cm_server_host=utils.getXmlValue("manager","host")
		logger.info("Cloudera Manager from XML "+cm_server_host)
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
		logger.info("Installing Database Client on CM Manager server!!! Processing.....!!")
		try:
			cm_server_host=utils.getXmlValue("manager","host")
			command =  "ssh "+cm_server_host+ " yum install MariaDB-client"
			utils.run_command(command)
			mysqlHA.main()
		except Exception, e:
			logger.error("Exception in MySQL HA Configuration. Exception is "+ str(e))
			logger.exception(e)
			return 1
		'''command="sudo yum -y install perl-DBI MariaDB-server"
		utils.run_command(command)
		command="sudo cp -r /opt/cto/conf/my.cnf /etc/my.cnf;"
		utils.run_command(command)
		
		result = os.system("sudo service mysql status")
		if result != 0:	
			result = os.system("sudo service mysql start")
		'''
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

		'''
		logger.info("Setting up Cloudera Manager Database")

		result = os.system("mysql -uroot -proot123 -e 'show databases' >> /dev/null 2>&1")
		if result != 0:	
		  command="sleep 15;mysql -u root -e \"grant all privileges on *.* to root@'localhost' identified by 'root123';flush privileges;\""
		  utils.run_os_command(command)

		command="mysql -u root -proot123 -e \"grant all privileges on *.* to root@'\""+cm_server_host+"\"' identified by 'root123';flush privileges;\""
		utils.run_os_command(command)
		command="mysql -u root --password='root123' -e \"UPDATE mysql.user SET Password=PASSWORD('root123') WHERE User='root';FLUSH PRIVILEGES;\""
		utils.run_os_command(command)
		command="mysql -u root  -proot123 -e \"grant all privileges on *.* to 'root'@'%' identified by 'root123';flush privileges;\""
		utils.run_os_command(command)
		'''
		'''
		#setup CM accounts and databases
		command="mysql -u root --password='root123' -e \"create database if not exists amon DEFAULT CHARACTER SET utf8; grant all on amon.* TO 'amon'@'%' IDENTIFIED BY 'amon_password';grant all on amon.* TO 'amon'@'"+ cm_server_host +"' IDENTIFIED BY 'amon_password';\""
		utils.run_os_command(command)
		command="mysql -u root --password='root123' -e \"create database if not exists rman DEFAULT CHARACTER SET utf8; grant all on rman.* TO 'rman'@'%' IDENTIFIED BY 'rman_password';grant all on rman.* TO 'rman'@'"+ cm_server_host +"' IDENTIFIED BY 'rman_password';\""
		utils.run_os_command(command)
		command="mysql -u root --password='root123' -e \"create database if not exists nav DEFAULT CHARACTER SET utf8; grant all on nav.* TO 'nav'@'%' IDENTIFIED BY 'nav_password';grant all on nav.* TO 'nav'@'"+ cm_server_host +"' IDENTIFIED BY 'nav_password';\""
		utils.run_os_command(command)
		command="mysql -u root --password='root123' -e \"create database if not exists navms DEFAULT CHARACTER SET utf8; grant all on navms.* TO 'navms'@'%' IDENTIFIED BY 'navms_password';grant all on navms.* TO 'navms'@'"+ cm_server_host +"' IDENTIFIED BY 'navms_password';\""
		utils.run_os_command(command)
		# Make sure DNS is set up properly so all nodes can find all other nodes
		'''
		# For master
		logger.info("Installing Cloudera Manager!!! Processing.....!!")
		command="sudo yum -y install cloudera-manager-agent cloudera-manager-daemons cloudera-manager-server"
		utils.run_command(command)
		sql_host = utils.getXmlValue("mysqldb","hosts").split(',')
		sqlhost = sql_host[0]
		MYSQL_HA_PROXY =  utils.getXmlValue("mysqldb","mysql-vip-config","mysql_load_balancer").split(':')
		mysqlha = MYSQL_HA_PROXY[0]
		result = os.system("ssh " +sqlhost+ " mysql -uroot -proot123 -s -e 'use scm' >> /dev/null 2>&1")
		if result == 0:	
			command = "ssh "+mysqlha+ "\" mysql -uroot -proot123 -e \\\" drop database scm;exit\\\"\""
			utils.run_os_command(command)
		logger.info("Creating Cloudera Manager Schema!!! Processing.....!!")
		command="/usr/share/cmf/schema/scm_prepare_database.sh mysql -u root -proot123 -h "+mysqlha+" -P 3306 scm root root123"	
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
		logger.info("Started deploying cloudera cluster....")
		deployCluster.main()
		
		logger.info("Configuring Backup .....!!")
		os.system("ssh "+cm_server_host+" 'echo \"0      22     *       *       * /usr/bin/python /opt/cto/scripts/dbBackup.py > /opt/cto/log/backup.log 2>&1\" | /usr/bin/crontab'")
		logger.info("Finished deploying cloudera cluster.")
		
		return 0
	except Exception, e:
		logger.error("Exception in setup_platform. Exception is "+ str(e))
		logger.exception(e)
		return 1
		
if __name__ == "__main__":
   main() 		

