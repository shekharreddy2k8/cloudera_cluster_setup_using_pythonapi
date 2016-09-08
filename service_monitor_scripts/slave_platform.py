import utils, time, socket, sys, re, os

os.system("mkdir -p /opt/cto/log/")
logger = utils.getLogger("/opt/cto/log/installation.log","a")
logger.info("*******INSTALLATION OF PLATFORM BEGIN******")

cm_server_host=utils.getXmlValue("manager","host")
logger.info("Cloudera Manager from XML "+cm_server_host)
ntp_server=utils.getXmlValue("manager","ntpserver")
logger.info("NTP Server from XML"+ntp_server)
repo_url = "http://"+cm_server_host+"/cm/"

# Prep Cloudera repo
logger.info("creating repo file to be placed on Cloudera Manager")
repo_file = open("cloudera-manager.repo","w")
repo_file.writelines(["[cloudera-manager]\n","name=cloudera-manager\n","baseurl="+repo_url+"\n","enabled=1\n","gpgcheck=0\n"])
repo_file.close()

#setup the repo on CM
logger.info("copying repo files to Cloudera Manager")
utils.run_os_command("sudo yum -y install createrepo httpd")
command = "service httpd start"
utils.run_os_command(command)

#if socket.gethostname() == cm_server_host or socket.gethostbyname(socket.gethostname) == cm_server_host:
utils.run_os_command("sudo cp cloudera-manager.repo /etc/yum.repos.d/")
	
# Turn off firewall
logger.info("Turning off firewall on the server")
utils.run_os_command("sudo service iptables stop")
utils.run_os_command("sudo service ip6tables stop")

# Turn off SELINUX
logger.info("Turning off selinux on the server")
utils.run_os_command("echo 0 | sudo tee /selinux/enforce > /dev/null")

# Set up NTP
logger.info("Enabling NTP on the server")
utils.run_os_command("sudo yum -y install ntp")
utils.run_os_command("sudo chkconfig ntpd on")

#setup NTPServer config
with open("/etc/ntp.conf", "r") as sources:
  lines = sources.readlines()
  
with open("/etc/ntp.conf", "w") as sources:
  for line in lines:
	if "server " in line:
		sources.write(re.sub(r"^server ", "#server ", line))
	else:
		sources.write(line)
  
  sources.write("server " + ntp_server)
	
utils.run_os_command("sudo service ntpd start")

# For master
command="sudo yum -y install cloudera-manager-agent cloudera-manager-daemons"
utils.run_os_command(command)

with open("/etc/cloudera-scm-agent/config.ini", "r") as sources:
  lines = sources.readlines()
  
with open("/etc/cloudera-scm-agent/config.ini", "w") as sources:
  for line in lines:
	sources.write(re.sub(r"server_host=.*", "server_host="+cm_server_host, line))
	
logger.info("Starting Cloudera Agent .....!!")
command="sudo service cloudera-scm-agent start"
utils.run_os_command(command)
