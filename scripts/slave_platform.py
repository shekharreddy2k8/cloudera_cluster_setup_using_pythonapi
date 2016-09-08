import utils, re, os, logging, ctologger

os.system("mkdir -p /opt/cto/log/")
## loggings ##
logger = ctologger.getCdhLogger()

logger.info("*******INSTALLATION OF SLAVE PLATFORM BEGIN******")

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

utils.run_os_command("cp -r cloudera-manager.repo /etc/yum.repos.d/; yum clean all")
	
# Turn off firewall
logger.info("Turning off firewall on the server")
utils.run_os_command("service iptables stop")
utils.run_os_command("service ip6tables stop")

# Turn off SELINUX
logger.info("Turning off selinux on the server")
utils.run_os_command("echo 0 | tee /selinux/enforce > /dev/null")

# Set up NTP
logger.info("Enabling NTP on the server")
utils.run_os_command("yum -y install ntp")
utils.run_os_command("chkconfig ntpd on")

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
			
utils.run_command("service ntpd start")

# Set up python
logger.info("Installing Python-Pip and CM API  on the server")
command = "yum -y install python-pip python-argparse"
utils.run_command(command)
command = "wget "+repo_url+"/cm_api-11.0.0.tar.gz -P /opt/cto/tmp"
utils.run_command(command)
command = "pip install /opt/cto/tmp/cm_api-11.0.0.tar.gz"
utils.run_command(command)
command = "rm -rf  /opt/cto/tmp/"
utils.run_command(command)

# For master
command="yum -y install cloudera-manager-agent cloudera-manager-daemons"
utils.run_os_command(command)

with open("/etc/cloudera-scm-agent/config.ini", "r") as sources:
  lines = sources.readlines()
  
with open("/etc/cloudera-scm-agent/config.ini", "w") as sources:
  for line in lines:
	sources.write(re.sub(r"server_host=.*", "server_host="+cm_server_host, line))
	
logger.info("Starting Cloudera Agent .....!!")
command="service cloudera-scm-agent start"
utils.run_command(command)
