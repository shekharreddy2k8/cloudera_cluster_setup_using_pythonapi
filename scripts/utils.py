import os, sys, subprocess as sp
import ctologger, filecmp, time
import xml.etree.ElementTree as ET
from datetime import datetime as dt

logger = ctologger.getCdhLogger()

try:
        tree = ET.parse("/opt/cto/conf/platform_template.xml")
except Exception as e:
        logger.error("Invalid XML.!! Please correct the XML.!!\n" + str(e))
		
		
# Map for service name role name mapping to XML name mappings
roleList = [
	"hdfs-Master",
	"hdfs-Slave",
	"yarn-Master",
	"yarn-Slave",
	"hbase-Master",
	"hbase-Slave",
	"hive-hosts",
	"zookeeper-hosts",
	"yarn-gateway",
	"spark-hosts",
	"sparkthrift-hosts"
	]
	
def run_command(command):
	opf = open("/opt/cto/log/cmdoutput.log", "a")
	opf.write("The command is "+command+"at "+str(dt.now())+"\n\n")
	status = sp.call(command,shell=True,stdout=opf,stderr=opf)
	opf.close()
	if status != 0:
		logger.error("Failed to execute command " + str(command))
		raise Exception("Failed to execute commnd " + str(command))

	return status

def run_os_command(cmd):

	result = os.system(cmd)
	if result != 0:
		logger.error("Failed to execute command" + str(cmd))
		raise Exception("Failed to execute commnd " + str(command))

def getXmlHostlist(basetag, *children):
	root=tree.getroot()
	index=0

	props = root.find(basetag)

	if props == None:
		return []

	for index in range(len(children)-1):
		parent = props.find(children[index])
		props = parent

	if props == None:
		return []

	prop_list = props.findall('property')

	for prop in prop_list:
		if prop.get('name') == children[len(children) - 1]:
			if len(prop.get('value')) > 0:
				return prop.get('value').split(",")
	
	return []

def getXmlValue(basetag, *children):
	root=tree.getroot()
	index=0
  
	props = root.find(basetag)
	
	if props == None:
		return ""

	for index in range(len(children)-1):
		parent = props.find(children[index])
		props = parent

   	if props == None:
		return ""

	prop_list = props.findall('property')

	for prop in prop_list:
		if prop.get('name') == children[len(children) - 1]:
			return prop.get('value')
	return 

def getXmlConf(basetag, *children):
	root=tree.getroot()
	index=0
	config ={ }
	if len(children) == 0:
		proplist = root.find(basetag).findall('property')
	else:
		props = root.find(basetag)
		for index in range(len(children)):
			if props == None:
				return ""
			parent = props.find(children[index])
			props = parent
    
	if props == None:
		return ""
	proplist = props.findall('property')

	for prop in proplist:
		config[prop.get('name')] = prop.get('value')

	return config

def getXmlRole(basetag, *children):
	#  tree=ET.parse('platform_template.xml')
	root=tree.getroot()
	index=0
	config ={ }
	if len(children) == 0:
		if root.find(basetag) != None:
			return root.find(basetag).get('name')
		else:
			return ""
	else:
		props = root.find(basetag)
		for index in range(len(children)):
			if props == None:
				return ""
			parent = props.find(children[index])
			props = parent

	return props.get('name')

def getAllHostsXml():
	nodelist = []
	for key in roleList:
		key = key.split("-")
		nodestr = getXmlValue(key[0],key[1])
		if nodestr != None:
			list = nodestr.split(",")
			for node in list:
				if node not in nodelist and len(node) > 0:
					nodelist.append(node)
  
	return nodelist
	 
	 
def pingHost( host_ip):
    return os.system("ping -c 2 " + host_ip + "> /dev/null")

# Get a handle to the API client
def getHostId( cm_host, host_ip):
	if pingHost(cm_host) != 0:
		logger.error("CM not reachable ", str(cm_host))
		raise Exception("CM not reachable ", str(cm_host))
		return

	resource = ApiResource(cm_host, username="admin", password="admin")
	hostId = ''
	for h in  resource.get_all_hosts():
		if host_ip == h.ipAddress or host_ip == h.hostname:
			hostId=h.hostId
			break

	return hostId

#setup the repo on CM
def setupRepo():
	run_command("yum -y install createrepo")
	run_command("yum -y install httpd")
	command = "mkdir -p /var/www/html/cm"
	run_command(command)

	pack_dir = getXmlValue("distribution","package_path")
	parcel_ver = getXmlValue("distribution","parcel_version")
	if pack_dir.rfind("/")+1 == len(pack_dir):
		pack_dir = pack_dir[:-1]	
	os.rename(pack_dir+"/CDH-"+parcel_ver+"-el6.parcel",pack_dir[:pack_dir.rfind("/")]+"/CDH-"+parcel_ver+"-el6.parcel")
   	for file in os.listdir(pack_dir):
		try:
     			if not filecmp.cmp(pack_dir+"/"+file,"/var/www/html/cm/"+file):
					command = "cp "+pack_dir+"/"+file+" /var/www/html/cm/"
					run_command(command)
		except Exception as e:
			command = "cp "+pack_dir+"/"+file+" /var/www/html/cm/"
			run_command(command)
      			

	os.rename(pack_dir[:pack_dir.rfind("/")]+"/CDH-"+parcel_ver+"-el6.parcel",pack_dir+"/CDH-"+parcel_ver+"-el6.parcel")
	run_command(command)
	command = "createrepo /var/www/html/cm/"
	run_command(command)
	command = "service httpd start"
	run_command(command)

#validate XML
def validateXml():
	for key in roleList:
		key = key.split("-")
		nodestr = getXmlValue(key[0],key[1])
		if nodestr != None:
			list = nodestr.split(",")
			if key[1] == "Master":
				if len(list) > 2:
					logger.error("Invalid Configuration, Only two nodes supported in Master Mode for " + key[0]);
					return 1
					
			elif key[0] == "yarn":
				nodestr = getXmlValue("hdfs","Slave")
				datanodes = [ ]
				if nodestr != None:
					datanodes = nodestr.split(",")

				if len(datanodes) > len(list):
					for node in list:
						if node not in datanodes:
							logger.error("Invalid Configuration, Every Slave in yarn should have Slave in hdfs.!!\nDatanodes : "+str(datanodes)+"\nNode Manager : "+str(list));
							return 1
# Copy files to agents
def setupCMAgents():
	cm_server_host = getXmlValue("manager","host")
	hosts = getAllHostsXml()
	if len(hosts) <= 1:
		return 0
	logger.info("Installing packages on Cloudera Agents.....!!")
	status = [ ]
	opf = open("/opt/cto/log/cmdoutput.log", "a")
	for host in hosts:
		if host != cm_server_host:
			command = "ssh "+ host +" mkdir -p /opt/cto/scripts/"
			run_command(command)
			command = "ssh "+ host +" mkdir -p /opt/cto/conf/"
			run_command(command)
			command = "ssh "+ host +" mkdir -p /opt/cto/log/"
			run_command(command)
			command = "scp /opt/cto/scripts/* "+ host +":/opt/cto/scripts/"
			run_command(command)
			command = "scp -r /opt/cto/service_monitor_scripts/ "+ host +":/opt/cto/"
			run_command(command)
			command = "scp /opt/cto/conf/platform_template.xml "+ host +":/opt/cto/conf/"
			run_command(command)
			command = "scp /opt/cto/conf/mas-env.sh "+ host +":/opt/cto/conf/"
			run_command(command)
			command = "scp /opt/cto/conf/clouderaconfig.ini "+ host +":/opt/cto/conf/"
			run_command(command)			
			command = "ssh "+ host +" python /opt/cto/scripts/slave_platform.py"
			stat = sp.Popen(command,shell=True,stdout=opf,stderr=opf)
			status.append(stat)

	break_while = True
	while break_while:
		time.sleep(5)
		for stat in status:
			if stat.poll() != None:
				break_while = False	
			else:
				break_while = True

	opf.close()

	logger.info("Waiting for Cloudera Agents to come up .....!!")
	count = 0
	while 1:
		stop_while = 1
		for host in hosts:
			command = "ssh "+host+" service cloudera-scm-agent status"
			status = os.system(command)
			if status != 0:
				logger.info("Waiting for Cloudera Agent in " + host + " to come up .....!!")
				stop_while = 0
				break
   
		if stop_while == 1:
			break

		time.sleep(10)
		count = count + 1
  
		if count > 60:
			logger.error("Exiting Not All agents are in Sync with Cloudera Manager .....!!")
			sys.exit(1)
