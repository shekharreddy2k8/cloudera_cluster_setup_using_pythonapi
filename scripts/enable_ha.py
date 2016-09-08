import cm_utils, ctologger, re, utils, subprocess
from subprocess import Popen, PIPE, STDOUT 
logger = ctologger.getCdhLogger()

def enableHA(hosts,service,role,proxy_name):
	try:
		logger.info("Enabling HA for Hosts ")
		## HA related 
		HA_PROXY =  utils.getXmlValue(service,role+"_load_balancer").split(':')
		if len(HA_PROXY) == 2:
			HA_VIRTUAL_IP = HA_PROXY[0]
			HA_PROXY_PORT = HA_PROXY[1]
			
			#configure and enable Haproxy
			haproxyRunning = isHaProxyRunningOnAllHosts(hosts)
			if(haproxyRunning == True):
			# append haproxy cong to existing configuration
				append_haproxy(HA_PROXY_PORT,hosts,proxy_name,service)
			else:
			# set up haproxy by installing haproxy and configurations
				haproxy_setup(HA_PROXY_PORT,hosts,service)
			
			
			#configure and enable keepalived for HA of Haproxy	
			keepalivedRunning = isKeepalivedRunningOnAllHosts(hosts)
			if(keepalivedRunning == True):
				logger.info("Keepalived is already running on all hosts. So using existing setup")
			else:
				keepalived_setup(HA_VIRTUAL_IP,hosts)
			
			logger.info("Successfully enabled HA for "+service)
			return 0
			
		else:
			logger.error("Wrong arguments for "+role+"_load_balancer property. Excepted format is vip:port")
			raise Exception("Wrong arguments for "+role+"_load_balancer property. Excepted format is vip:port")

	except Exception, e:
		logger.error("Error enabling HA for "+service + str(e))
		logger.exception(e)
		raise Exception(e)
	
def keepalived_setup(vip,hosts):
	try:
		priority = 100
		logger.info("Enabling keepalived on VIP "+ vip)
		for host in hosts:
		#   get host ip 
			logger.info("Enabling keepalived on host "+host)
			hostip_cmd = ["ssh "+host+" hostname -i"]
			hostip_result = Popen(hostip_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True).stdout.read()
			hostip = hostip_result.strip()
			logger.debug("host ip of "+ host + " is "+ hostip)
			
			ethif_cmd = ["ssh "+host+" ifconfig -a |grep -B1 "+ hostip+ " |head -n1 | awk '{print $1}'"]
			ethif_result = Popen(ethif_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True).stdout.read()
			ethif = ethif_result.strip()
			logger.debug("host "+ host + " is connected to eth interface "+ ethif)
			
			# priority for each keepalived server is increased by 1
			priority = priority+1
			with open("/opt/cto/conf/keepalived_default.conf", "r") as sources:
				lines = sources.readlines()

			with open("/tmp/keepalived.conf","w") as writesource:
				for line in lines:
					if re.search("interface <ethif>", line):
						writesource.write(re.sub("<ethif>",ethif,line))	
					elif re.search("priority <priority>", line):
						writesource.write(re.sub("<priority>",str(priority),line))
					elif re.search("<virtualip>", line):
						writesource.write(re.sub("<virtualip>",vip,line))				
					else:
						writesource.write(line)		

				writesource.close()
				
			#install and start keepalived service demon 
			logger.info("Installing keepalived on host " + host)
			command="ssh " + host + " yum -y install keepalived"
			utils.run_command(command)
			
			#copying keepalived.conf to haproxy machine
			logger.info("Copying keepalived.conf file to "+host)
			command = "scp /tmp/keepalived.conf "+ host +":/etc/keepalived/"
			utils.run_command(command)
			# starting keepalived service
			logger.info("Starting keepalived service on "+ host)
			command="ssh " + host + " service keepalived restart"
			utils.run_command(command)
			
			# delete the keepalived.conf file in /tmp
			command="rm -rf /tmp/keepalived.conf"
			utils.run_command(command)
			
			logger.info("Successfully enabled keepalived on host "+ host)
		return 0
	except Exception, e:
		logger.error("Error setting up keepalived service :" + str(e) )
		logger.exception(e)
		raise Exception(e)
		
def haproxy_setup(proxy_port,hosts,service):
	try:
		#configuring haproxy.cfg file
		logger.info("Configuring haproxy on servers "+ str(hosts) + " with proxy port "+ proxy_port)
		cnt=1
		with open("/opt/cto/conf/"+service+"_haproxy_default.cfg", "r") as sources:
			lines = sources.readlines()

		with open("/tmp/haproxy.cfg","w") as writesource:
			for line in lines:
				if re.match("^listen",line):
					writesource.write(re.sub("<proxy_port>",proxy_port, line))
				elif re.search("<server_host>", line):
					for host in hosts:
						writesource.write(re.sub("<server_host>",host,re.sub("ser<no>","ser"+str(cnt),line)))	
						cnt=cnt+1	
				else:	
					writesource.write(line)		

			writesource.close()
	
		# configure haproxy on each hosts
		for host in hosts:
			#install and start haproxy
			logger.info("Installing haproxy on host " + host + " Port:"+proxy_port)
			command="ssh " + host + " yum -y install haproxy"
			utils.run_command(command)

			#copying haproxy.cfg to haproxy machine
			logger.debug("Copying haproxy.cfg file to "+host)
			command = "scp /tmp/haproxy.cfg "+ host +":/etc/haproxy/"
			# starting haproxy service
			utils.run_command(command)
			logger.info("Starting haproxy service on "+ host)
			command="ssh " + host + " service haproxy restart"
			utils.run_command(command)
		
		#delete  haproxy.cfg file in /tmp
		del_cmd = "rm -rf /tmp/haproxy.cfg"
		utils.run_command(del_cmd)
		logger.info("Haproxy successfully installed on port ")
		return 0
	except Exception, e:
		logger.error("Error setting up haproxy : "+ str(e))
		logger.exception(e)
		raise Exception(e)

def isHaProxyRunningOnAllHosts(hosts):
	try:
		logger.info("Checking if Haproxy is running on "+str(hosts))
		noOfHostRunning = 0
		for host in hosts:
			cmd = subprocess.Popen(['ssh', host ,'service', 'haproxy', 'status'], stdout=subprocess.PIPE)
			string = cmd.stdout.read()
			string = string.rstrip()
			
			if re.search("is running",string):
				noOfHostRunning = noOfHostRunning + 1;
				logger.info("HAProxy is running on " + host)		
			else:
				logger.info("HAProxy is not running on " + host)
				
		if noOfHostRunning == len(hosts):
			logger.info("HAProxy is running on all hosts")
			return True
		elif noOfHostRunning == 0:
			logger.info( "HAProxy is not running on any hosts" )
			return False
		else:
			logger.error("HAProxy is not running on all.. Wrong hosts configuration")
			raise Exception("HAProxy is not running on all.. Wrong hosts configuration")		
			
	except Exception, e:
		logger.error("Exception in isHaProxyRunningOnAllHosts while checking on hosts "+ str(host) + " Error is :"+ str(e))
		logger.exception(e)
		raise Exception(e)

def isKeepalivedRunningOnAllHosts(hosts):
	try:
		logger.info("Checking if keepalived is running on "+str(hosts))
		noOfHostRunning = 0
		for host in hosts:
			cmd = subprocess.Popen(['ssh', host ,'service', 'keepalived', 'status'], stdout=subprocess.PIPE)
			string = cmd.stdout.read()
			string = string.rstrip()
			
			if re.search("is running",string):
				noOfHostRunning = noOfHostRunning + 1;
				logger.info("Keepalived is running on " + host)		
			else:
				logger.info("Keepalived is not running on " + host)
				
		if noOfHostRunning == len(hosts):
			logger.info("Keepalived is running on all hosts")
			return True
		elif noOfHostRunning == 0:
			logger.info( "Keepalived is not running on any hosts" )
			return False
		else:
			logger.error("Keepalived is not running on all.. Wrong hosts configuration")
			raise Exception("Keepalived is not running on all.. Wrong hosts configuration")	
			
	except Exception, e:
		logger.error("Exception in isKeepalivedRunningOnAllHosts while checking on host "+ str(host) + " Error is :"+ str(e))
		logger.exception(e)
		raise Exception(e)
		
def append_haproxy(proxy_port,hosts,proxy_name,service):
	try:
		logger.info("Appending haproxy on servers "+ str(hosts) + " with proxy port "+ proxy_port)
		for host in hosts:
			#copy haproxy.cfg file to append the configurations for hive
			command="scp " + host + ":/etc/haproxy/haproxy.cfg /tmp"
			utils.run_command(command)
			logger.debug("haproxy.cfg copy command : " + command)
			
			with open("/tmp/haproxy.cfg","r") as existing_source:
				existing_lines = existing_source.readlines()
			existing_source.close()
			
			# check if hiveserver2 related listen configuration already exist
			config = False
			for existing_line in existing_lines:
				if re.match("listen "+proxy_name,existing_line):
					config = True
					# remove the conf file copied in tmp
					command="rm -f /tmp/haproxy.cfg"
					utils.run_command(command)
					logger.info("hiveserver2 related haproxy listen configuration already exists on host "+ host)
			
			# append config only if hive config does not exist
			if(not config):
				# read the default hive_haproxy_default file
				with open("/opt/cto/conf/"+service+"_haproxy_default.cfg", "r") as sources:
					lines = sources.readlines()
				sources.close()
			
				config_string ="\n\n"
				
				listen_config = 0
				cnt=1
				# create hive related haproxy string which will be added to haproxy.cfg file
				for line in lines:
					if re.match("^listen "+proxy_name,line):
						config_string = config_string + re.sub("<proxy_port>",proxy_port, line)
						listen_config = 1
					elif re.search("<hive_server2_host>", line):
						for host1 in hosts:
							config_string = config_string + re.sub("<hive_server2_host>",host1,re.sub("ser<no>","ser"+str(cnt),line))
							cnt=cnt+1
					elif listen_config == 1:
						config_string = config_string + line
				
				# add the hive listen haproxy to cfg file
				logger.debug("Appending hive related haproxy conf to haproxy.cfg file. Listen String added is: "+ config_string)
				with open("/tmp/haproxy.cfg","a") as writesource:
					writesource.write(config_string)
				writesource.close()
				
				# copy back the configuration file to the server
				command="scp /tmp/haproxy.cfg " + host + ":/etc/haproxy/" #scp overwrites by default
				utils.run_command(command)
				logger.info("Copied the hive related haproxy configurations to server "+ host)
					
				logger.info("Re-Starting haproxy service on "+ host)
				command="ssh " + host + " service haproxy restart"
				utils.run_command(command)
								
				#delete  haproxy.cfg file in /tmp
				del_cmd = "rm -rf /tmp/haproxy.cfg"
				utils.run_command(del_cmd)			
				
	except Exception, e:
		logger.error("Error while appending haproxy conf for hiveserver2. Exception is "+ str(e))
		logger.exception(e)
		raise Exception(e)

if __name__ == "__main__":
	enableHA(['cemod86.nokia.com'],"sparkthrift","thrift","thrift_proxy")
