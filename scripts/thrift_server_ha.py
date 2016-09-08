import socket, sys, utils, logging, re, subprocess, ctologger
from subprocess import Popen, PIPE, STDOUT

logger = ctologger.getCdhLogger()

def deploySparkThrift():
	try:
		#initialize Variables
		CM_HOST = utils.getXmlValue("manager", "host")
		THRIFT_HS2_HOST = utils.getXmlValue("thrift-sql","hosts").split(',')
		THRIFT_LOAD_BALANCER =  utils.getXmlValue("thrift-sql","thrift_load_balancer").split(':')
		no_of_hosts = len(THRIFT_HS2_HOST)

		
		if no_of_hosts > 1:
			
			if len(THRIFT_LOAD_BALANCER) == 2:
				THRIFT_LOAD_BALANCER_HOST = THRIFT_LOAD_BALANCER[0]
				THRIFT_LOAD_BALANCER_PORT = THRIFT_LOAD_BALANCER[1]
			else:
				logger.exception("Wrong arguments for thriftrserver_load_balancer property. Expected is host:port")
				raise Exception("Wrong arguments for thriftrserver_load_balancer property. Expected is host:port")
				
			#we can delete  related variables as they are not being used anywhere
			'''
			HIVE_HA_LOAD_BALANCER =  utils.getXmlValue("hive","hive-server2-config","hiverserver2_load_balancer").split(':')
			if len(HIVE_HA_LOAD_BALANCER) == 2:
				HIVE_HA_LOAD_BALANCER_HOST = HIVE_HA_LOAD_BALANCER[0]
				HIVE_HA_LOAD_BALANCER_PORT = HIVE_HA_LOAD_BALANCER[1]
			else:
				logger.exception("Wrong arguments for hiveserver2_load_balancer property. Expected is host:port")
				raise Exception("Wrong arguments for hiveserver2_load_balancer property. Expected is host:port")
			'''
			
			prepare_ha(CM_HOST, THRIFT_HS2_HOST, THRIFT_LOAD_BALANCER, THRIFT_LOAD_BALANCER_HOST,	THRIFT_LOAD_BALANCER_PORT, no_of_hosts )
			
			start_thrift_server(CM_HOST, THRIFT_HS2_HOST)
		else:
			start_thrift_server(CM_HOST, THRIFT_HS2_HOST)
			
	except Exception, e:
		logger.exception(e)
		raise Exception(e)

def prepare_ha(CM_HOST, THRIFT_HS2_HOST, THRIFT_LOAD_BALANCER, THRIFT_LOAD_BALANCER_HOST,	THRIFT_LOAD_BALANCER_PORT, no_of_hosts ):
	
	try:
		status = 0
		
		for host in THRIFT_HS2_HOST:
			output = subprocess.Popen(['ssh', host ,'service', 'haproxy', 'status'], stdout=subprocess.PIPE) #we can ignore CM_HOST case here because we are not using logger here
			tmp = output.stdout.read()
			tmp = tmp.rstrip()
			
			if re.search("is running",tmp):
				status = status + 1;
				logger.info("HAProxy running on " + host)
			
			#checking for port is listening or not
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.settimeout(2)                                     
			
			result = sock.connect_ex((host,int(THRIFT_LOAD_BALANCER_PORT)))
			if result == 0:
			   logger.error( "Thrift Loadbalancer port is listening for host "+ host )
			   logger.exception( "returning from function abnormally because port is already being used")
			   raise Exception("Function returning abnormally because port is already being used")
			else:
			   logger.info( "Thrift Loadbalancer port is not listening for host " + host )
		
		if status == no_of_hosts:
			for host in THRIFT_HS2_HOST:
				append_hainfo(host, CM_HOST, THRIFT_HS2_HOST, THRIFT_LOAD_BALANCER, THRIFT_LOAD_BALANCER_HOST,	THRIFT_LOAD_BALANCER_PORT, no_of_hosts)
					
		elif status != 0:
			logger.error( "HAProxy is running on only "+ str(status) + " node" )
			logger.exception( "returning from function abnormally because haproxy is running on only "+ str(status) + " node" )
			raise Exception("Function returning abnormally because haproxy is running on only "+ str(status) + " node" )
		elif status == 0:
			logger.info("HAProxy not running on any of the nodes")
			logger.info("Installing HAProxy ...")
			
			haproxy_setup(CM_HOST, THRIFT_HS2_HOST, THRIFT_LOAD_BALANCER, THRIFT_LOAD_BALANCER_HOST,	THRIFT_LOAD_BALANCER_PORT, no_of_hosts )


		status = 0
		
		for host in THRIFT_HS2_HOST:
			output = subprocess.Popen(['ssh', host ,'service', 'keepalived', 'status'], stdout=subprocess.PIPE) #we can ignore CM_HOST case here because we are not using logger here
			tmp = output.stdout.read()
			tmp = tmp.rstrip()
			
			if re.search("is running",tmp):
				status = status + 1;
				
				try:
					with open("/etc/keepalived/keepalived.conf", "r") as sources:
						lines = sources.readlines()
					sources.close()
				except Exception, e:   
					logger.error("Error in opening file /etc/keepalived/keepalived.conf")
					logger.exception(e)
					raise Exception(e)
				
				myvip = ""
				
				for line in lines:
					if re.match("virtual_ipaddress",line):
						myvip = lines.next()
				
				logger.info("Keepalived running on " + host + " with vip " + myvip )
				return
		
		
		if status != 0:
			logger.error( "Keepalived is running on only "+ str(status) + " node" )
			logger.exception( "Function returning abnormally because keepalived is running on only "+ str(status) + " node" )
			raise Exception( "Function returning abnormally because keepalived is running on only "+ str(status) + " node" )
		elif status == 0:
			logger.info("Keepalived not running on any of the nodes")
			logger.info("Installing Keepalived...")

			vip = THRIFT_LOAD_BALANCER_HOST  #to be added from xml
			keepalived_setup(vip, CM_HOST, THRIFT_HS2_HOST, THRIFT_LOAD_BALANCER, THRIFT_LOAD_BALANCER_HOST,	THRIFT_LOAD_BALANCER_PORT, no_of_hosts)
		
	except Exception, e:
		logger.exception(e)
		raise Exception(e)
	
def start_thrift_server(CM_HOST, THRIFT_HS2_HOST):
	
	try:
		#to make sure all commands are in single shell session (exports)		
		#cmd1 = "source /opt/cto/conf/mas-env.sh; source \$SPARK_SCRIPT_DIR/mas-spark-env.sh; sh \$SPARK_SCRIPT_DIR/start-thriftserver.sh"
		
		
		for host in THRIFT_HS2_HOST:
			logger.info("Starting thrift server on "+ host)
			
			if host != CM_HOST:
				command = "ssh "+ host + " " + "yum -y install NSN-CTO-thirdparty-libs"
				utils.run_command(command)
				logger.info("command executed : " + command)
				
				command="ssh " + host + " "+ "\"source /opt/cto/conf/mas-env.sh; source \$SPARK_SCRIPT_DIR/mas-spark-env.sh; sh \$SPARK_SCRIPT_DIR/start-thriftserver.sh\""
				utils.run_command(command)
				logger.info("Command executed : " + command )
				
			else:
				
				command = "yum -y install NSN-CTO-thirdparty-libs"
				utils.run_command(command)
				logger.info("command executed : " + command)
				
				command="source /opt/cto/conf/mas-env.sh; source $SPARK_SCRIPT_DIR/mas-spark-env.sh; sh $SPARK_SCRIPT_DIR/start-thriftserver.sh"
				utils.run_command(command)
				logger.info("Command executed : " + command )
				
	except Exception, e:
		logger.exception( "\n\nPlease check if thrift server is already running? or mas-spark-env.sh exists?\n\n")
		logger.exception(e)
		raise Exception(e)

def append_hainfo(host, CM_HOST, THRIFT_HS2_HOST, THRIFT_LOAD_BALANCER, THRIFT_LOAD_BALANCER_HOST,	THRIFT_LOAD_BALANCER_PORT, no_of_hosts):
	#put try catch
	try:
		try:
			#change needed for reading/writing remote host file
			#we will copy /etc/haproxy/haproxy.cfg to temp location for both cm_host and ssh hosts and then do changes on this temp file and finally paste it in required location on cm_host/ssh host
			
			if host != CM_HOST:
				command="scp " + host + ":/etc/haproxy/haproxy.cfg /tmp"
				utils.run_command(command)
				logger.info("Command executed : " + command )
				
			else:
				command="cp /etc/haproxy/haproxy.cfg /tmp"
				utils.run_command(command)
				logger.info("Command executed : " + command )
				
			with open("/tmp/haproxy.cfg","r") as source1:
				lines1 = source1.readlines()
		
		except Exception, e:   
			logger.error("Error in opening file /tmp/haproxy.cfg" )
			logger.exception(e)
			raise Exception(e)
		
		for line1 in lines1:
			if re.match("listen thrift_proxy",line1):
				command="rm -f /tmp/haproxy.cfg"
				utils.run_command(command)
				logger.info("Command executed : " + command )
				return  #entry already in config file, dont append

		source1.close();
		
		cnt = 1
		#will change later to read from /opt/cto/conf/haproxy_default.cfg
		try:
			with open("/opt/cto/conf/thrift_haproxy_default.cfg", "r") as sources:
				lines = sources.readlines()
		except Exception, e:   
			logger.error("Error in opening file /opt/cto/conf/thrift_haproxy_default.cfg" )
			logger.exception(e)
			raise Exception(e)
		
		sources.close()
		
		mystring = "\n\n"

		mybool = 0

		for line in lines:
			if re.match("^listen",line):
				mystring = mystring + re.sub("<proxy_port>",THRIFT_LOAD_BALANCER_PORT, line)
				mybool = 1
			elif re.search("<thrift_server_host>", line):
				for host1 in THRIFT_HS2_HOST:
					mystring = mystring + re.sub("<thrift_server_host>",host1,re.sub("thriftser<no>","thriftser"+str(cnt),line))	
					cnt=cnt+1
			elif mybool == 1:
				mystring = mystring + line
				
		#will change later to point to /etc/haproxy/haproxy.cfg
		try:
			with open("/tmp/haproxy.cfg","a") as writesource:
				writesource.write(mystring)		#check if it is appending or deleting and writing
			writesource.close()
		except Exception, e:   
			logger.error("Error in opening file /tmp/haproxy.cfg" )
			logger.exception(e)
			raise Exception(e)
		
		if host != CM_HOST:
			command="scp /tmp/haproxy.cfg " + host + ":/etc/haproxy/" #scp overwrites by default
			utils.run_command(command)
			logger.info("Command executed : " + command )
			
		else:
			command="yes | cp -f /tmp/haproxy.cfg /etc/haproxy/" #overwriting without prompt
			utils.run_command(command)
			logger.info("Command executed : " + command )
		
		command="rm -f /tmp/haproxy.cfg"
		utils.run_command(command)
		logger.info("Command executed : " + command )

	
	except Exception, e:
		logger.exception(e)
		raise Exception(e)

def keepalived_setup(vip, CM_HOST, THRIFT_HS2_HOST, THRIFT_LOAD_BALANCER, THRIFT_LOAD_BALANCER_HOST,	THRIFT_LOAD_BALANCER_PORT, no_of_hosts ):
	try:
		priority = 100
		logger.info("Enabling keepalived on VIP "+ vip)
		for host in THRIFT_HS2_HOST:
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
			
			#need to change later to "/opt/cto/conf/keepalived_default.conf"
			try:
				with open("/opt/cto/conf/keepalived_default.conf", "r") as sources:
					lines = sources.readlines()
			except Exception, e:   
				logger.error("Error in opening file /opt/cto/conf/keepalived_default.conf" )
				logger.exception(e)
				raise Exception(e)
			#need to change later to "/opt/cto/conf/keepalived.conf"
			try:
				with open("/opt/cto/conf/keepalived.conf","w") as writesource:
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
			except Exception, e:   
				logger.error("Error in opening file /opt/cto/conf/keepalived.conf")
				logger.exception(e)
				raise Exception(e)
					
			#install and start keepalived service demon 
			logger.info("Installing keepalived on host " + host)
			if host != CM_HOST:
				command="ssh " + host + " yum -y install keepalived"
			else:
				command="yum -y install keepalived"
			utils.run_command(command)
			
			#copying keepalived.conf to haproxy machine
			logger.info("Copying keepalived.conf file to "+host)
			
			if host != CM_HOST:			
				command = "scp /opt/cto/conf/keepalived.conf "+ host +":/etc/keepalived/"
			else:
				command = "cp /opt/cto/conf/keepalived.conf /etc/keepalived/"
			utils.run_command(command)
			# starting keepalived service
			if host != CM_HOST:			
				command="ssh " + host + " service keepalived restart"
			else:
				command="service keepalived restart"
			utils.run_command(command)
			
			logger.info("Starting keepalived service on reboot "+ host)
			if host != CM_HOST:			
				command="ssh " + host + " chkconfig keepalived on"
			else:
				command = "chkconfig keepalived on"
			utils.run_command(command)
						
			# delete the keepalived.conf file in /opt/cto/conf
			command="rm -rf /opt/cto/conf/keepalived.conf"
			utils.run_command(command)
			
			logger.info("Successfully enabled keepalived on host "+ host)
	except Exception, e:
		logger.error("Error setting up keepalived service " )
		logger.exception(e)
		raise Exception(e)
		
def haproxy_setup(CM_HOST, THRIFT_HS2_HOST, THRIFT_LOAD_BALANCER, THRIFT_LOAD_BALANCER_HOST,	THRIFT_LOAD_BALANCER_PORT, no_of_hosts):
	try:
		proxy_port = THRIFT_LOAD_BALANCER_PORT
		#configuring haproxy.cfg file
		logger.info("Configuring haproxy on proxy port "+ proxy_port)
		cnt=1
		
		for host in THRIFT_HS2_HOST:
			#editing haproxy.cfg file to be placed finally in /etc/haproxy/
			
			#need to change to /opt/cto/conf/thrift_haproxy_default.cfg
			try:
				with open("/opt/cto/conf/thrift_haproxy_default.cfg", "r") as sources:
					lines = sources.readlines()
			except Exception, e:   
				logger.error("Error in opening file /opt/cto/conf/thrift_haproxy_default.cfg" )
				logger.exception(e)
				raise Exception(e)

			#need to change to /opt/cto/conf/haproxy.cfg
			try:
				with open("/opt/cto/conf/haproxy.cfg","w") as writesource:
					for line in lines:
						if re.match("^listen",line):
							writesource.write(re.sub("<proxy_port>",proxy_port, line))
						elif re.search("<thrift_server_host>", line):
							for host1 in THRIFT_HS2_HOST:
								writesource.write(re.sub("<thrift_server_host>",host1,re.sub("thriftser<no>","thriftser"+str(cnt),line)))	
								cnt=cnt+1	
						else:	
							writesource.write(line)		

				writesource.close()
			except Exception, e:   
				logger.error("Error in opening file /opt/cto/conf/haproxy.cfg")
				logger.exception(e)
				raise Exception(e)

			#install and start haproxy
			if host != CM_HOST:
				logger.info("Installing haproxy on host " + host + " Port:"+proxy_port)
				command="ssh " + host + " yum -y install haproxy"
				utils.run_command(command)

				#copying haproxy.cfg to haproxy machine
				logger.debug("Copying haproxy.cfg file to "+host)
				command = "scp /opt/cto/conf/haproxy.cfg " + host +":/etc/haproxy/"
				# starting haproxy service
				utils.run_command(command)
				logger.info("Starting haproxy service on "+ host)
				command="ssh " + host +" service haproxy restart"
				utils.run_command(command)
				
				logger.info("Starting haproxy service on reboot "+ host)
			
				command="ssh " + host + " chkconfig haproxy on"
				utils.run_command(command)
			
			else:
				logger.info("Installing haproxy on host " + host + " Port:"+proxy_port)
				command="yum -y install haproxy"
				utils.run_command(command)

				#copying haproxy.cfg to haproxy machine
				logger.debug("Copying haproxy.cfg file to "+host)
				command = "cp /opt/cto/conf/haproxy.cfg /etc/haproxy/"
				# starting haproxy service
				utils.run_command(command)
				logger.info("Starting haproxy service on "+ host)
				command="service haproxy restart"
				utils.run_command(command)
				
				logger.info("Starting haproxy service on reboot "+ host)
			
				command="chkconfig haproxy on"
				utils.run_command(command)

		#delete  haproxy.cfg file in /opt/cto/conf
		del_cmd = "rm -rf /opt/cto/conf/haproxy.cfg"
		utils.run_command(del_cmd)
		logger.info("Haproxy successfully installed on port ")
	except Exception, e:
		logger.error("Error setting up haproxy ")
		logger.exception(e)
		raise Exception(e)
	
		
if __name__ == "__main__":
	deploySparkThrift()
	