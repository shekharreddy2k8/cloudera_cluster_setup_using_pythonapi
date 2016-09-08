#!/usu/bin/python
import sys,os,subprocess,commands,re
import ctologger,utils 

logger = ctologger.getCdhLogger()

###=======This function will check if all the necessary rpms listed in the configuration file 'rpm_config.txt' are pre-installed.=====###
def check_rpm():
    try:
        with open('/opt/cto/conf/rpm_config.txt','r') as f:
            content = f.read().splitlines()
	    installed=True
            for lines in content:
                cmd=subprocess.Popen('rpm -q %s'%(lines), shell=True, stdout=subprocess.PIPE)
                for line in cmd.stdout:
                    pattern='is not installed'
                    result=pattern in line
                    if result==True:
                        logger.error('%s is not installed'%(lines))
			installed=False
	    if not installed:
            	raise Exception("All the RPMS need to be pre-installed!!")
            logger.info('The rpms are installed!!!')  
    except Exception, e:
	raise Exception(e)
	
###=======This function will validate the versions of haproxy and keepalived.=======###
def check_version(sw1,sw2):
    try:
        #cmd=subprocess.Popen('%s -version'%(sw1), shell=True, stdout=subprocess.PIPE)
        res,output=commands.getstatusoutput('%s -version'%(sw1))
        #logger.info('%s'%(output))
        out=output.split('\n')
        for line in out:
            pattern='1.5.2'
            found=False
            if pattern in line:
                logger.info('The version of %s is %s'%(sw1,pattern))
                found=True
                break
        if not found:
            logger.warning('%s %s does not exist'%(sw1,pattern))


        #cmd=subprocess.Popen('%s -version'%(sw2), shell=True, stdout=subprocess.PIPE)
        res,output=commands.getstatusoutput('%s -version'%(sw2))
        #logger.info('%s'%(output))
        out=output.split('\n')
        for line in out:
            pattern='v1.2.13'
            found=False
            if pattern in line:
                logger.info('The version of %s is %s'%(sw2,pattern))
                found=True
                break
        if not found:
            logger.warning('%s %s does not exist'%(sw2,pattern))
            #sys.exit()

    except Exception, e:
        logger.error('Error in getting ha proxy version or keepalived version. Exception:' + str(e))
        raise Exception(e)
        

###======This function will validate the pre-installation configurations.======###
def check_config():
    try:

        ##Checking pre-installation Step3:validating vm.swappiness=0 is present in /etc/sysctl.conf and echo never > /sys/kernel/mm/redhat_transparent_hugepage/defrag is added in /etc/rc.local

        os.system('sysctl vm.swappiness=0')
        with open('/etc/sysctl.conf','a+')as f:
            content = f.read().splitlines()
            found=False
            f.seek(0,0)
            pattern='vm.swappiness=0'
            for lines in content:
                line=re.sub('[^a-zA-Z0-9-_*.\=#]', '', lines)
                if pattern in line:
                    if '#' in line:
                        logger.info('%s in /etc/sysctl.conf is commented'%(line))
                    else: 
                        logger.info('%s is present in /etc/sysctl.conf'%(line))
                        found=True
                        break 
            if not found:
                logger.warning('vm.swappiness = 0 is not present in /etc/sysctl.conf.So,Adding this line into /etc/sysctl.conf')
                f.write('vm.swappiness = 0')

        os.system('echo never > /sys/kernel/mm/redhat_transparent_hugepage/defrag')
        with open('/etc/rc.local','a+')as f:
            content = f.read().splitlines()
            found=False
            f.seek(0,0)
            pattern='/sys/kernel/mm/redhat_transparent_hugepage/defrag'
            for lines in content:
                line=re.sub('[^a-zA-Z0-9-_*.\>/=#]', '', lines)
                if pattern in line:
                    if '#' in line:
                        logger.info('%s in /etc/rc.local is commented'%(lines))
                    else: 
                        logger.info('%s is present in /etc/rc.local'%(lines))
                        found=True
                        break 
            if not found:
                logger.warning('echo never > /sys/kernel/mm/redhat_transparent_hugepage/defrag is not present in /etc/rc.local.So,Adding this line into /etc/rc.local.')
                f.write('echo never > /sys/kernel/mm/redhat_transparent_hugepage/defrag\n')


        ##===Checking pre-installation Step2:Validating if all the hostnames in platform_template.xml are added in /etc/hosts and the installed machine hostname is added in /etc/sysconfig/network===##

        #cmd=subprocess.Popen('hostname', shell=True, stdout=subprocess.PIPE)
        res,output=commands.getstatusoutput('hostname')
        out=output.split('\n')
        host_name=out[0]
        #logger.info(host_name)
        '''
        for lines in cmd.stdout:
        #print lines
        host_name=lines
        print host_name
        '''
        with open('/etc/sysconfig/network','a+')as f:
            f.seek(0,0)
            content = f.read().splitlines()
            found=False
            for line in content:
                result=host_name in line
                if host_name in line:
                    #logger.info('hostname is present in /etc/sysconfig/network')
                    found=True
                    break
            if not found:
                logger.warning('Hostname is not present in /etc/sysconfig/network.So, adding the hostname into this file\n')
                host=('HOSTNAME=%s\n'%host_name)
                f.write(host)
        ####Getting all the hostnames from platform_template.xml###
        manager_host=[(utils.getXmlValue("manager", "host"))]
        kerberos_host=[(utils.getXmlValue("kerberos", "KDC_HOST"))]
        zookeeper_hosts=utils.getXmlValue("zookeeper", "hosts").split(",")
        hdfs_hosts=utils.getXmlValue("hdfs", "Master").split(",")+utils.getXmlValue("hdfs", "Slave").split(",")+utils.getXmlValue("hdfs", "journal").split(",")
        yarn_hosts=utils.getXmlValue("yarn", "Master").split(",")+utils.getXmlValue("yarn", "Slave").split(",")+utils.getXmlValue("yarn", "gateway").split(",")
        hive_hosts=utils.getXmlValue("hive", "hosts").split(",")+utils.getXmlValue("hive", "gateway").split(",")
        hbase_hosts=utils.getXmlValue("hbase", "Master").split(",")+utils.getXmlValue("hbase", "Slave").split(",")+utils.getXmlValue("hbase", "gateway").split(",")
        spark_hosts=utils.getXmlValue("spark", "hosts").split(",")
        thrift_hosts=utils.getXmlValue("thrift-sql", "hosts").split(",")
        hosts=manager_host+kerberos_host+zookeeper_hosts+hdfs_hosts+yarn_hosts+hive_hosts+hbase_hosts+spark_hosts+thrift_hosts
        #print hosts
        host_list=list(set(hosts))
        host_names=filter(None,host_list)
        i,j=0,0
	with open('/etc/hosts','a+')as f:
            f.seek(0,0)
            content = f.read().splitlines()
	    flag=True
            for host in host_names:
                #print(host)
                found=False
                i=i+1
                for line in content:
                    res=host in line
                    if res==True:
			found=True
                        #logger.info('%s is found in /etc/hosts'%(host))
                        break
            	    
		if not found:
		    logger.error('%s is not found in /etc/hosts.'%(host))
		    flag=False
	    if not flag:
		raise Exception('All the hostnames should be present')
		
                    
        ##Checking pre-installation Step6:Validating if passwordless ssh is available for all the hosts in platform_template.xml##

        logger.info('#######VALIDATION OF PASSWORDLESS SSH BEGINS#########')
	flag=True
        for host in host_names:
            j=j+1
            #print host
            #print j
            HOST=host
            COMMAND="hostname"
            ssh = subprocess.Popen(["ssh", "-oNumberOfPasswordPrompts=0","%s"%HOST, COMMAND],shell=False,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            result = ssh.stdout.readlines()
            #print result
            if result == []:
                error = ssh.stderr.readlines()
                #print >>sys.stderr, "ERROR: %s" % error
                logger.error('Passwordless ssh failed for %s. Error: %s'%(host,error))
		flag=False
                continue
            else:
                out=str(result)
                #res=re.sub('[^a-zA-Z0-9-_*.\=]', '', out)
                #print res
                output=HOST in out
                #print output
                if output==True:
                    #logger.info('passwordless ssh successfuli for %s'%host)
                    pass
                    #print out
	if not flag:
		raise Exception('Passwordless ssh should be successful for all the hosts!!')

        logger.info('Passwordless ssh successful!!') 
    except Exception,e:
        logger.error('Exception in checking configurations.Exception is :'+str(e))
        raise Exception(e)

###======This function will check if the correct parcels exists or not=======###
def check_parcel():
    try:
        PARCEL_VERSION = utils.getXmlValue("distribution", "parcel_version")
        #print PARCEL_VERSION
        #os.system('rpm -q %s'%(PARCEL_VERSION))
        parcel_name1='CDH-'+PARCEL_VERSION+'-el6.parcel'
        if (os.path.isfile('/opt/cloudera/parcel-repo/%s'%(parcel_name1))):
            logger.info('%s parcel exist'%(parcel_name1))
        else:
            logger.error('%s parcel does not exist'%(parcel_name1))
            sys.exit()
            
        parcel_name2='CDH-'+PARCEL_VERSION+'-el6.parcel.sha'
        if (os.path.isfile('/opt/cloudera/parcel-repo/%s'%(parcel_name2))):
            logger.info('%s parcel exist'%(parcel_name2))
        else:
            logger.error('%s parcel does not exist'%(parcel_name2))
            sys.exit()

    except Exception,e:
        logger.error('Error in checking if parcel exists. Exception:' + str(e))
        logger.exception(e)
        return 1
    
###==================This is the main method====================###
def main():
    # print "This is Main method"
    logger.info('*******VALIDATION OF ENVIRONMENT BEGIN******')
    rpm=check_rpm()
    version=check_version('haproxy','keepalived')
    config=check_config()
    parcel=check_parcel()
    
if __name__ == "__main__":
    main() 





