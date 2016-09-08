# cloudera_cluster_setup_using_pythonapi

Prerequisites for Automated Installation:
-------------------------------------------

1. Sufficient diskspace should be available under / (Atleast 20GB). Check using command "df -kh"

2. Check hostname on INSTALL_MACHINE:

	all hosts FQDN should be present in /etc/hosts
	lines in /etc/hosts should look like:
		cat /etc/hosts :
			xxx.xxx.xxx.xxx	FQDN short_hostname
		example:
			135.250.193.206	xxx.org.com	xxx
			
	Edit /etc/sysconfig/network and make sure the hostname is the FQDN of INSTALL_MACHINE
			example: hostname=xxx.org.com

3. Other settings on INSTALL_MACHINE:
	run command on INSTALL_MACHINE:
		sysctl vm.swappiness=0

	edit /etc/sysctl.conf add line on INSTALL_MACHINE
		vm.swappiness = 0
	
	run command on INSTALL_MACHINE:
		echo never > /sys/kernel/mm/redhat_transparent_hugepage/defrag

	edit /etc/rc.local add line on INSTALL_MACHINE:
		echo never > /sys/kernel/mm/redhat_transparent_hugepage/defrag

4. Following rpms need to be pre-installed

		createrepo-0.9.9-22.el6.noarch
		cyrus-sasl-gssapi.x86_64
		deltarpm.x86_64
		httpd-2.2.15-47.el6_7.x86_64
		mod_ssl.x86_64
		mysql-libs.x86_64
		MySQL-python.x86_64
		ntp-4.2.6p5-5.el6_7.4.x86_64
		perl-DBI-1.609-4.el6.x86_64
		postgresql-libs.x86_64
		python-argparse-1.2.1-2.el6.noarch
		python-deltarpm.x86_64
		python-pip-7.1.0-1.el6.noarch
		python-psycopg2-2.0.14-2.el6.x86_64

		(OR)
		
	Create OS_Repo.repo in /etc/yum.repos.d/ (this we are creating because some rpms were needed for installation, this will be removed in future as the required rpms will be installed by default as part of OS installation)
		[OS_Repo]
		baseurl=http://10.62.81.179/mnt/
		enabled=1
		gpgcheck=0
		
		run command:
			yum repolist

5. Reboot for the above changes to take place
	Run command:
		reboot now
		
6. Password-less ssh for root user should be there among all hosts/node even to self

7. Copy the keys in known hosts so that it wont ask for certificate(yes/no) while doing ssh for first time

==============================================================================================================================

Steps executed for Automated Installation
-------------------------------------------

In the platform delivery package ,There will be multiple rpms and parcels,below are the rpms which involved to install CDH platform 

1. Copy all the cloudera parcels, cloudera manager rpms and cto specific rpms in /opt/cto/distribution 

	1.1	Steps for copying cloudera and cloudera manager specific parcels/rpms

		Mounting shared server(nfsshare-10.63.63.233) and copying required rpms from shared server
			Run command:
				mkdir -p /opt/cto/distribution 
				
				mount 10.63.63.233:/nemesis/Cloudera/Static/ /media

				cd /media

				cp -R *.* /opt/cto/distribution/

				umount -l /media

				mkdir -p /opt/cloudera/parcel-repo 

				cd /opt/cto/distribution/

				mv CDH* /opt/cloudera/parcel-repo/

				cd /opt/cloudera/parcel-repo
 
				chmod -R 777 .
				
	1.2 As of now, we have to manually download rpms from http://10.142.139.149:8080/job/CLoudera_Build/ and place in /opt/cto/distribution later complete package will be bundled and given.

2. Extracting all the python installation scripts and templates in /opt/cto/scripts/ location

	Run the the command:
		cd /opt/cto/distribution/

		rpm -ivh NSN-CTO-dev-script-1.0.0-47.noarch.rpm

3. Modify the host related values in platform_template.xml file under /opt/cto/conf/ directory which specifies the host on which CM and CDH components is to be installed.
	
	For single node installation give only one host in all places in platform_template.xml file.
	For multi node installation give the FQDN of hosts in the required places(like HDFS, HBase etc.) in platform_template.xml file.


4. Modify ntpserver value in platform_template.xml file under /opt/cto/conf/ directory which specifies the host on which ntp server is running.

	Currently uncomment Yarn master, slave tag in platform_template.xml


5. Execute script to start installation of CM & CDH components from /opt/cto/scripts directory
	Run the command:
		cd /opt/cto/scripts

		python setup_platform.py (currently giving some errors due to latest checkins in SVN (will be corrected) )
		
6. To update the spark-assembly jar in "/opt/cloudera/parcels/CDH-5.7.0-1.cdh5.7.0.p0.45/jars" with the custom built jar, to extract spark-thrift server jar in "/opt/cloudera/parcels/CDH-5.7.0-1.cdh5.7.0.p0.45/jars" location, to make necessary links to these jars and to make the jars executable
		Run the command: 
			cd /opt/cto/distribution

			rpm -ivh NSN-CTO-thirdparty-libs-1.0.0-47.noarch.rpm

7. To update the ngdb-hive-udf, nexr-hive-udf jars in /opt/nsn/ngdb/customudf/lib/
	Run the command:
		cd /opt/cto/distribution
		
		rpm -ivh NSN-CTO-thirdparty-udf-1.0.0-47.noarch.rpm
