import utils


def getCMHost():
	return utils.getXmlValue("manager", "host")

def getClusterName():
	return utils.getXmlValue("distribution", "cluster_name")

def getYarnServiceName():
	return utils.getXmlRole("yarn")

def getSparkServiceName():
	return utils.getXmlRole("spark")

def getSparkHosts():
	return utils.getXmlHostlist("spark", "hosts")

def getSparkServiceConfig():
	return utils.getXmlConf("spark","spark-service-config")

def getSparkHSConfig():
	return utils.getXmlConf("spark","history-server-config")

def getSparkthriftHosts():
	return utils.getXmlHostlist("sparkthrift", "hosts")

def getSparkThriftServiceName():
	return utils.getXmlRole("sparkthrift")

def getSparkThriftConfig():
	return utils.getXmlConf("sparkthrift","sparkthrift-service-config")


