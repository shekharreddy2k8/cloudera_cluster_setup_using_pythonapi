import time, utils, ctologger

logger = ctologger.getCdhLogger()

PARCEL_VERSION = utils.getXmlValue("distribution", "parcel_version")
PARCEL_NAME = "CDH"

def activate_parcel(cluster):
	parcel = cluster.get_parcel(PARCEL_NAME,PARCEL_VERSION)
	parcels = cluster.get_all_parcels()
	active_parcel_available = False
	
	for p in parcels:
		if p.stage == "ACTIVATED" and p.version < parcel.version and p.version != parcel.version:
			logger.info("Upgrading the %s from Parcel version %s to %s" % (p.product,p.version,parcel.version))
			cluster.upgrade_cdh(cdh_parcel_version=PARCEL_VERSION,rolling_restart=True)
			active_parcel_available = True
			break
		elif p.stage == "ACTIVATED" and parcel.version < p.version:
			logger.info("Already higher version %s of the %s Parcel Active compared to %s" % (p.version,p.product,parcel.version))
			return 0

	if active_parcel_available == False:
		logger.info("Activating the %s Parcel version %s" % (parcel.product,parcel.version))
		parcel.activate()

	while True:
		parcel = cluster.get_parcel(PARCEL_NAME,PARCEL_VERSION)
		if "ACTIVATED" in parcel.stage:
			logger.info("Activated the %s Parcel version %s state %s" % (parcel.product,parcel.version,parcel.stage))
			return 0
		elif parcel.state.errors:
			logger.error("Failed to activate the %s parcel %s with error %s !!!" % (parcel.product,parcel.version,parcel.state.errors))
			raise Exception(str(p.state.errors))

		logger.info( "Activating %s: %s / %s" % (parcel.product,parcel.state.progress, parcel.state.totalProgress))
		time.sleep(20)

def distribute_parcel(cluster):
	parcel = cluster.get_parcel(PARCEL_NAME, PARCEL_VERSION)
	parcel.start_distribution()
	while True:
		parcel = cluster.get_parcel(PARCEL_NAME, PARCEL_VERSION)
		if parcel.stage == "DISTRIBUTED":
			res = activate_parcel(cluster)
			return res
		if parcel.state.errors:
			logger.error("Failed to distribute the %s parcel %s with error %s !!!" % (parcel.product,parcel.version,parcel.state.errors))
			raise Exception(str(parcel.state.errors))
			
		logger.info( "Distributing %s: %s / %s" % (parcel.product,parcel.state.progress, parcel.state.totalProgress))
		time.sleep(20)

# Downloads and distributes parcels
def deploy_parcels(cluster):
	logger.info('Inside deploy_parcels.')
	res = 0
	if len(PARCEL_VERSION) == 0:
		logger.error("Parcel Version from XML is Empty!!")
		return -1
		
	p = cluster.get_parcel(PARCEL_NAME, PARCEL_VERSION)
	logger.info('p.stage: '+p.stage)

	if p.state.errors:
		logger.error("Failed to deploy the %s parcel %s with error %s !!!Please correct the Error!!" % (p.product,p.version,p.state.errors))
		raise Exception(str(p.state.errors))
	
	if "ACTIVATED" == p.stage :
		logger.info("%s Parcel %s is already Activated !!!" % (p.product,p.version))
		
	elif p.stage == "DISTRIBUTED":
		logger.info("parcels already distributed")
		res = activate_parcel(cluster)
		
	elif  p.stage == "DOWNLOADED":
		logger.info( "parcels already downloaded")
		res = distribute_parcel(cluster)
		
	elif p.stage == "AVAILABLE_REMOTELY":
		logger.info( "Downloading %s: %s / %s" % (p.product,p.state.progress, p.state.totalProgress))
		p.start_download()
		while True:
			p = cluster.get_parcel(PARCEL_NAME,PARCEL_VERSION)
			if p.stage == "DOWNLOADED":
				res = distribute_parcel(cluster)
				break
			if p.state.errors:
				logger.error("Failed to download the %s parcel %s with error %s !!!" % (p.product,p.version,p.state.errors))
				raise Exception(str(p.state.errors))
			
			logger.info( "Downloading %s: %s / %s" % (p.product,p.state.progress, p.state.totalProgress))
			time.sleep(20)
	else:
		while p.stage != "ACTIVATED" and p.stage != "DISTRIBUTED" and p.stage != "DOWNLOADED":
			time.sleep(20)
			p = cluster.get_parcel(PARCEL_NAME, PARCEL_VERSION)
			
		deploy_parcels(cluster)
	
	return res
