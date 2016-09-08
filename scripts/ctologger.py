import logging

def getCdhLogger():
	logger = logging.getLogger("CDHLOGGER")
	if len(logger.handlers) == 0:
		logger = setConsoleFormat(logger)
		logger = setFileFormat(logger)
		logger.setLevel(logging.DEBUG)
	return logger

def setConsoleFormat(logger):
	consoleHandler = logging.StreamHandler()
	consoleHandler.setLevel(logging.DEBUG)
	formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
	consoleHandler.setFormatter(formatter)
	logger.addHandler(consoleHandler)
	return logger

def setFileFormat(logger):
	formatter = logging.Formatter('%(asctime)s %(filename)s:%(lineno)s %(levelname)s %(message)s')
	fileHandler = logging.FileHandler('/opt/cto/log/cdhInstallation.log')
	fileHandler.setLevel(logging.DEBUG)
	fileHandler.setFormatter(formatter)
	logger.addHandler(fileHandler) 
	return logger
