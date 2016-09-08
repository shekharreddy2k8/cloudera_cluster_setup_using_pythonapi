#!/usr/bin/env python
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Restarts the cluster gracefully.
import sys
import os
import ConfigParser
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiService
from cm_api.endpoints.cms import ApiLicense
sys.path.insert(0,'/opt/cto/scripts')
import utils,cm_utils,ctologger
#import /opt/cto/scripts/utils.py, /opt/cto/scripts/cm_utils.py,/opt/cto/scripts/ctologger.py

def usage():
  print "Usage: managecluster.py <start|stop|restart> [service]"
  sys.exit(1)

### Do some initial prep work ###

# Prep for reading config props from external file
CONF_DIR=os.getenv("MAS_ROOT_DIR", "/opt/cto")
CONFIG = ConfigParser.ConfigParser()
#CONFIG.read(CONF_DIR+"/conf/clouderaconfig.ini")

### Set up environment-specific vars ###

# This is the host that the Cloudera Manager server is running on
#CM_HOST = CONFIG.get("CM", "cm.host")
CM_HOST = utils.getXmlValue("manager", "host")

# CM admin account info
#ADMIN_USER = CONFIG.get("CM", "admin.name")
#ADMIN_PASS = CONFIG.get("CM", "admin.password")

ADMIN_USER = utils.getXmlValue("manager", "cm_user")
ADMIN_PASS = utils.getXmlValue("manager", "cm_pass")

#### Cluster Definition #####
#CLUSTER_NAME = CONFIG.get("CM", "cluster.name")
CLUSTER_NAME = utils.getXmlValue("distribution", "cluster_name")

if sys.argv.__len__() < 2 or sys.argv.__len__() > 3 or (sys.argv[1] != "start" and sys.argv[1] != "stop" and sys.argv[1] != "restart" and  sys.argv[1] != "status") :
  usage()

CLUSTER_CMD = sys.argv[1]
SERVICE=""
if sys.argv.__len__() == 3 :
  SERVICE=sys.argv[2].upper()

CDH_VERSION = "CDH5"

### Main function ###
def main():
  API = ApiResource(CM_HOST, version=5, username=ADMIN_USER, password=ADMIN_PASS)
  APILicense = ApiLicense(API)
  enterprise=True
  if APILicense.owner == None: 
    enterprise=False

  if enterprise:
    print "Connected to CM host on " + CM_HOST + " (enterpise)"
  else:
    print "Connected to CM host on " + CM_HOST + " (express)"

  CLUSTER = API.get_cluster(CLUSTER_NAME)

  torestart = CLUSTER
  servicetype = "cluster"
  if SERVICE != "" :
    for s in CLUSTER.get_all_services():
      if s.type == SERVICE:
        torestart = s
        servicetype = SERVICE
        break
    if servicetype == "cluster":
      print "Error: could not find service "+SERVICE
      sys.exit(1)

  result=1
  if CLUSTER_CMD == "start" :
    result=start(torestart,servicetype)
  elif CLUSTER_CMD == "restart" :
    result=restart(torestart,servicetype,enterprise)
  elif CLUSTER_CMD == "stop" :
    result=stop(torestart,servicetype)
  elif CLUSTER_CMD == "status" :
    result=status(torestart,servicetype)

  if result:
    sys.exit(0)
  sys.exit(1)
   
def start(service,servicetype):
   print "About to start "+servicetype+"."
   cmd=service.start().wait()
   print "Done starting "+servicetype+"."
   return cmd.success

def stop(service,servicetype): 
   print "About to stop "+servicetype+"." 
   cmd=service.stop().wait()
   print "Done stopping "+servicetype+"."
   return cmd.success

def status(service,servicetype): 
   print "Check status of "+servicetype+"." 
   if servicetype == "cluster":
     result=service.entityStatus
     #print "Found status "+servicetype+"="+result+"."
     print result
     return True
   else:
     result=service.serviceState
     print "Found status "+servicetype+"="+result+"."
     return ("STARTED" == result) 

def restart(service,servicetype,enterprise): 
   print "About to restart "+servicetype+"."
   #rolling restart is only supported in Cloudera Enterprise
   if enterprise:
       cmd=service.rolling_restart().wait()
   else:
     cmd=service.restart().wait()
   print "Done restarting "+servicetype+"."
   return cmd.success

if __name__ == "__main__":
   main()
