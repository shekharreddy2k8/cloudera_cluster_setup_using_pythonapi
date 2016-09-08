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

# Restarts the role on a cluster gracefully.
import sys
import os
import platform
import ConfigParser
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiService
sys.path.insert(0,'/opt/cto/scripts')
import utils,cm_utils,ctologger

def usage():
  print "Usage: managenode.py <start|stop|restart|status|installed> <service>"
  sys.exit(1)

def getHost(hostname):
  return hostname.split(".")[0]

def getCmHost(api,fqdn):
  local_host = getHost(fqdn)
  host_list =  api.get_all_hosts()
  host = None
  for h in host_list:
    if fqdn == h.hostname or local_host == h.hostname or local_host == getHost(h.hostname):
      return h
  return None 

def getRole(role):
  if role == "JHS" or role == "JOBHISTORYSERVER":
    return "JOBHISTORY"
  elif role == "SHS" or role == "SPARKHISTORYSERVER" or role == "HISTORYSERVER":
    return "SPARK_YARN_HISTORY_SERVER"
  elif role == "HUE" or role == "HUESERVER":
    return "HUE_SERVER"
  elif role == "ZOOKEEPER" or role == "ZOOKEEPERSERVER" or role == "HBASEZOOKEEPER" or role == "HZOOKEEPER":
    return "SERVER"
  elif role == "HREGIONSERVER":
    return "REGIONSERVER"
  elif role == "HMASTER":
    return "MASTER"
  return role

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

if sys.argv.__len__() != 3 or (sys.argv[1] != "start" and sys.argv[1] != "stop" and sys.argv[1] != "restart" and  sys.argv[1] != "status" and  sys.argv[1] != "installed") :
  usage()

CLUSTER_CMD = sys.argv[1]
ROLE=getRole(sys.argv[2].upper())

CDH_VERSION = "CDH5"

### Main function ###
def main():
  API = ApiResource(CM_HOST, version=5, username=ADMIN_USER, password=ADMIN_PASS)
#  print "Connected to CM host on " + CM_HOST
  fqdn = platform.node()
  host = getCmHost(API,fqdn)
  if host == None:
    print "Error: "+fqdn+" is not part of cluster "+CLUSTER_NAME
    sys.exit(1)

  CLUSTER = API.get_cluster(CLUSTER_NAME)

  service = None
  role = None 
 
  if ROLE != "" :
    for s in CLUSTER.get_all_services():
      service_roles = s.get_all_roles()
      for r in service_roles:
        if r.type == ROLE and host.hostId == r.hostRef.hostId:
          service = s
          role = r
          break
      if service != None:
        break
  if role == None:
    print "Error: could not find role "+ROLE+" on "+fqdn
    sys.exit(1)
  elif CLUSTER_CMD == "installed":
    sys.exit(0)

  result=False
  if CLUSTER_CMD == "start" :
    result=start(service, role)
  elif CLUSTER_CMD == "restart" :
    result=restart(service, role)
  elif CLUSTER_CMD == "stop" :
    result=stop(service, role)
  elif CLUSTER_CMD == "status" :
    result=status(role)
  if result:
    sys.exit(0)
  sys.exit(1)

def waitForCmd(cmds):
  success = False
  for cmd in cmds:
    cmd.wait()
    success = cmd.success
  return (success == None)
   
def start(service, role):
#  print "Start "+role.name+"."
  if role.roleState == "STARTED":
    #print role+" is already started"
    return True 
  return waitForCmd(service.start_roles(role.name))

def stop(service, role): 
#  print "Stop "+role.name+"." 
  if role.roleState == "STOPPED":
    #print role+" is already stopped"
    return True
  return waitForCmd(service.stop_roles(role.name))

def restart(service, role): 
#   print "Restart "+role.name+"."
   return waitForCmd(service.restart_roles(role.name))

def status(role): 
#   print "Check status of "+role.name+"." 
   result=role.roleState
   print "Found roleState "+result
   return ("STARTED" == result) 

if __name__ == "__main__":
   main()
