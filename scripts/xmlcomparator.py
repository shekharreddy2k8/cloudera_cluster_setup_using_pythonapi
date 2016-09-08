#!/usr/bin/python
import logging
import sys
import xml.etree.ElementTree as ET
import time
import ConfigParser
######==============================Read Config File==============================######

def read_configFile():
	configParser =ConfigParser.RawConfigParser()
	configFilePath =r'/opt/cto/conf/xmlcomparatorConf.txt'
	configParser.read(configFilePath)
	file1=configParser.get('xml file location','ref_file')
	file2=configParser.get('xml file location','plat_template_file')
	file3=configParser.get('xml file location','mandFieldConf')
	return file1,file2,file3

######============================== Current Time Stamp ==========================#######
def timestamp():
	now=time.strftime("%c")
	list=now.split(' ')
	currenttime="_".join(list)
	return currenttime


def getTree(filename):
	try:
		tree=ET.parse(filename)
	except Exception as e:
		logging.error("Invalid File :"+filename)
	return tree


#####========================Get Root Tag =======================================######

def getRootTag(tree1,tree2,xmlfile1,xmlfile2):
	if tree1.getroot().tag==tree2.getroot().tag:
		logging.info("Root Element in "+xmlfile1+" and "+xmlfile2+" is same :"+tree1.getroot().tag)
		return 1
	else:
		logging.error("Root Element in "+xmlfile1+ " is "+tree1.getroot().tag+" but Root Element in "+xmlfile2+" is "+tree2.getroot().tag)
		return 0
	

#####========================Get The Number Of Child of Root Element============#######

def rootChild(root):
	count=0
	for child in root:
		count+=1
	return count

def numOfChildInRoot(r1,r2):
	count1=rootChild(r1)
	count2=rootChild(r2)

	if count1==count2:
		logging.info("Number of Child in Refrence xml and Platform xml are same")
		return 1
	elif count1 > count2:
		logging.error("Number of Child in Refrence xml is greater than Platform xml")
		return 0
	else:
		logging.error("Number of Child in Refrence xml is less than in Platform xml")
		return 0

#####========================= Check For An Element In xml File ================########

def checkElementInXml(root,elem,xmlFile):
	try:
		el=root.findall(elem)
	except:
		print("Erorrrrrrrrr!!!!!!!!!!!!!!!!!!!!!!!!!!! in Finding Element"+elem)

	if el is None:
		logging.error("Element "+elem+" not found in file "+xmlFile)
		return 0
	else:
		#print("Element "+elem+" Found in File "+xmlFile)
		return 1
#####======================== Match Root Child Names in Both xml ===============-########

def forLoop(childList1,childList2,string1,string2):
	found=""
	for c1 in childList1:
		found=False
		for c2 in childList2:
			if c1==c2:
				#print"Match Found :"+c1+" & "+c2
				found=True
		if found==False:
			#c1=ET.tostring(c1)
			logging.error(string1+" "+c1+" "+string2+" in Platform.xml")
	
		#found=False

	return found

def compareChildNames(r1,r2,root1_child_list,root2_child_list):
	for child in r1:
		root1_child_list.append(child.tag)
	for child in r2:
		root2_child_list.append(child.tag)

	int1=len(root1_child_list)
	int2=len(root2_child_list)

	if int1 > int2:
		logging.error("Missing Root Child Element in Platform.xml")
		#print("Missing Root Child Element in Platform.xml")
		flag=forLoop(root1_child_list,root2_child_list,"Element","Missing")
	
	if int2 > int1:
		logging.error("Extra Root Child Element in Platform.xml")
		#print("Extra Root Child Element in Platform.xml")
		flag=forLoop(root2_child_list,root1_child_list,"Element","Extra")
	if int1==int2:
		logging.info("Root Child Element is same in number in Both xml")
		#print("Root Child Element is same in number in Both xml")
		flag=forLoop(root2_child_list,root1_child_list,"Element","Same")

	if flag==True:
		return 1
	else:
		return 0
#####======================== Get Root Child Element Properties =================########

def getChildElementProp(root,el,tree,xmlfile):
	propList=[]
	var=el+"/property"
	#print "var : "+var
	
	checkElem=checkElementInXml(root,el,xmlfile)
	if checkElem==1:
		for elem in tree.findall(var):
			propList.append(elem.get("name"))
	else:
		logging.error(el+" not found in file :"+xmlfile)
	
	return propList

def compareChildPropNames(list1,list2,xmlfile1,xmlfile2,root1,root2,tree1,tree2):
	propList1=[]
	propList2=[]
	flag=""
	for elem in list1:
		propList1=getChildElementProp(root1,elem,tree1,xmlfile1)
		try:	
			checkElem=checkElementInXml(root2,elem,xmlfile2)
			#print("checkElem :"+str(checkElem))
			if checkElem==1:
				propList2=getChildElementProp(root2,elem,tree2,xmlfile2)
				len1=len(propList1)
				len2=len(propList2)

				if len1 > len2:
					#logging.error("Missing Property in Elemenet "+elem+" in Platform Template xml")
					flag=forLoop(propList1,propList2,"Property","Missing")
				if len2 > len1:
					#logging.error("Extra Property in Elemenet "+elem+" in Platform Template xml")
					flag=forLoop(propList2,propList1,"Property","Extra")
				if len1==len2:
					#logging.info("Number of property in element "+elem+" is same in Platform and Refrence xml")
					flag=forLoop(propList1,propList2,"Property","Same")
		
		except:
			print"Error!!!!!!!!Lookslike Element "+elem+" is missing in "+xmlfile2+" Please Check Logfile"

	if flag==True:
		return 1
	else:
		return 0
#####================================ Get Inner Child Number and Names & Comapre =============######

def compareSecondChildCount(list1,list2,xmlfile1,xmlfile2,root1,root2):
	cList1=[]
	cList2=[]
	#count1=0
	#count2=0
	#cPropList1=[]
	#cPropList2=[]
	
	flag=False
	#el="none"
	for el in list1:
		count1=0
		count2=0
		#el=s
		for child in root1.find(el):
			if child.tag!="property":
				count1+=1
				cList1.append(child.tag)
		try:
			checkEl=checkElementInXml(root2,el,xmlfile2)
			if checkEl==1:
				for child in root2.find(el):
					if child.tag!="property":
						count2+=1
						cList2.append(child.tag)
				if count1==count2:
					logging.info("Number of Children in Element "+el+" is "+str(count1)+" in both xml")
					flag=True
					#return 1
				else:
					logging.error("Number of Children in Element "+el+" in ref xml is "+str(count1)+" But Number of Children in Plat form xml in Element"+el+" is "+str(count2))
					#return 0
			else:
				continue
		except:
			print("Error!!!!!!!!!!!LooksLike Some Second Child ELement is missing in Platform  ")
	if flag==True:
		return 1
	else:
		return 0
######================================ Compare Second Child Properties=================#######
def getSecondChildProp(elem,root,subelem):
        el=subelem+"/property"
        c=root.find(elem)
        subelemproplist=[]
        for var in c.findall(el):
                if var.get("name")!=None:
                        subelemproplist.append(var.get("name"))
        return subelemproplist

######================================ Compare Second Child Names =====================######

def compareSecondChildName(list1,list2,xmlfile1,xmlfile2,root1,root2):
	#print list1
	status=""
	flag1=""
	flag2=""
	cList1=[]
	cList2=[]
	cListProp1=[]
	cListProp2=[]
	#cPropList1=[]
	#cPropList2=[]
	
	for elem in list1:
		#print "Element :"+elem
		for child in root1.find(elem):
			if child.tag!="property":
				#print child.tag
				cList1.append(child.tag)
		try:
			checkEl=checkElementInXml(root2,elem,xmlfile2)	
			if checkEl==1:
				for child in root2.find(elem):
					if child.tag!="property":
						cList2.append(child.tag)	
		except:
			logging.error("Element "+elem+" missing in xmlfile2"+xmlfile2)
		
		int1=len(cList1)
		int2=len(cList2)
		
		if int1==int2==0:
			logging.info("Number of Sub Child in element "+elem+" is 0 in both xml.This is Fine!!!!!!!!!!!!")	
		elif int1==int2:
			logging.info("Number of Sub Child in element "+elem+" is same in both xml and is equal to :"+str(int1))
			
			flag1=forLoop(cList1,cList1,"Property Name","Missing")
			#print "This is Flag1 Result :"+str(flag1)
		elif int1 > int2:
			logging.error(" Sub Child in Element "+elem+" is missing in Platform xml file.")
			flag1=forLoop(cList1,cList2,"Property Name","Missing")
		else:
			logging.error(" Sub Child in Element "+elem+" is Extra in Platform xml file.")
			flag1=forLoop(cList2,cList1,"Property Name","Extra")	
		#### Match Second Child	Properties
		try:	
			for c in cList1:
				cListProp1=getSecondChildProp(elem,root1,c)
				checkFlag=checkElementInXml(root2,c,xmlfile2)
				#print "Check Flag :"+str(checkFlag)
				if checkFlag==1:
					cListProp2=getSecondChildProp(elem,root2,c)
					cPropLen1=len(cListProp1)
					#print "Lenth 1 :"+str(cPropLen1)
					cPropLen2=len(cListProp2)
					#print "Lenth 2 :"+str(cPropLen2)
					
					if cPropLen1==cPropLen2==0:
						logging.info("Both xml has 0 subchild Properties child"+c+" in element :"+elem+" in Platform Template")
						flag2=True
					elif cPropLen1==cPropLen2:
						logging.info("Both xml has same number of subchild Properties child"+c+" in element :"+elem+" in Platform Template")
						flag2=forLoop(cListProp1,cListProp2,"Property Name","Missing")
						#print "This is Flag2 Result :"+str(flag2)
					elif cPropLen1>cPropLen2:
						logging.error("Missing Second child Properties in child "+c+" of element "+elem+" in Platform Template")
						flag2=forLoop(cListProp1,cListProp2,"Property Name","Missing")
					else:
						logging.error("Extra Second child Properties in child "+c+" of element "+elem+" in Platform Template")
						flag2=forLoop(cListProp2,cListProp1,"Property Name","Extra")
				else:
					continue
			if flag1 and flag2 is True:
				status=True
				print "Status :"+status
			else:
				status=False
				print "Status :"+status
		except:
			logging.info("")	
			#print("Element "+elem+" has no child")
		cList1=[]
		cList2=[]
				
	if status is True:
		return 1
	else:
		return 0
######================================ XML Element Values =============================#######

def getElementPropVal(root,tree,elem,elem_name,xmlfile):
	#print elem_name
	elem_name=elem_name.strip('\n')
	flag=0
	list_val=[]
	var=elem+"/property"
	#print "VARIABLE :"+var
	status=checkElementInXml(root,elem,xmlfile)
	if status==True:
		for t in tree.findall(var):
			if t.get("name")==elem_name:
				#print "Element Name:"+t.get("name")+"======="+"Matched With :"+elem_name
				value_hosts=t.get("value")
				#print value_hosts+" is Value for "+elem_name
				if value_hosts and not value_hosts.isspace():
					flag=1
					for hostval in t.get("value").split(','):
						list_val.append(hostval)
				else:
					logging.error(elem_name+" is Empty")
        
	return list_val,flag
######================================ Hive & Thrift Host Check =======================#######

def hive_thrift_host_validation(root,tree,xmlfile):
	logging.info("*****************************HIVE THRIFT HOST VALIDATION**********************************")
	list_hive_host,statusFlag1=getElementPropVal(root,tree,"hive","hosts",xmlfile)
	list_thrift_host,statusFlag2=getElementPropVal(root,tree,"thrift-sql","hosts",xmlfile)
	set_hive=set([])
	set_thrift=set([])

	for l in list_hive_host:
		set_hive.add(l)
	for l2 in list_thrift_host:
		set_thrift.add(l2)

	status1=set_hive<=set_thrift
	status2=set_hive.isdisjoint(set_thrift)
	
	if status1:
		logging.info("Hive and Thrift has correct Host Configuration")
		#print("Pass Method!!!!!!!!!!!")
		return 1
	else:
		if status2:
			logging.info("Hive and Thrift has correct Host Configuration")
			print("Pass Method!!!!!!!!!!")
			return 1
		else:
			logging.error("Hive and Thrift Host Configuration is NOT Correct")
			print("Fail Method!!!!!!!!!!")
			return 0

######================================ Mandatory Field Check ==========================#######

def mandatoryFieldCheck(root,tree,xmlfile,mandFieldConf):
	logging.info("*****************************MANDATORY ELEMENTS FILED VALIDATION**********************************")
	status=1
	list11=[]
	with open(mandFieldConf,'r') as infile:
		for line in infile:
			list1=[]
			line=line.strip('\n')
			for l in line.split('='):
				list1.append(l)			
			if list1[0]!="kerberos":

				if ',' in list1[1]:
					list1[1].strip('\n')
					list2=[]
					for l2 in list1[1].split(','):
						list2.append(l2)	
				
					for l3 in list2:
						list11,sFlag=getElementPropVal(root,tree,list1[0],l3,xmlfile)
						if sFlag==1:
							logging.info(list1[0]+"/"+l3+" is Non Empty.")
						else:
							status=0
							logging.error(list1[0]+"/"+l3+" is EMPTY.THIS IS A MANDATORY FIELD.THIS CAN NOT BE LEFT EMPTY")
				else:
					list11,sFlag=getElementPropVal(root,tree,list1[0],list1[1],xmlfile)
					if sFlag==1:
						logging.info(list1[0]+"/"+list1[1]+" is Non Empty.")
					else:
						status=0
						logging.error(list1[0]+"/"+list1[1]+" is Empty.THIS IS A MANDATORY FIELD.THIS CAN NOT BE LEFT EMPTY")
	
	return status

def kerberos_check(root,tree,xmlfile,mandFieldConf):
	#print "KERBEROS HOST VALIDATION"
	status=1
	#list1=[]
	logging.info("*****************************KERBEROS HOST VALIDATION**********************************")
	fo = open(mandFieldConf, "rw+")
	line = fo.readlines()
	for l in line:
		list1=[]
		if "kerberos" in l:
			for val in l.split("="):
				list1.append(val)
			enable_val,flag=getElementPropVal(root,tree,list1[0],"enable",xmlfile)
			#print "ENABLE : "+str(enable_val)	
			if "yes" in enable_val:
				logging.info("Kerberos ENABLED")
				#print "Kerberos ENABLED"
				if ',' in list1[1]:
					list1[1].strip('\n')
					list2=[]
					for l2 in list1[1].split(","):
						#print l2
						list2.append(l2)
					for l3 in list2:
						#print "Element Name :"+l3
						list11,sFlag=getElementPropVal(root,tree,list1[0],l3,xmlfile)
						#print "Value :"+str(list11)
						if sFlag==1:
							logging.info(list1[0]+"/"+l3+" is Non Empty.")
						else:
							status=0
							logging.error(list1[0]+"/"+l3+" is EMPTY.KERBEROS IS ENABLED.THIS FIELD CAN NOT BE LEFT EMPTY")
				else:
					list11,sFlag=getElementPropVal(root,tree,list1[0],list1[1],xmlfile)
					if sFlag==1:
						logging.info(list1[0]+"/"+list1[1]+" is Non Empty.")
					else:
						status=0
						logging.error(list1[0]+"/"+list1[1]+" is Empty.KERBEROS IS ENABLED.THIS CAN NOT BE LEFT EMPTY")
						
			else:
				logging.info("Kerberos DISABLED.SKIPPING KERBEROS Fields Value Validation")
				#print "Kerberos DISABLED"
	return status
######================================ Main Function ==================================#######
def main():
	try:
		status=0
		systime=timestamp()
		LogFileName="/opt/cto/log/"+str(systime)+"_xmlcomparator.log"
		logging.basicConfig(filename=LogFileName,level=logging.DEBUG)
		
		xmlfile1,xmlfile2,mandFieldConf=read_configFile()
		#print xmlfile1,xmlfile2	
		root1_child_list=[]
		root2_child_list=[]
		
		tree1=getTree(xmlfile1)
		root1=tree1.getroot()
		tree2=getTree(xmlfile2)
		root2=tree2.getroot()
				
		logging.info("================Calling Main===================")
		result1=getRootTag(tree1,tree2,xmlfile1,xmlfile2)
		result2=numOfChildInRoot(root1,root2)
		result3=compareChildNames(root1,root2,root1_child_list,root2_child_list)
		result4=compareChildPropNames(root1_child_list,root2_child_list,xmlfile1,xmlfile2,root1,root2,tree1,tree2)
		result5=compareSecondChildCount(root1_child_list,root2_child_list,xmlfile1,xmlfile2,root1,root2)
		result6=compareSecondChildName(root1_child_list,root2_child_list,xmlfile1,xmlfile2,root1,root2)
		result7=hive_thrift_host_validation(root2,tree2,xmlfile2)
		result8=mandatoryFieldCheck(root2,tree2,xmlfile2,mandFieldConf)		
		result9=kerberos_check(root2,tree2,xmlfile2,mandFieldConf)
		if result1 and result2 and result3 and result4 and result5 and result6 and result7 and result8 and result9 is 1:
			logging.info("========================================================================================\n                                                                                ")
			logging.info("Huraaaayyyyyyyyy!!!!!!!This template can be used for installation \nPASS ")
			logging.info("========================================================================================\n                                                                                ")
			status=1
		else:
			logging.error("========================================================================================\n                                                                                ")
			logging.error("Fail\nPlease check Platform Template xml for changes before proceeding with Installation")
			logging.error("========================================================================================\n                                                                                ")
			raise Exception("==================FAIL XML TEMPLATE CHECK=====================")
	except Exception,e:
		#logging.error("Some ERROR in XML Template")
		print("Some ERROR in XML Template.Please check xmlcomparator log in /opt/cto/log for exact error message ")
		raise Exception(e)
	#sys.stdout.write(str(status))
######================================= Calling Main Function============================#######
#main()

if __name__ == "__main__":
	main()

