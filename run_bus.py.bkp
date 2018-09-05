#!/usr/bin/env python
# -*- encoding: utf-8 -*-
## Created by Pelezinho
##    16/05/2018

import sys
import os
from zabbix_api import ZabbixAPI
import csv
import re
from zabbix_sender
import time
import subprocess
import json

class BusController:

   def __init__(self, host, parameterFile, reportDir, MQ, MQPort, ip):
          self.busHost = host
          self.parameter = parameterFile
          self.busReportDir = reportDir
          self.queueManager = MQ
          self.queuePort = MQPort
          self.queueIP = ip
          
   def exist(self, zapi, host):
   
       items = zapi.item.get({
           "output": "extend",
           "host": host,
       })
   
       if items is None or len(items) == 0:
           print("Cannot find item {}".format(host))
           exit(4)
       else:
   	   print(list(map(lambda x: x['key_'], items)) , host)
           return (list(map(lambda x: x['key_'], items)))
   
   def param(self, item):
       if not re.findall(r'^.*\[', item):
           return None
       else:
           return re.search('.*\[(.*)\]', item).group(1)

   def discovery(self, metric, name):
       data = [{"{#" + metric.upper() + "_NAME}": name}]
       json_obj = (json.dumps({"data": data}, indent=4))
       print(json_obj)
       sender = self.sender_object()
       sender.add("Template BUS", "lld_bus" , json_obj)
 
   def sender_object(self):
       ZABBIX_HOST = "10.55.0.204"
       ZABBIX_PORT = 10051

       return ZabbixSender(ZABBIX_HOST, ZABBIX_PORT)
 
   def runPlugin(self):
         print "*** Running BUS stats..."
 
         cmdline = ["java -cp "
                    "/opt/perfcenter/monitoring_bus/bus-1.0.0/BUS_PLUGIN_RECURSOS.jar",
                    "BusResourceMonitor",
                    self.queueIP, self.queuePort, self.queueManager, "SDXUSERS.SVRCONN", "BUS_RESOURCE_SNAPSHOT",
                    self.busReportDir + "/", "1"]
 
         print (' '.join(cmdline))
         os.system(' '.join(cmdline))
 
  	 #proc = subprocess.Popen(["pgrep", cmdline], stdout=subprocess.PIPE) 

   def parseReport(self, item, host, reportDir):
   
       path = reportDir + "/stats-bus-server-"
       files = ["threads", "jvm"]
   
       for i in files:
           with open(path + i + ".csv") as file:
               rownum = 0
               reader = csv.reader(file)
               for row in reader:
                   if rownum == 0 or header == row:
                       header = row
                   else:
                       for col in row:
                           self.discovery(i, col.split(";")[3])
                           if col.find(item) >= 0 and col.find(host) >= 0:
                               result = self.switch(i, col)
                               self.send_data(result, i, item, host)
                   rownum += 1
   
   def switch(self, i, col):
       case = {}
       
       if i == "threads":
          case['threads'] = [int(col.split(";")[0])/1000, col.split(";")[-1]]
       
       if i == "jdbc":
          case['jdbc'] = [int(col.split(";")[0])/1000, col.split(";")[-1]]

       if i == "jvm":
          case['jvm'] = [int(col.split(";")[0])/1000, col.split(";")[4], col.split(";")[5], col.split(";")[-1]]

       return case.get(i)

   def send_data(self, result, metric, param, host):
       if result != None:
           sender = self.sender_object()
   
           if metric == "threads":
              sender.add(host, "threads_useds[" + param + "]" , result[1], result[0])
   
           if metric == "jdbc":
              sender.add(host, "jdbc_max[" + param + "]" , result[4], result[0])
              sender.add(host, "jdbc_used" + param + "]" , result[5], result[0])

           if metric == "jvm":
              sender.add(host, "jvm_max[" + param + "]" , result[1], result[0])
              sender.add(host, "jvm_used[" + param + "]" , result[2], result[0])
              sender.add(host, "gc_time[" + param + "]" , result[3], result[0])
   
           sender.send()
   
   def load_properties(self, propertieFile, sep='=', comment='#'):
       props = {}
       with open(propertieFile, "rt") as f:
          for line in f:
	     l = line.strip()
             if l and not l.startswith(comment):
                  key_value = l.split(sep)
 		  key = key_value[0].strip()
                  value = sep.join(key_value[1:]).strip().strip('"')
                  props[key] = value

       return props
   
   def run(self):
       properties = self.load_properties(self.parameter)
       
       zabbixServerHost = properties.get("zabbix_server") 
       zapi = ZabbixAPI(server="https://" + zabbixServerHost + "/zabbix")
       user = properties.get('username')
       passwd = properties.get('password')
       zapi.login(user, passwd)

       self.runPlugin()
   
       lista = self.exist(zapi, host)
       if lista is not None:
           for i in lista:
               vu = self.param(i)
               if vu is None: continue
               else: self.parseReport(vu, host, self.busReportDir)
   
       exit(0) 

if __name__ == '__main__':
     host = sys.argv[1]
     parameterFile = sys.argv[2]
     reportDir = sys.argv[3]
     queue = sys.argv[4]
     port = sys.argv[5]
     ip = sys.argv[6]

     controller = BusController(host, parameterFile, reportDir, queue, port, ip)
     controller.run()

