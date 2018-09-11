#!/usr/bin/env python
# -*- encoding: utf-8 -*-
## Created by Pelezinho
##    16/05/2018

import sys
import os
import string
from zabbix_api import ZabbixAPI
import csv
import re
from sender import ZabbixSender
import time
import subprocess
import json
import threading

class BusController:

   def __init__(self, host, parameterFile, reportDir, MQ, MQPort, ip):
          self.busHost = host
          self.parameter = parameterFile
          self.busReportDir = reportDir
          self.queueManager = MQ
          self.queuePort = MQPort
          self.queueIP = ip
          
   def exist(self, zapi, host):
   
       host_ = zapi.host.get({
           "output": ["hostid"],
           "filter": {
                   "host": host
           }
       })
       
       print(host_)
       if not host_:
          raise Exception("%s não existe" % host)
          exit(4)
       else:       
          return int(host_[0]['hostid'])

   def param(self, item):
       if not re.findall(r'^.*\[', item):
           raise("Cannot find item {}")
           exit(4)
       else:
           return re.search('.*\[(.*)\]', item).group(1)

   def items_find(self, zapi, metric, host, name):
        
            name_ = string.capwords(name) + " " + metric.lower()
        
            return zapi.item.get({
                  "output": ["key_"],
                  "filter": {
                         "host": host,
                         "name": name_
                  }
            })

   def discovery(self, zapi, metric, name, host, hostid):

           details_to_create = zapi.host.get({
                                "selectInterfaces": ["interfaceid"],
                                "filter": {
                                       "host": host
                                }
                   })
           
           interfaceid = details_to_create[0]['interfaces'][0]['interfaceid']
           ref_key = metric.replace(" ","_").lower() + "[" + name + "]"

           item_id = zapi.item.create({
                           "name": string.capwords(name) + " " + metric.lower(),
                           "key_": ref_key,
                           "hostid": hostid,
                           "type": 2,
                           "value_type": 3,
                           "interfaceid": interfaceid,
                           "history": "90",
                           "trends": "365"
                 })
           print("Item criado com id %d" % int(item_id['itemids'][0]))
 
   def sender_object(self):
       ZABBIX_HOST = "localhost"
       ZABBIX_PORT = 10051

       return ZabbixSender(ZABBIX_HOST, ZABBIX_PORT)
 
   def runPlugin(self):
         print "*** Running BUS stats..."
 
         cmdline = ["java -cp "
                    "/opt/perfcenter/monitoring/bus/BUS_PLUGIN_RECURSOS.jar",
                    "BusResourceMonitor",
                    self.queueIP, self.queuePort, self.queueManager, "SYSTEM.DEF.SVRCONN", "ALE.QL.RESOURCESTATS",
                    self.busReportDir + "/", "1"]
 
         print (' '.join(cmdline))
         os.system(' '.join(cmdline))

   def metrics(self, header):
        columns = header.split(";")
        metrics = []
        for metric in columns[4:]:
           metrics.append(metric)

        return metrics

   def parseReport(self, host, hostid, reportDir, zapi):
   
       path = reportDir + "/stats-bus-server-"
       files = ["threads", "jvm"]
   
       for i in files:
           with open(path + i + ".csv") as file:
               rownum = 0
               reader = csv.reader(file)
               for row in reader:
                      if rownum == 0 or header == row:
                          header = row
                      elif any(host in s for s in row):
                             for col in row:
                                 name = col.split(";")[3]
                                 lista_de_metricas = self.metrics(str(header).replace("']",""))
                                 for metric in lista_de_metricas:
                                     item = self.items_find(zapi, metric, host, name)
                                     if not item:
                                          self.discovery(zapi, metric, name, host, hostid)
                                     elif col.find(name) >= 0 and col.find(host) >= 0:
                                          result = self.switch(i, col)
                                          self.send_data(result, i, name, host)
                      rownum += 1

   def switch(self, i, col):
       case = {}

       if i == "threads":
          case['threads'] = [int(col.split(";")[0])/1000, col.split(";")[-1]]

       if i == "jdbc":
          case['jdbc'] = [int(col.split(";")[0])/1000, col.split(";")[4], col.split(";")[-1]]

       if i == "jvm":
          case['jvm'] = [int(col.split(";")[0])/1000, col.split(";")[4], col.split(";")[5], col.split(";")[-1]]

       return case.get(i)

   def send_data(self, result, metric, param, host):
       if result != None:
           sender = self.sender_object()
           print "*** Send data..."
           if metric == "threads":
              sender.add(host, "threads_used[" + param + "]" , result[1], result[0])

           if metric == "jdbc":
              sender.add(host, "max_size_pool[" + param + "]" , result[1], result[0])
              sender.add(host, "actual_size_pool[" + param + "]" , result[2], result[0])

           if metric == "jvm":
              if result[1] == "-1":
                 result[1] = 512

              sender.add(host, "max_memory_in_mb[" + param + "]" , result[1], result[0])
              sender.add(host, "used_memory_in_mb[" + param + "]" , result[2], result[0])
              sender.add(host, "cumulative_gc_time_in_seconds[" + param + "]" , result[3], result[0])

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
       zapi = ZabbixAPI(server="http://" + zabbixServerHost)
       user = properties.get('username')
       passwd = properties.get('password')
       zapi.login(user, passwd)

       self.runPlugin()
       hostid = self.exist(zapi, host)
       if hostid > 0:
         self.parseReport(host, hostid, self.busReportDir, zapi)
   
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

