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
           "output": "extend",
           "filter": {
                   "host": host
           }
       })
       
       print(host_)
       
       if host_ is None:
          raise Exception(host + " não criado")
          exit(4)
       else:
          items = zapi.item.get({
              "output": "extend",
              "host": host,
          })
#          if items is None or len(items) == 0:
#                raise("Cannot find item {}".format(host))
#                exit(4)
#          else:
          print(list(map(lambda x: x['key_'], items)) , host)
          return (list(map(lambda x: x['key_'], items)))
   
   def param(self, item):
       if not re.findall(r'^.*\[', item):
           return None
       else:
           return re.search('.*\[(.*)\]', item).group(1)

   def discovery(self, zapi, header, name, host, application):
        keys_in_host = []
        columns = header.split(";")
        details_to_create = zapi.host.get({
                             "output": ["hostid","interfaceids"],
                             "selectItems": "extend",
                             "filter": {
                                    "host": host
                             }
                })
 
        for x in details_to_create[0]['items']:
            interfaceid = x['interfaceid']
            hostid = x['hostid']
            keys_in_host.append(x['key_'])
 
        for metric in columns[4:]:
            ref_key = metric.replace(" ","_").lower() + "[" + name + "]"
            if ref_key not in keys_in_host:
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
                  print(item_id)
 #                  except:
 #                     print("Não foi possível criar o item %s" % (ref_key))
 
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
 
  	 #proc = subprocess.Popen(["pgrep", cmdline], stdout=subprocess.PIPE) 

   def parseReport(self, item, host, reportDir, zapi):
   
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
                           self.discovery(zapi, str(header).replace("']",""), col.split(";")[3], host, i)
                           if col.find(item) >= 0 and col.find(host) >= 0:
                               result = self.switch(i, col)
                               self.send_data(result, i, item, host)
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
           print("Send data...")
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
   
       lista = self.exist(zapi, host)
       if lista is not None:
           for i in lista:
               value = self.param(i)
               if value is None: continue
               else: self.parseReport(value, host, self.busReportDir, zapi)
   
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

