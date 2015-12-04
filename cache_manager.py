import select
from socket import *
import thread
import boto.ec2
import pylibmc
import time

cache_machine_ips = []

def handler(clientsocket, clientaddr):
    global cache_machine_ips
    while True:
        data = clientsocket.recv(1024)
        if not data:
            break
        else:
            print data
            msg = ','.join(cache_machine_ips)
            clientsocket.send(msg)
    clientsocket.close()


class CacheManager():
    
  def __init__(self, client_count, hit_cache_range, max_client=1):
    self.cache_list = []
    self.server_sockets = []

    # connect to EC2
    print "hi"
    self.conn = boto.ec2.connect_to_region("us-west-2")

    # cache machine ips
    self.cache_machine_ips = []
    self.memcached = []
    self.ip_to_memcached = {}
    self.cache_instance_ids = {}


    #print "runnign test"
    #ip = "52.35.8.106"
    #mc = pylibmc.Client([ip])
    #print mc.get_stats()
    
    self.special_instance = None
    self.CreateSpecialInstance()

    self.CreateNewCacheMachine()
    print "Created a cache"

    self.hit_rate_range = hit_cache_range #tuple
  
    # Creating sockets to connect to the servers
    host = ''
    port = 5001
    buf = 1024
    addr = (host, port)
    serversocket = socket(AF_INET, SOCK_STREAM)
    serversocket.bind(addr)
    serversocket.listen(client_count)
  
    counter = 0
    while True:
      print "Server is listening for connections\n"

      clientsocket, clientaddr = serversocket.accept()
      thread.start_new_thread(handler, (clientsocket, clientaddr))
      counter = counter + 1
        
      if counter == 2:
            #serversocket.close()
        print "got all the conections!"
        break

  def CreateSpecialInstance(self):
    instance = self.CreateNewMemcachedInstance()
    self.special_instance = pylibmc.Client([instance.ip_address])
    
  def CreateNewCacheMachine(self):
    global cache_machine_ips
    instance = self.CreateNewMemcachedInstance()

    ip = instance.ip_address
    cache_machine_ips.append(ip)
    
    mc = pylibmc.Client([ip])
    self.memcached.append(mc)
    self.ip_to_memcached[ip] = mc 
    self.cache_instance_ids[ip] = instance.id
    
  def CreateNewMemcachedInstance(self):
    # Create script to run on instance
    script = "#!/bin/bash\nsudo apt-get install memcached\nsudo sed -i '35s/.*/# -l 127.0.0.1/' /etc/memcached.conf\nsudo service memcached restart"
    # Create a new cache instance
    reservation = self.conn.run_instances('ami-5189a661', key_name='unicorn', instance_type='t2.micro', security_groups=['launch-wizard-6'], user_data=script)

    instance = reservation.instances[0]

    while instance.update() != 'running':
      time.sleep(10) # wait for five seconds

    time.sleep(60)
    return instance

  def TerminateCacheMachine(self, num_keys):
    global cache_machine_ips
    # Need at least one machine in the cache
    #if len(self.cache_machine_ips) == 1:
    if len(cache_machine_ips) == 1:
      return

    ip_with_lowest_hit_rate = None
    hit_rate = 2
    # Determine which cache machine to terminate
    #for ip in self.cache_machine_ips:
    for ip in cache_machine_ips:
      mc = self.ip_to_memcached[ip]
      stats = mc.get_stats()
      hr = self.CalculateHitRate(stats)
      if hr < hit_rate:
        hit_rate = hr
        ip_with_lowest_hit_rate = ip

    # Get instance id of that cache machine
    instance = self.cache_instance_id[ip_with_lowest_hit_rate]

    # TODO: figure out how to random select num_keys from the terminated instance

    # Terminate the instance
    self.conn.terminate_instances([instance])

  def CalculateHitRate(self, stats):
    hits = float(stats[0][1]["get_hits"])
    misses = float(stats[0][1]["get_misses"])
    if (hits + misses) == 0:
      return -1
    hit_rate = hits / (hits +  misses)
    return hit_rate

  def GetAverageHitRate(self):
    averages = []

    for mem in self.memcached:
      print mem
      stats = mem.get_stats()  
      hit_rate = self.CalculateHitRate(stats)
      if hit_rate >= 0:
        averages.append(hit_rate)

    # compute the average
    if len(averages) > 0:
      return sum(averages) / len(averages)
    else:
      return -1

  def AlterCachingLayer(self):
    # Get average
    average = self.GetAverageHitRate()

    # Check if average within range
    if average < self.hit_rate_range[0]:
      self.ExpandCachingLayer()
    elif average > self.hit_rate_range[1]:
      self.ShrinkCachingLayer()


  def ShrinkCachingLayer(self):
    print "shrink cache layer"

  def ExpandCachingLayer(self):
    print "expand cache layer"


cache_manager = CacheManager(1, (.8, .9), 1)
# periodically ping the cache machines
while True:
 cache_manager.AlterCachingLayer()
 time.sleep(5) # wait five seconds


### TODO: Rebalancing -- expand/shrink cache
#test()

