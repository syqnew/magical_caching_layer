import select
from socket import *
import thread
import boto.ec2
import pylibmc
import time
import random

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
        
      if counter == 100:
            #serversocket.close()
        print "got all the conections!"
        break

  def CreateSpecialInstance(self):
    instance = self.CreateNewMemcachedInstance()
    print instance.ip_address
    self.special_instance = pylibmc.Client([instance.ip_address])
    
  def CreateNewCacheMachine(self):
    global cache_machine_ips
    instance = self.CreateNewMemcachedInstance()

    ip = instance.ip_address
    cache_machine_ips.append(ip)
    
    mc = pylibmc.Client([ip])
    self.memcached.append(mc)
    self.special_instance[ip] = []
    
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

    if average == -1:
      return

    # Check if average within range
    if average < self.hit_rate_range[0]:
      self.ExpandCachingLayer()
    elif average > self.hit_rate_range[1]:
      self.ShrinkCachingLayer()


  def ShrinkCachingLayer(self):
    global cache_machine_ips

    # if there is only one cache machine, don't terminate it
    if len(cache_machine_ips) == 1:
      return

    print "shrink cache layer"
    # Randomly pick an instance to terminate
    index = random.randint(0, len(cache_machine_ips) - 1)
    instance = cache_machine_ips[index]

    # Get recent keys from special instance
    keys = self.special_instance[instance]
    mc = self.memcached[index]
    key_value_pairs = mc.get_multi(keys)
    
    # Remove from cache_machine_ips and memcached
    del cache_machine_ips[index]
    del self.memcached[index]

    # Redistribute keys to other instances
    for key, value in key_value_pairs.iteritems():
      i = random.randint(0, len(self.memcached) - 1)
      cache_machine = self.memcached[i]
      cache_machine[key] = value

      # Update metadata
      keys_list = self.special_instance[cache_machine_ips[i]]
      keys_list.append(key)
      if len(keys_list) > 100: 
        new_index = len(keys_list) - 100
        keys_list = keys_list[new_index:]
      self.special_instance[cache_machine_ips[i]] = keys_list

    self.special_instance.delete([instance])

    # Terminate instance
    self.conn.terminate_instances([instance])

  def ExpandCachingLayer(self):
    global cache_machine_ips
    print "expand cache layer"

    # Calculate that number
    num_keys = 100 / len(cache_machine_ips)

    # Create a new cache machine - side effect adds the ip to cache list already
    self.CreateNewCacheMachine()

    new_instance = cache_machine_ips[-1]
    new_keys = []
    # Query all active cache machines for a certain number
    for index in range(0, len(cache_machine_ips) - 2):
      key_list = self.special_instance[cache_machine_ips[index]]
      key_value_pairs = self.memcached[index].get_multi(key_list)
      i = 0
      for key, value in key_value_pairs.iteritems():
        new_instance[key] = value
        new_keys.append(key)
        
        i += 1
        if i == num_keys:
          break
    self.special_instance[new_instance] = new_keys

cache_manager = CacheManager(100, (.8, .9), 1)
# periodically ping the cache machines
while True:
 cache_manager.AlterCachingLayer()
 time.sleep(5) # wait five seconds

