from socket import *
import time,os, random
import boto
from boto.s3.key import Key 
import pylibmc
import random
import sys

class Server():

  def __init__(self, cache_manager_address, client_address=('', 5000), maxClient=1):
    # Setup cache_manager_socket
    self.cache_manager_socket = socket(AF_INET, SOCK_STREAM)
    self.cache_manager_socket.connect(cache_manager_address)

    # Get special memcached instance that keeps track of the last 20% of keys
    self.cache_manager_socket.send("Get_special_memcached_instance")
    special_ip = self.cache_manager_socket.recv(1024).decode()
    if not special_ip:
      print "didn't get the special memcached ip"
    else:
      print "got special memcached ip"
      self.special_instance = pylibmc.Client([special_ip], binary=False, behaviors={"cas": True})

    # Get cache machine IPs
    self.cache_list = []
    self.GetCacheList()
     
    # connect to S3
    self.conn = boto.connect_s3()
    self.bucket = self.conn.create_bucket('magicalunicorn')

    # Populate the memcached list
    self.memcached = []
    for ip in self.cache_list:
      temp = pylibmc.Client([ip])
      self.memcached.append(temp)
      self.special_instance[ip] = []


    # Client hit and miss counter
    self.hits = 0
    self.misses = 0

  def GetCacheList(self):
    self.cache_manager_socket.send("Retrieve_cache_list")
    data = self.cache_manager_socket.recv(1024).decode()
    if not data:
      print "didn't get the list"
    else:
      # print "got cache list"
      print data
      caches = data.split(",")
      new_cache_list = []
      new_memcached = []
      for cache in caches:
        new_cache_list.append(cache)
        if cache in self.cache_list:
          new_memcached.append(self.memcached[self.cache_list.index(cache)])
        else:
          new_memcached.append(pylibmc.Client([cache]))
          self.special_instance[cache] = []

      # Reassign the cache and memcached lists
      self.cache_list = new_cache_list
      self.memcached = new_memcached

      print self.cache_list
    
  def Get(self, key):
    # print key
    value = None

    deactivated_memcaches = []
    
    # Contact all servers
    for mem in self.memcached: 
      try:
        if mem.get(key): # found value for key
          #print "found key in caching layer"
          value = mem.get(key)
          self.hits = self.hits + 1
          break
      except pylibmc.Error:
        # print "Removing memcache machine"
        deactivated_memcaches.append(mem)

    # Remove deactivated_memcaches from the cache list
    for deactivated_cache in deactivated_memcaches:
      self.memcached.remove(deactivated_cache)

    if not value: # value not in caching layer
      # Randomly contact a memcached server to insert
      index = random.randint(0, len(self.memcached) - 1)
      cache_machine = self.memcached[index]

      # check if key exists in S3
      possible_key = self.bucket.get_key(int(key)) # not sure of response when key does not exist in S3

      if possible_key:
        # print key + "retrieved key from S3"
        value = possible_key.get_contents_as_string()
        # insert value into caching layer
        #cache_machine[str(key)] = value
        self.setMemcacheKey(cache_machine, str(key), value)

        # determine whether or not to perform
        self.KeepCacheKey(self.cache_list[index], key)
      # else:
        # print "key %s is not in S3" % key

        # increment miss counter
        self.misses = self.misses + 1

    return value

  def KeepCacheKey(self, ip, key):
    # print "in keep cache key"
    keys= self.special_instance[str(ip)]
    #print keys
    keys.append(key)
    if len(keys) > 100:
      # remove keys until there is only 100
      remove_index = len(keys) - 100
      keys = keys[remove_index:]
    self.special_instance[ip] = keys
  
  def ConnectToNewCacheMachine(self, IpAddress):
    self.cache_list.append(IpAddress)
    self.memcached.append(pylibmc.Client([IpAddress]))

  def UpdateHitsMisses(self):
    # update hits and misses in the special instance and reset
    print self.special_instance
    set_misses = False
    while not set_misses:
      curr_miss_value = self.special_instance.gets("misses")
      set_misses = self.special_instance.cas("misses", self.misses + curr_miss_value[0], curr_miss_value[1])

    set_hits = False
    while not set_hits:
      curr_hit_value = self.special_instance.gets("hits")
      set_hits = self.special_instance.cas("hits", self.hits + curr_hit_value[0], curr_hit_value[1])

    # reset hits and misses counters to 0
    self.hits = 0
    self.misses = 0

  def setMemcacheKey(self, client, key, value):
    value_set = False
    while not value_set:
      try:
        client[key] = value
        value_set = True
      except pylibmc.Error:
        pass


Stupid = Server(('localhost', 5003))
counter = 0
with open('wifi_data_original_half.txt', 'r') as ins: 
  start = time.time()
  for line in ins:
    # print list(line[:-2])
    Stupid.Get(line[:-2])

    # Update the cache list every 200 requests
    counter += 1
    if counter % 50 == 0:
      print line
      print time.time()
      print "Updating the cache list"
      Stupid.GetCacheList()
      Stupid.UpdateHitsMisses()
      
  end = time.time()
  print end - start
