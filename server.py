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
    self.special_instance = pylibmc.Client([special_ip])

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

  # TODO: This does not update the cache list and memcached lists
  def GetCacheList(self):
    self.cache_manager_socket.send("Retrieve_cache_list")
    data = self.cache_manager_socket.recv(1024).decode()
    if not data:
      print "didn't get the list"
    else:
      print "got cache list"
      print data
      caches = data.split(",")
      for cache in caches:
        self.cache_list.append(cache)
    
    
  def Get(self, key):
    value = None

    deactivated_memcaches = []
    
    # Contact all servers
    for mem in self.memcached: 
      try:
        if mem.get(key): # found value for key
          print "found key in caching layer"
          value = mem.get(key)
          break
      except pylibmc.Error:
        print "Removing memcache machine"
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
        print key + "retrieved key from S3"
        value = possible_key.get_contents_as_string()
        # insert value into caching layer
        print sys.getsizeof(value)
        cache_machine[str(key)] = "poop" #value

        # determine whether or not to perform
        self.KeepCacheKey(self.cache_list[index], key)
      else:
        print "key %s is not in S3" % key

    return value

  def KeepCacheKey(self, ip, key):
    print "in keep cache key"
    keys= self.special_instance[str(ip)]
    keys.append(key)
    if len(keys) > 100:
      # remove keys until there is only 100
      remove_index = len(keys) - 100
      keys = keys[remove_index:]
    self.special_instance[ip] = keys
  
  def ConnectToNewCacheMachine(self, IpAddress):
    self.cache_list.append(IpAddress)
    self.memcached.append(pylibmc.Client([IpAddress]))


Stupid = Server(('localhost', 5001))
counter = 0
with open('wifi_data_original.txt', 'r') as ins: 
  for line in ins:
    print list(line[:-2])
    Stupid.Get(line[:-2])

    # Update the cache list every 200 requests
    counter += 1
    if counter % 200 == 0:
      print "Updating the cache list"
      Stupid.GetCacheList()
