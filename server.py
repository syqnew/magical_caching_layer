from socket import *
import time,os, random
import boto
from boto.s3.key import Key 
import pylibmc
import random

class Server():


  def __init__(self, cache_manager_address, client_address=('', 5000), maxClient=1):
    # start up the client socket
    #self.client_socket = socket.socket()
    #self.client_socket.bind(client_address)
    #self.client_socket.listen(maxClient)
    #print("1")
    #self.client_socket, self.client_addr = (self.client_socket.accept())
    
    # start up the manager socket
    #self.cache_manager_socket = socket.socket()
    #self.cache_manager_socket.bind(cache_manager_address)
    #self.cache_manager_socket.listen(maxClient)
    #self.cache_socket, self.cache_addr = (self.cache_manager_socket.accept())
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
      
    # connect to S3
    self.conn = boto.connect_s3()
    self.bucket = self.conn.create_bucket('magicalunicorn')

    # Populate the memcached list
    self.memcached = []
    for ip in self.cache_list:
      temp = pylibmc.Client([ip])
      self.memcached.append(temp)

  def Get(self, key):
    value = None

    # Contact all servers
    for mem in self.memcached: 
      if key in mem: # found value for key
        value = mem[key]
        break

    if not value: # value not in caching layer
    # Randomly contact a memcached server to insert
      index = random.randint(0, len(self.memcached) - 1)
      cache_machine = self.memcached[index]

    # contact S3 to get item
    k = Key(self.bucket)

    # check if key exists in S3
    possible_key = self.bucket.get_key(key) # not sure of response when key does not exist in S3

    if possible_key:
      value = k.get_contents_as_string()
      # insert value into caching layer
      cache_machine[key] = value
      # determine whether or not to perform
      self.KeepCacheKey(ip, key)

    return value

  def KeepCacheKey(self, ip, key):
    keys = self.special_instance[ip]
    keys.append(key)
    if len(keys) > 100:
      # remove keys until there is only 100
      remove_index = len(keys) - 100
      keys = keys[remove_index:]
    self.special_instance[ip] = keys
  
  def ConnectToNewCacheMachine(self, IpAddress):
    self.cache_list.append(IpAddress)
    self.memcached.append(pylibmc.Client([IpAddress]))


Stupid=Server(('18.111.82.207', 5001))
