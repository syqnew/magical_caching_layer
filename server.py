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
    # Keep track of a certain subset of keys in each cache
    self.keys_in_cache = {}
    for ip in self.cache_list:
      temp = pylibmc.Client([ip])
      self.memcached.append(temp)

      self.keys_in_cache[ip] = []


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
      self.KeepCacheValue(ip, key)

    return value

  def KeepCacheValue(self, ip, key):
    keys = self.keys_in_cache[ip]
    ran = random.random()
    if ran > 0.8:
      key.append(key)
      if len(key) > 100: 
        key = key[1:] # remove the oldest key
  
  def SendCacheListToManager(self, ip):
    # Get the key list associated with the cache machine
    keys = self.keys_in_cache[ip]
    # Get the values associated with the keys
    index = self.cache_list.index(ip)
    mc = self.memcached[index]
    key_value_pairs = mc.get_multi(keys)

    # Remove ip from cache list
    del self.cache_list[index]
    del self.memcached[index]

    # Tell cache manager to terminate the instance
    self.cache_manager_socket.send("Can_terminate_cache_instance")

    self.InsertValues(key_value_pairs)

  def InsertValues(self, key_value_pairs):
    for key, value in key_value_pairs.iteritems():
      # randomly pick a cache machine
      index = random.randint(0, len(self.memcached) - 1)
      cache_machine = self.memcached[index]
      cache_machine[key] = value

  def ConnectToNewCacheMachine(self, IpAddress):
    self.cache_list.append(IpAddress)
    self.memcached.append(pylibmc.Client([IpAddress]))

  def ListenRequests(self):
    while True:
      # listen on both sockets
      # http://stackoverflow.com/questions/15101333/is-there-a-way-to-listen-to-multiple-python-sockets-at-once
      ready_socks,_,_ = select.select([self.client_socket, self.cache_manager_socket], [], []) 
      for sock in ready_socks:
        if self.client_socket == sock: 
          key = self.client_socket.recv(64).decode()

          # assume that data is always a key
          value = self.Get(key)
        else: 
          l = self.cache_manager_socket.recv(64).decode()
          for ip in l:
            self.ConnectToNewCacheMachine(ip)

Stupid=Server(('localhost', 5001))
#Stupid.ListenRequests()
