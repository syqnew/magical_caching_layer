import socket, time,os, random
import boto
from boto.s3.key import Key 
import pylibmc

class Server():


  def __init__(self, cache_manager_address, client_address=('', 5000), maxClient=1):
    # start up the client socket
    #self.client_socket = socket.socket()
    #self.client_socket.bind(client_address)
    #self.client_socket.listen(maxClient)
    #print("1")
    #self.client_socket, self.client_addr = (self.client_socket.accept())
    
    print("here")

    # start up the manager socket
    #self.cache_manager_socket = socket.socket()
    #self.cache_manager_socket.bind(cache_manager_address)
    #self.cache_manager_socket.listen(maxClient)
    #self.cache_socket, self.cache_addr = (self.cache_manager_socket.accept())
    self.cache_manager_socket = socket.socket()
    self.cache_manager_socket.connect(cache_manager_address)
    

    print("connected")
    # list of all the caches
    self.cache_list = []

    # connect to S3
    self.conn = boto.connect_s3()
    self.bucket = self.conn.create_bucket('magicalunicorn')

    # Grabbing IP address of all the caches
    self.cache_manager_socket.send("Retrieve_cache_list")
    caches_initialized = False
    while not caches_initialized:
      data = self.cache_manager_socket.recv(64).decode()
      caches = data.split(",")
      for cache in caches:
        self.cache_list.append(cache)
      if data != None:
        caches_initialzied = True

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

    return value


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

Stupid=Server(('52.33.107.185', 5500))
#Stupid.ListenRequests()
