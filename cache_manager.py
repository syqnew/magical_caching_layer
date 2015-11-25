import socket, select
import boto.ec2
import pylibmc
import time


class CacheManager():

  def __init__(self, client_count, hit_cache_range, max_client=1):
    self.cache_list = []
    self.server_sockets = []

    # Open server sockets for all the client servers
    for port_num in xrange(5500, 5500+client_count, 1):
      server_socket = socket.socket()
      server_socket.bind(('', port_num))
      server_socket.listen(max_client)
      server_socket.accept()
      server_sockets.append(server_socket)

      # connect to EC2
      self.conn = boto.ec2.connect_to_region("us-west-2")

      # cache machine ips
      self.cache_machine_ips = []
      self.memcached = []
      self.ip_to_memcached = {}
      self.CreateNewCacheMachine()
      self.cache_instance_ids = {}

      self.hit_cache_range = hit_cache_range #tuple

    # Broadcasting the cache list to client servers
    def sendCacheList(self):
      for server_socket in self.server_sockets:
        server_socket.send(self.cache_list.endcode())

    def ListenOnSockets(self):
      ready_socks, _, _ = select.select(self.server_sockets, [], [])
      for sock in ready_socks:
        message = sock.recv(64).decode()
        if message == "Retrieve_cache_list":
          # send over the cache list
          sock.send(self.cache_list.encode())

    def CreateNewCacheMachine(self):
      # Create script to run on instance
      script = "#!/bin/bash\nsudo apt-get install memcached\nsudo sed -i '35s/.*/# -l 127.0.0.1/' /etc/memcached.conf\nsudo service memcached restart"
      # Create a new cache instance
      reservation = self.conn.run_instances('ami-5189a661', key_name='unicorn', instance_type='t2.micro', security_groups=['launch-wizard-6'], user_data=script)

      instance = reservation.instances[0]

      while instance.update() != 'running':
        time.sleep(5) # wait for five seconds

      ip = instance.ip_address
      self.cache_machine_ips.append(ip)
      mc = pylibmc.Client([ip])
      self.memcached.append(mc)
      self.ip_to_memcached[ip] = mc 
      self.cache_instance_ids[ip] = instance.id

  def TerminateCacheMachine(self, num_keys):
    # Need at least one machine in the cache
    if len(self.cache_machine_ips) == 1: 
      return

    ip_with_lowest_hit_rate = None
    hit_rate = 2
    # Determine which cache machine to terminate
    for ip in self.cache_machine_ips:
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
    hits = stats[0][1]["get_hits"]
    misses = stats[0][1]["get_misses"]
    hit_rate = hits / (hits +  misses)
    return hit_rate

  def GetAverageHitRate(self):
    averages = []

    for mem in self.memcached: 
      stats = mem.get_stats()  
      hit_rate = self.CalculateHitRate(stats)
      averages.append(hit_rate)

    # compute the average
    return reduce(lambda x, y: x + y, averages) / len(averages)

  def AlterCachingLayer(self):
    # Get average
    average = self.GetAverageHitRate()

    # Check if average within range
    if average < self.hit_rate_range[0]:
      self.ExpandCachingLayer()
    else if average > self.hit_rate_range[1]:
      self.ShrinkCachingLayer()


  def ShrinkCachingLayer(self):
    print "shrink cache layer"

  def ExpandCachingLayer(self):
    print "expand cache layer"


cache_manager = CacheManager()
# periodically ping the cache machines
  while True:
    cache_manager.AlterCachingLayer()
    time.sleep(5) # wait five seconds


### TODO: Rebalancing -- expand/shrink cache

