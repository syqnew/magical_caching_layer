import socket
import boto
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
      self.conn = boto.ec2.connect_to_region("us-west-2b")

      # cache machine ips
      self.cache_machine_ips = []
      self.memcached = []
      self.worst_hit_rate = None
      self.CreateNewCacheMachine()

      self.hit_cache_range = hit_cache_range #tuple

    # Broadcasting the cache list to client servers
    def sendCacheList(self):
      for server_socket in self.server_sockets:
        server_socket.send(self.cache_list.endcode())


# TODO: Need to add in logic to receive the "send list message" and broadcast cache list to servers
    def ListenOnSockets(self):


    def CreateNewCacheMachine(self):
      # Create script to run on instance
      script = "sudo apt-get install memcached\n sudo sed -i '35s/.*/# -l 127.0.0.1/' /etc/memcached.conf \n sudo service memcache restart"
      # Create a new cache instance
      reservation = self.conn.run_instances('ami-5189a661', key_name='unicorn', instance_type='t1.micro', security_groups=['launch-wizard-6'], user_data=script)

      instance = reservation.instances[0]

      while instance.update() != running:
        time.sleep(5) # wait for five seconds

      ip = instance.ip_address
      self.cache_machine_ips.append(ip)
      self.memcached.append(pylibmc.Client([ip]))

  def TerminateCacheMachine(self):
  # Determine which cache machine to terminate

  def GetAverageHitRate(self):
    averages = []

    for mem in self.memcached: 
      stats = mem.get_stats()  
      hits = stats[0][1]["get_hits"]
      misses = stats[0][1]["get_misses"]
      hit_rate = hits / (hits +  misses)
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

