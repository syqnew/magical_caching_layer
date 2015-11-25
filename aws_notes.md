Managing the EC2 Instances Notes
================================

Initializing the Cache Instances
--------------------------------

Create an Ubuntu EC2 instance with the very loose security settings of allowing all traffic from all ports.

Install memcached using 
  
  sudo apt-get install memcached

 memcached is automatically started. However, it is just listening to localhost. 
 Go to the configuration file for memcached at /etc/memcached.conf and comment out the line

  -l 127.0.0.1

Restart the memcached server using

  sudo service memcache restart

to load the new configuration file.

Cache Manager Instance
----------------------
Need to install pip

  sudo apt-get install python-pip

Need to install libmemcahed in order to get pylibmc

  sudo apt-get install libmemcached10

Need to install the proper python packages

  sudo pip install boto
  sudo apt-get install python-pylibmc

Add the access keys in ~/.boto. It looks like this

  [Credentials]
  aws_access_key_id = {ACCESS KEY ID}
  aws_secret_access_key = {SECRET ACCESS KEY}

  
