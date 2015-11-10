import socket, time,os, random
import boto
from boto.s3.key import Key

class Server():
  def __init__(self,Adress=('',5000),MaxClient=1):
      self.s = socket.socket()
      self.s.bind(Adress)
      self.s.listen(MaxClient)

      self.conn = boto.connect_s3()
      self.bucket = self.conn.create_bucket('magicalunicorn')

  def WaitForConnection(self):
      self.Client, self.Adr=(self.s.accept())
      print('Got a connection from: '+str(self.Client)+'.')

  def ListenRequests(self):
      while True:
          data = self.Client.recv(64).decode()
          print "received data:", data
          split = data.split(",") # Assume that there is only a comma and no parens
          key = split[0]
          value = split[1]

          k = Key(self.bucket)
          
          # check if key exists
          possible_key = self.bucket.get_key(key)
          if possible_key: 
            reply = "key found"
          else:
            k.key = key
            k.set_contents_from_string(value)
            reply = "key inserted"
          print reply 


Stupid=Server()
Stupid.WaitForConnection()
Stupid.ListenRequests()
