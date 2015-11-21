import socket
import sys

class Client():
   def __init__(self,Adress=("18.189.106.35", 5000)):
      self.s = socket.socket()
      self.s.connect(Adress)
'''
   def send_message(self, msg):
      self.s.send(msg.encode())
'''

   def get(key):
      self.s.send(key.encode())
      # prints out to standard output

def main():
   TC=Client()

   while True:
      value = input("Enter key value pair:")

      try:
         message = str(value)
         print("sending: message")
         TC.send_message(message)
      except ValueError:
         print("Invalid Input")

if __name__ == "__main__":
   main()
