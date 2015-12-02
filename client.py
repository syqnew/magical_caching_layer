import socket
import sys

class Client():
    #52.32.231.184
   def __init__(self,Adress=("52.34.191.24", 5500)):
      self.s = socket.socket()
      self.s.connect(Adress)

   def get(self, key):
      self.s.send(key.encode())
      # prints out to standard output

   def listen(self):
      print "implement me"

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
