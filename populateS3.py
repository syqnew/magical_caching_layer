import boto
import random
from boto.s3.key import Key

conn = boto.connect_s3()

bucket = conn.create_bucket('magicalunicorn')

# Read small, medium, and large
f = open('small.txt', 'r')
small = f.read()

f = open('medium.txt', 'r')
medium = f.read()

f = open('large.txt', 'r')
large = f.read()

for i in range(0, 4739):
  value = random.randint(0, 2)
  k = Key(bucket)
  k.key = i
  str_to_set = None
  if value == 0: 
    str_to_set = small
  elif value == 1:
    str_to_set = medium
  else:
    str_to_set = large

  k.set_contents_from_string(str_to_set)
 

