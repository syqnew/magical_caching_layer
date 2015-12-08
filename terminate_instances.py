import boto.ec2

conn = boto.ec2.connect_to_region("us-west-2")
reservations = conn.get_all_reservations()

# get ids of instances to terminate
instances_to_terminate = []

for reservation in reservations:
  instances = reservation.instances
  for instance in instances:
    instances_to_terminate.append(instance.id)

# terminate instances
conn.terminate_instances(instance_ids=instances_to_terminate)

