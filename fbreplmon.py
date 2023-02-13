#!/usr/bin/python3
#####################################################################################
#                                                                                   #
# Script Purpose: To collect replication metrics from a FB                          #
#                 at a given interval over a specified number of iterations.        #
#                                                                                   #
# Author: Somu Rajarathinam                                                         #
# Date: 02/13/2023                                                                  #
#                                                                                   #
#*******Disclaimer:*****************************************************************#
# This script is offered "as is" with no warranty or support.                       #
# While this script is tested and worked in our environment, it is recommended      #
# that you test this script before using it in a production environment.            #
# No written permission needed to use this script.                                  #
# Neither me nor Pure Storage will be liable for any damage/loss to the system.     #
#####################################################################################
#
import pypureclient
from pypureclient import flashblade
from pypureclient.flashblade import BucketReplicaLink, ObjectStoreRemoteCredentials, rest
import sys
import time
import argparse
from datetime import datetime
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

parser = argparse.ArgumentParser(description='Replication metrics collection script')

parser.add_argument('--fb', help='Mgmt IP of the FlashBlade from which replication metrics are to be collected')
parser.add_argument('--api', help='API token of the user to connect to the FlashBlade')
parser.add_argument('--bucket', help='Bucket for which replication metrics to be collected')
parser.add_argument('--interval', help='Metrics collection interval in seconds', default=60)
parser.add_argument('--count', help='Iteration count', default=1)
parser.add_argument('--unit', help='Unit of Space (KB/MB/GB)', default='B')

args = vars(parser.parse_args())

fb1 = args['fb']
token = args['api']
bucket = args['bucket']
interval = int(args['interval'])
count = int(args['count'])
unit = args['unit']

if (unit not in ['GB','MB','KB','TB']):
  unit = 'B'

if len(sys.argv) <= 1:
  print("fbreplmon.py --fb mgmt_ip --api api_token --bucket bucket_name --interval interval_in_seconds --count iteration_count --unit space_unit in KB/MB/GB")
  sys.exit(-1)

# create PurityFb object for a certain array
try:
  client1 = flashblade.Client(target=fb1, api_token=token)
except pypureclient.exceptions.PureError as e:
  print("Exception when logging into the array: %s\n" % e)

if (client1):
   try:
       for i in range(0, count):
         now = datetime.now()
         current_time = now.strftime("%Y-%m-%d %H:%M:%S")

         fr='name=\'' + bucket + '\''
         res = client1.get_buckets(filter=fr)
         fb_bkt = list(res.items)
         res = client1.get_bucket_replica_links(local_bucket_names=[bucket])
         fb_brl =list(res.items)
         res = client1.get_array_connections_performance_replication()
         fb_apr = list(res.items)

         if (unit == 'TB'):
           fb_bkt_space = fb_bkt[0].space.virtual/1024.00/1024.00/1024.00/1024.00;
           fb_obj_bklog = fb_brl[0].object_backlog.bytes_count/1024.00/1024.00/1024.00/1024.00;
           fb_txn_bps   = fb_apr[0].aggregate.transmitted_bytes_per_sec/1024.00/1024.00;
         elif (unit == 'GB'):
           fb_bkt_space = fb_bkt[0].space.virtual/1024.00/1024.00/1024.00;
           fb_obj_bklog = fb_brl[0].object_backlog.bytes_count/1024.00/1024.00/1024.00;
           fb_txn_bps   = fb_apr[0].aggregate.transmitted_bytes_per_sec/1024.00/1024.00;
         elif (unit == 'MB'):
           fb_bkt_space = fb_bkt[0].space.virtual/1024.00/1024.00;
           fb_obj_bklog = fb_brl[0].object_backlog.bytes_count/1024.00/1024.00;
           fb_txn_bps   = fb_apr[0].aggregate.transmitted_bytes_per_sec/1024.00/1024.00;
         elif (unit == 'KB'):
           fb_bkt_space = fb_bkt[0].space.virtual/1024.00;
           fb_obj_bklog = fb_brl[0].object_backlog.bytes_count/1024.00;
           fb_txn_bps   = fb_apr[0].aggregate.transmitted_bytes_per_sec/1024.00/1024.00;
         else:
           fb_bkt_space = fb_bkt[0].space.virtual;
           fb_obj_bklog = fb_brl[0].object_backlog.bytes_count;
           fb_txn_bps   = fb_apr[0].aggregate.transmitted_bytes_per_sec;


         if (i == 0):
            print("date_time, object_count, space_usage, repl_lag_sec, backlog_bytes, status, repl_rcv_bps, repl_send_bps")

         print("{0}, {1}, {2:12.2f}, {3:5.2f}, {4:12.2f}, {5}, {6:12.2f}, {7:12.2f}".format(current_time, fb_bkt[0].object_count, fb_bkt_space, fb_brl[0].lag/1000.00, fb_obj_bklog, fb_brl[0].status, fb_apr[0].aggregate.received_bytes_per_sec, fb_txn_bps))
         if (i == (count-1)):
           break
         time.sleep(interval)
   except rest.ApiException as e:
     print("Exception when listing replica links: %s\n" % e)

   client1.logout()
