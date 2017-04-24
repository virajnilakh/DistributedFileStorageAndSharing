#!/bin/bash
#
# This script is used to start the server from a supplied config file
#

#TODO: REPLACE the route-1.conf with your corresponding config info

java -server -Xms500m -Xmx1000m -cp ".;lib/*;classes/" gash.router.server.MessageApp runtime/route-1.conf