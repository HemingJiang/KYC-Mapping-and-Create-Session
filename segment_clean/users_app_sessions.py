import os
from tendo import singleton

me = singleton.SingleInstance()
dir_path = '/PATH'
filename = 'users_app_sessions.sql'
os.system('psql -h AWS HOST ADDRESS -p 5439 \
	-f %(d)s/%(f)s DATABASE USER' % {"d": dir_path, "f": filename})
