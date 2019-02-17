import os
from tendo import singleton

me = singleton.SingleInstance()
dir_path = '/repos/coinsquare_data_analytics/coinsquare-data-analytics/production/segment_clean'
filename = 'users_app_sessions.sql'
os.system('psql -h cumulonimbogenitus.cvpqdfcjsb13.ca-central-1.redshift.amazonaws.com -p 5439 \
	-f %(d)s/%(f)s fractus colin' % {"d": dir_path, "f": filename})
