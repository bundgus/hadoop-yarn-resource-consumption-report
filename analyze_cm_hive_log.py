import json
import requests
import datetime
from pytz import timezone
from datetime import timedelta
from requests.auth import HTTPBasicAuth
import configuration

#########################################

cm_user = configuration.cm_user
cm_password = configuration.cm_password
functional_account = configuration.hive_functional_account

# replace with the year,month,day (In US/Central time) that you want to collect the list of queries.
year = configuration.hive_year
month = configuration.hive_month
day = configuration.hive_day

##########################################

start_datetime = datetime.datetime(year, month, day, 0, 0, 0)
start_datetime_central = timezone('US/Central').localize(start_datetime)
start_datetime_utc = start_datetime_central.astimezone(timezone('UTC'))  # Convert to UTC time zone

end_datetime = start_datetime + timedelta(1)
end_datetime_central = timezone('US/Central').localize(end_datetime)
end_datetime_utc = end_datetime_central.astimezone(timezone('UTC'))  # Convert to UTC time zone

print('from:', str(start_datetime_utc))
print('to:', str(end_datetime_utc))
print()

url = 'http://' + configuration.yarn_resource_manager_address + ':7180/api/v7/clusters/cluster/services/yarn/yarnApplications' \
      '?from={0}-{1}-{2}T{3}%3A{4}%3A{5}.000Z' \
      '&to={6}-{7}-{8}T{9}%3A{10}%3A{11}.000Z' \
      '&filter=user+%3D+%22{12}%22'.format(str(start_datetime_utc.year),
                                           str(start_datetime_utc.month).zfill(2),
                                           str(start_datetime_utc.day).zfill(2),
                                           str(start_datetime_utc.hour).zfill(2),
                                           str(start_datetime_utc.minute).zfill(2),
                                           str(start_datetime_utc.second).zfill(2),
                                           str(end_datetime_utc.year),
                                           str(end_datetime_utc.month).zfill(2),
                                           str(end_datetime_utc.day).zfill(2),
                                           str(end_datetime_utc.hour).zfill(2),
                                           str(end_datetime_utc.minute).zfill(2),
                                           str(end_datetime_utc.second).zfill(2),
                                           functional_account
                                           )

print(url)

jo = json.loads(requests.get(url, auth=HTTPBasicAuth(cm_user, cm_password)).content.decode())

queries = {}

# print('start_time|end_time|hdfs_bytes_read|hdfs_bytes_written|query')
print('task count:', str(len(jo['applications'])))
for app in jo['applications']:
    print(app)
    if app['state'] != 'RUNNING':
        st = app['startTime']
        et = app['endTime']
        hq = app['attributes']['hive_query_string'].replace('\n', ' ')
        br = app['attributes']['hdfs_bytes_read']
        bw = app['attributes']['hdfs_bytes_written']
        row = '|'.join([str(st), "str(et)", str(br), str(bw), str(hq)])
        # results are returned in reverse chronological order - this ensures the earlies related task start is captured
        queries[hq] = st
print()
print('unique query count:', str(len(queries)))
print('-------------------')
#for q in queries:
#    print(queries[q], q)

print()
print('start_time|query')
# show the start time of the query by sorting for the first timestamp
for w in sorted(queries, key=queries.get, reverse=False):
    print('|'.join((queries[w], w)))

print()

for w in sorted(queries, key=queries.get, reverse=False):
    print(w.strip(), ';')