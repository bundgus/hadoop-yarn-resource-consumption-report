# Mark Bundgus 2019
import luigi
import logging
from yarn_api_client import ResourceManager  # https://python-client-for-hadoop-yarn-api.readthedocs.io
from datetime import datetime
from datetime import timedelta
import pandas as pd
from tabulate import tabulate
import os
import configuration

log = logging.getLogger("luigi-interface")


class LeaderBoard(luigi.Task):
    jobs_year = luigi.Parameter()
    jobs_month = luigi.Parameter()
    jobs_day = luigi.Parameter()

    def output(self):
        output_path = os.path.join('daily_leader_boards',
                                   'leader_board_'
                                   + str(self.jobs_year)
                                   + '-' + str(self.jobs_month).zfill(2)
                                   + '-' + str(self.jobs_day).zfill(2) + '.csv')
        return luigi.LocalTarget(output_path)

    def run(self):
        analysis_timestamp = str(datetime.now())
        output_path = os.path.join('daily_leader_boards',
                                   'leader_board_'
                                   + str(self.jobs_year)
                                   + '-' + str(self.jobs_month).zfill(2)
                                   + '-' + str(self.jobs_day).zfill(2) + '.csv')

        rm = ResourceManager(configuration.yarn_resource_managers)
        metrics = rm.cluster_metrics()
        cluster_vcores_total = metrics.data['clusterMetrics']['totalVirtualCores']
        cluster_daily_vcore_seconds = int(cluster_vcores_total * 60 * 60 * 24)

        cluster_memory_total_mb = metrics.data['clusterMetrics']['totalMB']
        cluster_daily_megabyte_memory_seconds = int(cluster_memory_total_mb * 60 * 60 * 24)

        begin_date = datetime(int(str(self.jobs_year)), int(str(self.jobs_month)), int(str(self.jobs_day)))
        end_date = begin_date + timedelta(1)
        begin_ms = str(int(begin_date.timestamp() * 1000))
        end_ms = str(int(end_date.timestamp() * 1000))

        # filter out jobs that started after the end of the analyzed day
        apps = rm.cluster_applications(
                                       # finished_time_begin=begin_ms,
                                       started_time_end=end_ms
                                       )
        applist = apps.data['apps']['app']
        total_vcore_seconds = 0
        total_mb_seconds = 0
        sum_elapsed_time_ms = 0
        overall_started_time_ms = 9999999999999
        overall_finished_time_ms = 0
        total_yarn_apps = 0

        users = {}

        app_file = 'app_lists/apps_' + str(self.jobs_year) \
                   + '-' + str(self.jobs_month).zfill(2) \
                   + '-' + str(self.jobs_day).zfill(2) + '.csv'

        apps_df = pd.DataFrame(applist)
        apps_df.to_csv(app_file)

        for app in applist:

            begin_ms_int = int(begin_ms)
            end_ms_int = int(end_ms)
            started_time = app['startedTime']
            finished_time = app['finishedTime']
            elapsed_time = app['elapsedTime']

            # disregard apps that haven't ever or yet consumed any resources
            if app['state'] not in ['FINISHED', 'FAILED', 'KILLED', 'RUNNING']:
                continue

            # disregard apps that finished before the beginning of the analyzed day
            if 0 < finished_time < begin_ms_int:
                continue

            # for scenario where job began and ended in the same day
            percent_within_day = 1.0

            # scenario where job began before the beginning of the day and ended before the end of the day
            if started_time < begin_ms_int < finished_time < end_ms_int:
                percent_within_day = (finished_time - begin_ms_int)/elapsed_time

            # scenario where job began before the beginning of the day and continued beyond the end of the day
            if started_time < begin_ms_int and (finished_time == 0 or finished_time > end_ms_int):
                percent_within_day = 86400000/elapsed_time

            # scenario where job began before the end of the day and continued beyond the end of the day
            if begin_ms_int < started_time < end_ms_int \
                    and (finished_time == 0 or end_ms_int < finished_time):
                percent_within_day = (end_ms_int-started_time)/elapsed_time

            weighted_app_vcore_seconds = int(app['vcoreSeconds'] * percent_within_day)
            weighted_app_memory_seconds = int(app['memorySeconds'] * percent_within_day)

            user = users.setdefault(app['user'], {'user_first_task_started_time_ms': 9999999999999,
                                                  'last_task_finished_time_ms': 0})
            total_vcore_seconds += weighted_app_vcore_seconds
            total_mb_seconds += weighted_app_memory_seconds

            user['user_first_task_started_time_ms'] = app['startedTime'] \
                if app['startedTime'] < user['user_first_task_started_time_ms'] \
                else user['user_first_task_started_time_ms']
            user['last_task_finished_time_ms'] = app['finishedTime'] \
                if app['finishedTime'] > user['last_task_finished_time_ms'] \
                else user['last_task_finished_time_ms']

            overall_started_time_ms = app['startedTime'] if app['startedTime'] < overall_started_time_ms \
                else overall_started_time_ms
            overall_finished_time_ms = app['finishedTime'] if app['finishedTime'] > overall_finished_time_ms \
                else overall_finished_time_ms

            sum_elapsed_time_ms += app['elapsedTime']
            total_yarn_apps += 1

            user_total_vcore_seconds = user.setdefault('total_vcore_seconds', 0)
            user['total_vcore_seconds'] = user_total_vcore_seconds + weighted_app_vcore_seconds

            user_total_mb_seconds = user.setdefault('total_MB_seconds', 0)
            user['total_MB_seconds'] = user_total_mb_seconds + weighted_app_memory_seconds

        header = ['jobs_year',
                  'jobs_month',
                  'jobs_day',
                  'cluster_daily_vcore_seconds',
                  'cluster_daily_megabyte_memory_seconds',
                  'user',
                  'used_vcore_seconds',
                  'percent_used_of_all_used_vcore_seconds',
                  'percent_used_of_total_cluster_vcore_seconds',
                  'used_MB_seconds',
                  'percent_used_of_all_used_MB_seconds',
                  'percent_used_of_total_cluster_MB_seconds',
                  'user_first_task_started_time',
                  'user_last_task_finished_time'
                  ]

        table = []

        for user in users:

            # set last_task_finished_time to None if timestamp == 0 representing that the task hasn't finished yet
            if int(users[user]['last_task_finished_time_ms']) == 0:
                last_task_finished_time_string = ''
            else:
                last_task_finished_time_string = \
                    datetime.fromtimestamp(users[user]['last_task_finished_time_ms'] / 1000.0)\
                        .strftime('%Y-%m-%d %H:%M')

            row = [
                   self.jobs_year,
                   self.jobs_month,
                   self.jobs_day,
                   cluster_daily_vcore_seconds,
                   cluster_daily_megabyte_memory_seconds,
                   user,
                   round(users[user]['total_vcore_seconds'], 0),
                   round(100 * users[user]['total_vcore_seconds'] / total_vcore_seconds, 2),
                   round(100 * users[user]['total_vcore_seconds'] / cluster_daily_vcore_seconds, 2),
                   round(users[user]['total_MB_seconds'], 0),
                   round(100 * users[user]['total_MB_seconds'] / total_mb_seconds, 2),
                   round(100 * users[user]['total_MB_seconds'] / cluster_daily_megabyte_memory_seconds, 2),
                   datetime.fromtimestamp(users[user]['user_first_task_started_time_ms'] / 1000.0)
                       .strftime('%Y-%m-%d %H:%M'),
                   last_task_finished_time_string,
                   ]

            table.append(row)

        df = pd.DataFrame(table, columns=header)
        df = df.sort_values(by='used_MB_seconds', ascending=False)

        print()
        print('analysis timestamp: ' + analysis_timestamp)
        # print('functional account:', job_user)
        print('jobs date: ' + begin_date.strftime('%Y-%m-%d'))
        print('----------------------')
        print('count of yarn apps: ' + str(total_yarn_apps))
        print('overall daily jobs started time ',
              datetime.fromtimestamp(overall_started_time_ms / 1000.0).strftime('%Y-%m-%d %H:%M'))
        print('overall daily jobs finished time',
              datetime.fromtimestamp(overall_finished_time_ms / 1000.0).strftime('%Y-%m-%d %H:%M'))
        print()

        print(tabulate(df, headers='keys', showindex=False))
        df.to_csv(output_path,
                  index=False)


# create leader boards for the last 3 days
class CreateDailyLeaderBoards(luigi.Task):

    def complete(self):
        return False

    def requires(self):
        required = []
        now = datetime.now()

        log.info('Attempting to create leader board')
        for days_int in range(1, 3):
            date = now - timedelta(days_int)
            year = date.year
            month = date.month
            day = date.day

            required.append(
                LeaderBoard(
                    jobs_year=str(year),
                    jobs_month=str(month),
                    jobs_day=str(day)))

        return required
