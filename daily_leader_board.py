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
        output_path = os.path.join('daily_leader_boards',
                                   'leader_board_'
                                   + str(self.jobs_year)
                                   + '-' + str(self.jobs_month).zfill(2)
                                   + '-' + str(self.jobs_day).zfill(2) + '.csv')

        rm = ResourceManager(address=configuration.resource_manager_address, port=configuration.resource_manager_port)

        metrics = rm.cluster_metrics()
        cluster_vcores_total = metrics.data['clusterMetrics']['totalVirtualCores']
        cluster_daily_vcore_seconds = int(cluster_vcores_total * 60 * 60 * 24)

        cluster_memory_total_mb = metrics.data['clusterMetrics']['totalMB']
        cluster_daily_megabyte_memory_seconds = int(cluster_memory_total_mb * 60 * 60 * 24)

        begin_date = datetime(int(str(self.jobs_year)), int(str(self.jobs_month)), int(str(self.jobs_day)))
        end_date = begin_date + timedelta(1)
        begin_ms = str(int(begin_date.timestamp() * 1000))
        end_ms = str(int(end_date.timestamp() * 1000))

        apps = rm.cluster_applications(state='FINISHED',
                                       started_time_begin=begin_ms,
                                       finished_time_end=end_ms
                                       )
        applist = apps.data['apps']['app']
        total_vcore_seconds = 0
        total_mb_seconds = 0
        sum_elapsed_time_ms = 0
        overall_started_time_ms = 9999999999999
        overall_finished_time_ms = 0
        total_yarn_apps = 0

        users = {}

        for app in applist:
            user = users.setdefault(app['user'], {'user_first_task_started_time_ms': 9999999999999,
                                                  'last_task_finished_time_ms':0})
            total_vcore_seconds += app['vcoreSeconds']
            total_mb_seconds += app['memorySeconds']

            user['user_first_task_started_time_ms'] = app['startedTime'] \
                if app['startedTime'] < user['user_first_task_started_time_ms'] \
                else user['user_first_task_started_time_ms']
            user['last_task_finished_time_ms'] = app['finishedTime'] \
                if app['finishedTime'] > user['last_task_finished_time_ms'] \
                else  user['last_task_finished_time_ms']

            overall_started_time_ms = app['startedTime'] if app['startedTime'] < overall_started_time_ms \
                else overall_started_time_ms
            overall_finished_time_ms = app['finishedTime'] if app['finishedTime'] > overall_finished_time_ms \
                else overall_finished_time_ms

            sum_elapsed_time_ms += app['elapsedTime']
            total_yarn_apps += 1

            user_total_vcore_seconds = user.setdefault('total_vcore_seconds', 0)
            user['total_vcore_seconds'] = user_total_vcore_seconds + app['vcoreSeconds']

            user_total_mb_seconds = user.setdefault('total_MB_seconds', 0)
            user['total_MB_seconds'] = user_total_mb_seconds + app['memorySeconds']

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
                  'last_task_finished_time'
                  ]

        table = []

        for user in users:
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
                   datetime.fromtimestamp(users[user]['last_task_finished_time_ms'] / 1000.0)
                       .strftime('%Y-%m-%d %H:%M'),
                   ]

            table.append(row)

        df = pd.DataFrame(table, columns=header)
        df = df.sort_values(by='used_MB_seconds', ascending=False)

        print()
        print('analysis timestamp: ' + str(datetime.now()))
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


# create leader boards for the last 7 days
class CreateDailyLeaderBoards(luigi.Task):

    def complete(self):
        return False

    def requires(self):
        required = []
        now = datetime.now()

        log.info('Attempting to create leader board')
        for days_int in range(1, 6):
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
