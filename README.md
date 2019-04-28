# hadoop-yarn-resource-consumption-report
## For a multi-tenant Hadoop environment, create daily reports on the usage of shared Hadoop cluster YARN resources by user.

Mark Bundgus 2019

Tested on Cloudera 5.12.1

The time to create a daily report in csv format with this script is less than 1 second.

#### Configuration

Configure for your environment by editing the configuration.py file

    yarn_resource_manager_address = 'server.domain.com'
    yarn_resource_manager_port = 8088

#### Running

To start a scheduled job that runs immediately, then again at 12:00 every day to produce a leader board for the prior 7 day days:

    python run_scheduled_reports.py

#### Results

Results will be written in csv format to an individual file per day in the daily_leader_board directory.

Field definitions:

|Field|Description|
|---|---|
|jobs_year|year of metric|
|jobs_month|month of metric|
|jobs_day|day of metric|
|cluster_daily_vcore_seconds|total allocated YARN daily VCore capacity for the cluster in VCore-seconds|
|cluster_daily_megabyte_memory_seconds|total allocated YARN daily memory capacity for the cluster in megabyte-seconds|
|user|YARN user account associated with tasks|
|used_vcore_seconds|VCore-seconds used by YARN user for the day|
|percent_used_of_all_used_vcore_seconds|For all VCore capacity used by all YARN users, the percentage of the used VCore capacity that was specifically used by the individual YARN user account|
|percent_used_of_total_cluster_vcore_seconds|The percentage of total daily YARN cluster VCore capacity that was specifically used by the individual YARN user account|
|used_MB_seconds|Megabyte-seconds used by YARN user for the day|
|percent_used_of_all_used_MB_seconds|For all memory capacity used by all YARN users, the percentage of the used memory capacity that was specifically used by the individual YARN user account|
|percent_used_of_total_cluster_MB_seconds|The percentage of total daily YARN cluster memory capacity that was specifically used by the individual YARN user account|
|user_first_task_started_time|For the specific user for the indicated day, the datetime that the first YARN task started|
|last_task_finished_time|For the specific user for the indicated day, the datetime that the last YARN task finished|


Sample csv output:

    jobs_year,jobs_month,jobs_day,cluster_daily_vcore_seconds,cluster_daily_megabyte_memory_seconds,user,used_vcore_seconds,percent_used_of_all_used_vcore_seconds,percent_used_of_total_cluster_vcore_seconds,used_MB_seconds,percent_used_of_all_used_MB_seconds,percent_used_of_total_cluster_MB_seconds,user_first_task_started_time,last_task_finished_time
    2019,4,26,257817600,564385708800,user1,53118060,71.66,20.6,205637409339,58.03,36.44,2019-04-26 00:25,2019-04-26 23:57
    2019,4,26,257817600,564385708800,user2,1740261,2.35,0.67,57144608173,16.13,10.13,2019-04-26 00:06,2019-04-26 23:16
    2019,4,26,257817600,564385708800,user3,4437423,5.99,1.72,19898742116,5.62,3.53,2019-04-26 00:00,2019-04-26 23:14
    2019,4,26,257817600,564385708800,user4,651052,0.88,0.25,11884601658,3.35,2.11,2019-04-26 00:48,2019-04-26 23:49
    2019,4,26,257817600,564385708800,user5,2055650,2.77,0.8,8505839157,2.4,1.51,2019-04-26 01:50,2019-04-26 07:54
    2019,4,26,257817600,564385708800,user6,1783740,2.41,0.69,8097954845,2.29,1.43,2019-04-26 06:35,2019-04-26 08:48
    2019,4,26,257817600,564385708800,user7,1774089,2.39,0.69,7310847944,2.06,1.3,2019-04-26 00:30,2019-04-26 07:00
    2019,4,26,257817600,564385708800,user8,1632582,2.2,0.63,6907374621,1.95,1.22,2019-04-26 02:00,2019-04-26 21:07
    2019,4,26,257817600,564385708800,user9,1648962,2.22,0.64,6800135430,1.92,1.2,2019-04-26 10:00,2019-04-26 10:42
    2019,4,26,257817600,564385708800,user10,1167160,1.57,0.45,4885440653,1.38,0.87,2019-04-26 00:19,2019-04-26 23:23
    2019,4,26,257817600,564385708800,user11,703338,0.95,0.27,3198766475,0.9,0.57,2019-04-26 18:00,2019-04-26 18:26
    2019,4,26,257817600,564385708800,user12,567929,0.77,0.22,2385953423,0.67,0.42,2019-04-26 02:44,2019-04-26 06:56
    2019,4,26,257817600,564385708800,user13,399277,0.54,0.15,1653569071,0.47,0.29,2019-04-26 16:08,2019-04-26 17:06
    2019,4,26,257817600,564385708800,user14,324048,0.44,0.13,1374964457,0.39,0.24,2019-04-26 05:45,2019-04-26 13:50
    2019,4,26,257817600,564385708800,user15,245452,0.33,0.1,1012116338,0.29,0.18,2019-04-26 01:00,2019-04-26 05:12
    2019,4,26,257817600,564385708800,user16,215693,0.29,0.08,893749081,0.25,0.16,2019-04-26 08:42,2019-04-26 09:38

During runtime the results will also be printed to the console.

Sample Console Output:

    analysis timestamp: 2019-04-27 22:16:14.518770
    jobs date: 2019-04-26
    ----------------------
    count of yarn apps: 1223
    overall daily jobs started time  2019-04-26 00:00
    overall daily jobs finished time 2019-04-26 23:58

      jobs_year    jobs_month    jobs_day    cluster_daily_vcore_seconds    cluster_daily_megabyte_memory_seconds  user                        used_vcore_seconds    percent_used_of_all_used_vcore_seconds    percent_used_of_total_cluster_vcore_seconds    used_MB_seconds    percent_used_of_all_used_MB_seconds    percent_used_of_total_cluster_MB_seconds  user_first_task_started_time       last_task_finished_time
    -----------  ------------  ----------  -----------------------------  ---------------------------------------  ------------------------  --------------------  ----------------------------------------  ---------------------------------------------  -----------------  -------------------------------------  ------------------------------------------  ---------------------------------  ----------------------------
           2019             4          26                      257817600                             564385708800  user1                                 53118060                                     71.66                                          20.6        205637409339                                  58.03                                       36.44  2019-04-26 00:25                   2019-04-26 23:57
           2019             4          26                      257817600                             564385708800  user2                                  1740261                                      2.35                                           0.67        57144608173                                  16.13                                       10.13  2019-04-26 00:06                   2019-04-26 23:16
           2019             4          26                      257817600                             564385708800  user3                                  4437423                                      5.99                                           1.72        19898742116                                   5.62                                        3.53  2019-04-26 00:00                   2019-04-26 23:14
           2019             4          26                      257817600                             564385708800  user4                                   651052                                      0.88                                           0.25        11884601658                                   3.35                                        2.11  2019-04-26 00:48                   2019-04-26 23:49
           2019             4          26                      257817600                             564385708800  user5                                  2055650                                      2.77                                           0.8          8505839157                                   2.4                                         1.51  2019-04-26 01:50                   2019-04-26 07:54
           2019             4          26                      257817600                             564385708800  user6                                  1783740                                      2.41                                           0.69         8097954845                                   2.29                                        1.43  2019-04-26 06:35                   2019-04-26 08:48
           2019             4          26                      257817600                             564385708800  user7                                  1774089                                      2.39                                           0.69         7310847944                                   2.06                                        1.3   2019-04-26 00:30                   2019-04-26 07:00
           2019             4          26                      257817600                             564385708800  user8                                  1632582                                      2.2                                            0.63         6907374621                                   1.95                                        1.22  2019-04-26 02:00                   2019-04-26 21:07
           2019             4          26                      257817600                             564385708800  user9                                  1648962                                      2.22                                           0.64         6800135430                                   1.92                                        1.2   2019-04-26 10:00                   2019-04-26 10:42
           2019             4          26                      257817600                             564385708800  user10                                 1167160                                      1.57                                           0.45         4885440653                                   1.38                                        0.87  2019-04-26 00:19                   2019-04-26 23:23
           2019             4          26                      257817600                             564385708800  user11                                  703338                                      0.95                                           0.27         3198766475                                   0.9                                         0.57  2019-04-26 18:00                   2019-04-26 18:26
           2019             4          26                      257817600                             564385708800  user12                                  567929                                      0.77                                           0.22         2385953423                                   0.67                                        0.42  2019-04-26 02:44                   2019-04-26 06:56
           2019             4          26                      257817600                             564385708800  user13                                  399277                                      0.54                                           0.15         1653569071                                   0.47                                        0.29  2019-04-26 16:08                   2019-04-26 17:06
           2019             4          26                      257817600                             564385708800  user14                                  324048                                      0.44                                           0.13         1374964457                                   0.39                                        0.24  2019-04-26 05:45                   2019-04-26 13:50
           2019             4          26                      257817600                             564385708800  user15                                  245452                                      0.33                                           0.1          1012116338                                   0.29                                        0.18  2019-04-26 01:00                   2019-04-26 05:12
           2019             4          26                      257817600                             564385708800  user16                                  215693                                      0.29                                           0.08          893749081                                   0.25                                        0.16  2019-04-26 08:42                   2019-04-26 09:38
    ...
    2019-04-27 22:16:14.561792
    waiting until 12:00