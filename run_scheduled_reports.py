# Mark Bundgus 2019
import schedule
import time
from datetime import datetime
import daily_leader_board
import luigi


def job():
    print('creating daily leader boards')
    luigi.build([daily_leader_board.CreateDailyLeaderBoards()],
                # workers=3,
                local_scheduler=True,
                )
    print(datetime.now())
    print('waiting until 12:00')


schedule.every().day.at("12:00").do(job)
job()  # run job one time at startup

while True:
    schedule.run_pending()
    time.sleep(600)  # wake up every 10 minutes to see if there is something secheduled to run
