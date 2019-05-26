import glob

read_files = glob.glob("daily_leader_boards/*.csv")

header_row = True

with open("combined_daily_leader_board.csv", "w") as outfile:
    for f in read_files:
        with open(f, "r") as infile:
            for line in infile:
                if header_row:
                    print(line, end='')
                    print(line, file=outfile, end='')
                    header_row = False
                elif line[:9] != 'jobs_year':
                        print(line[:9])
                        print(line, end='')
                        print(line, file=outfile, end='')
