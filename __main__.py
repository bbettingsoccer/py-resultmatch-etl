# This is a sample Python script.
from src.config.enviroment_conf import env_check
import sys
from src.jobs.etl_jobs import execute_job


# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    champ = sys.argv[1]
    print("<<<<- ARGUMENTO [1] ->>>>> ", champ)
    match_date = sys.argv[2]
    print("<<<<- ARGUMENTO [2] ->>>>> ", match_date)

    env_check()
    execute_job(championship=champ, match_date=match_date)
