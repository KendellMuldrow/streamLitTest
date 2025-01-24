# This runs the ETL process for every state MLS:
# NJ: Bridge Interactive MLS
# MA: MLSPIN
# CT: CTMLS
# IL: MLSGRID
# NY: MLSMATRIX

# The ultimate goal is to define an abstract class in this file that all of the mls sync files implement, and then instantiate and sync all the MLS sync programs from here, but for now we just run the update functions
# for now, run with `nohup python3 ETL.py > etl_log.txt &`

import BRIDGEsync
import MLSPINsync
import CTMLSsync
import MLSGRIDsync
import MLSMATRIXsync

import datetime
import logging
import os
import time
import schedule

import sys
try:
    import socket
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    ## Create an abstract socket, by prefixing it with null.
    s.bind( '\0no_multi_reauth_bot')
except socket.error as e:
    sys.exit (0)

def updateOne(updateMethod, timeDelta):
    failcount = 0
    try:
        updateMethod(timeDelta)
    except Exception:
        failcount += 1
        if(failcount > 3):
             logging.info(f"{updateMethod} failed more than 3 times. Skipping.\n")

def updateAll(timeDelta: datetime.timedelta) -> None:
    logging.info(f"\nStarting All MLS updates for timedelta of: {timeDelta}\n")
    st = time.time()

    updateMethods = (BRIDGEsync.update, MLSPINsync.update, CTMLSsync.update, MLSGRIDsync.update, MLSMATRIXsync.update)
    for updateMethod in updateMethods:
        updateOne(updateMethod, timeDelta)

    elapsedTime = time.time()-st
    logging.info(f"\nAll MLS updates for timedelta of: {timeDelta} took {round(elapsedTime, 2)} seconds\n")
    print(f"\nAll MLS updates for timedelta of: {timeDelta} took {round(elapsedTime, 2)} seconds\n")

def main():
    # Initialize logging
    local_directory = os.path.dirname(os.path.realpath(__file__))
    if not os.path.exists(local_directory+'/logs'):
        os.makedirs(local_directory+'/logs')
    logging.basicConfig(filename=local_directory+'/logs/ETL_'+time.strftime("%b-%d-%Y")+'.log',
                        level=logging.INFO, format='%(asctime)s : %(levelname)s : %(message)s')


    updateAll(datetime.timedelta(hours=24))
    updateAll(datetime.timedelta(1))
    updateAll(datetime.timedelta(0, 3600*2))
    updateAll(datetime.timedelta(0, 3600//2))

    schedule.every(5).minutes.do(updateAll, datetime.timedelta(0, 60*10))

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
