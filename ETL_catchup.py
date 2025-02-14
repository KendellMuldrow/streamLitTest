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

def updateOne(updateMethod, timeDelta):
    try:
        updateMethod(timeDelta)
    except Exception:
        updateOne(updateMethod, timeDelta)

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


    updateAll(datetime.timedelta(days=20))

if __name__ == "__main__":
    main()
