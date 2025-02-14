# This runs the ETL process for every state MLS:
# NJ: Bridge Interactive MLS
# MA: MLSPIN
# CT: CTMLS
# IL: MLSGRID
# NY: MLSMATRIX

# The ultimate goal is to define an abstract class in this file that all of the mls sync files implement, and then instantiate and sync all the MLS sync programs from here, but for now we just run the update functions
# for now, run with `nohup python3 ETL.py > etl_log.txt &`

import BRIDGEsync
import TRESTLEsync
# import MLSPINsync
# import CTMLSsync
# import MLSGRIDsync
# import MLSMATRIXsync

import datetime
import logging
import os
import time
import schedule



def updateOne(updateMethod, timeDelta):
    try:
        updateMethod(timeDelta)
    except Exception as e:
        logging.error(f'Exception in ETL.py: {e}')
        updateOne(updateMethod, timeDelta)

def updateAll(timeDelta: datetime.timedelta) -> None:
    logging.info(f"\nStarting All MLS updates for timedelta of: {timeDelta}\n")
    st = time.time()

    updateMethods = (BRIDGEsync.update, TRESTLEsync.update)
    for updateMethod in updateMethods:
        updateMethod(timeDelta)
    
    
    elapsedTime = time.time()-st
    logging.info(f"\nAll MLS updates for timedelta of: {timeDelta} took {round(elapsedTime, 2)} seconds\n")
    print(f"\n{datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}: All MLS updates for timedelta of: {timeDelta} took {round(elapsedTime, 2)} seconds\n")

def main():
    # Initialize logging
    local_directory = os.path.dirname(os.path.realpath(__file__))
    if not os.path.exists(local_directory+'/logs'):
        os.makedirs(local_directory+'/logs')
    logging.basicConfig(filename=local_directory+'/logs/ETL_'+time.strftime("%b-%d-%Y")+'.log',
                        level=logging.INFO, format='%(asctime)s : %(levelname)s : %(message)s')



    updateAll(datetime.timedelta(1))
    #updateAll(datetime.timedelta(12))
    #updateAll(datetime.timedelta(0, 3600*2))
    #updateAll(datetime.timedelta(0, 3600//2))
   #
    #
#
    #schedule.every(5).minutes.do(updateAll, datetime.timedelta(0, 60*10))

   #while True:
   #    schedule.run_pending()
   #    time.sleep(1)


if __name__ == "__main__":
    main()