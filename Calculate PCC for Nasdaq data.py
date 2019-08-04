from datetime import datetime
import multiprocessing
from multiprocessing import Process
from multiprocessing import Queue

# This function will be the message loop in each child process that gets launched
def ChildProcessMessages(queueIn,queueOut,output_path_csv, stocks_path):
    import pandas as pd
    import os.path
    import csv
    import os
    from scipy.stats.stats import pearsonr
               
    # Output will not be sent back to the parent host.
    # Output will be saved to a file that will be merged with the files from all other child
    # processes when all messages have been completed.
    # This keeps our lanes flowing instead of crossing back into the parent host lane.
    pid = os.getpid()
    with open(output_path_csv+str(pid)+".csv", 'a') as f:
        writer = csv.writer(f)
       
        while True:
            
            try:
                msg = None
                # Get next msg from cross process queue
                msg = queueIn.get(True, timeout=1)
                
                # Parse msg into parameters
                (StockA, StockB)=msg
                
                if not StockA and not StockB:
                    # Kill message
                    return
                
            except Exception as e:
                pass
                continue
            
            
            try:
                startingKey = "2019-06-17"
                keyColumnName="timestamp"
                valueColumnName="close"
                stock_A = pd.read_csv(stocks_path+StockA+".csv", delimiter=",", quotechar='"',usecols=[keyColumnName,valueColumnName])
                stock_B = pd.read_csv(stocks_path+StockB+".csv", delimiter=",", quotechar='"',usecols=[keyColumnName,valueColumnName])
                RowCount = 100
                
                # stock_A
                if len(stock_A.index) < 110:
                    # not enough data in this file
                    print("** 1 **")
                    continue

                if len(stock_A.columns) > 2 or len(stock_A.columns) < 2:
                    # not valid data
                    print("** 2 **")
                    continue

                # Scroll down to the startingKey for the KeyColumn and then read in RowCount values down from there (inclusive)

                # Loop until we have found the first row that matches our startingKey and store index in start
                Start = 0
                for index, row in stock_A.iterrows():

                    if row[keyColumnName].strip() == startingKey or Start > 100:
                        break

                    Start += 1
                #print(Start)

                if Start > 100:
                    # not a valid starting point ... should have found much earlier
                    print("** 3 **")
                    continue

                # Keep first RowCount rows after Start
                stock_A = stock_A[Start:Start+RowCount]
                
                # stock_B ============================                
                if len(stock_B.index) < 110:
                    # not enough data in this file
                    print("** 1 **")
                    continue

                if len(stock_B.columns) > 2 or len(stock_B.columns) < 2:
                    # not valid data
                    print("** 2 **")
                    continue

                # Scroll down to the startingKey for the KeyColumn and then read in RowCount values down from there (inclusive)

                # Loop until we have found the first row that matches our startingKey and store index in start
                Start = 0
                for index, row in stock_B.iterrows():

                    if row[keyColumnName].strip() == startingKey or Start > 100:
                        break

                    Start += 1
                #print(Start)

                if Start > 100:
                    # not a valid starting point ... should have found much earlier
                    print("** 3 **")
                    continue

                # Keep first RowCount rows after Start
                stock_B = stock_B[Start:Start+RowCount]
                
                # Now both arrays contain a cleaned list of 100 items starting on the same date (startingKey)
                p = pearsonr(stock_A[valueColumnName], stock_B[valueColumnName])
                chanceOfCorrelation=p[0]
                                
                # Write your results to disk in the file for this child process                
                writer.writerow([StockA,StockB,chanceOfCorrelation])
                
            except Exception as e : 
                # Send this exception message back to the parent host
                print(stocks_path)
                print(e)
                queueOut.put([None,e])
                continue
            finally:
                # The host process is blocking until all these signals are returned for each message
                # We do this in a finally so the host processes does not get hund on an error or early return
                # We also wait until all data is writen to disk so we do not accidently leave data out
                queueIn.task_done()
                
    print('child worker done: ' + str(datetime.now()))
    
    
    
    

def job_function(jobID):
    import json
    import os.path
    import pandas as pd
    import numpy as np
    
    # Setup paths for this job
    input_data_path = "e:\\job{}\\job{}_input_data.csv".format(jobID,jobID)
    output_path_csv="e:\\job{}\\job{}_final_output.csv".format(jobID,jobID)
    output_path_h5="e:\\job{}\\job{}.h5".format(jobID,jobID)
    output_path_children="e:\\job{}\\output_path_children\\".format(jobID)
    symbols_path="e:\\job{}\\nasdaq\\".format(jobID)
    
    # Create the child output folder if it does not exist
    if not os.path.exists(output_path_children):
        try:
            os.makedirs(output_path_children)
        except OSError as e:
            if e.errno != errno.EEXIST:
                print(e)
                return
            
    # Delete any child output files if they exist
    # Read all child process output data into the HDF5 file as a table
    directory = os.fsencode(output_path_children)
    for file in os.listdir(directory):
        filename = os.path.join(output_path_children, file.decode("utf-8"))
        try:
            os.remove(filename)
        except:
            pass

    # Delete the HDF5 file if it already exists
    if os.path.exists(output_path_h5):
        try:
            os.remove(output_path_h5)
        except OSError as e:
                print("Error deleting h5")
                print(e)
                return
    
    # Scale by offloading work to background process workers
    # Set max amount of concurrent processes
    processes = int(round(multiprocessing.cpu_count()*0.75))
    
    # Setup message queues
    queueIn = multiprocessing.JoinableQueue(10000)
    queueOut = multiprocessing.Queue(10000)
    
    # Launch child processes
    for _ in range(processes): 
        multiprocessing.Process(target=ChildProcessMessages, args=(queueIn,queueOut,output_path_children,symbols_path)).start()
    
    try:
        # Read in data to be processed
        
        #input_rows = pd.read_csv(input_data_path, delimiter=",", quotechar='"',usecols=["ParamA","ParamB","ParamC"])
        
        # Start ==================================
        print('Start: ' + str(datetime.now()))
        
        # Get symbol list from file names in folder
        symbols = []
        directory = os.fsencode(symbols_path)
        print(directory)
        circuit = 0
        for file in os.listdir(directory):
            filename = os.path.join(symbols_path, file.decode("utf-8"))
            symbol = file.decode("utf-8").replace(".csv","")
            symbols.append(symbol)
        
        symbols = np.array(symbols)
            
        for row in symbols:
            Symbol=row.strip()
            Symbol2=None
            # Queue all the work to be done for this Symbol vs all other Symbols
            for row2 in symbols:
                Symbol2=row2.strip()
                if Symbol != Symbol2:                       
                        queueIn.put((Symbol,Symbol2))
                        
                        
        
        
        
        
        
    except Exception as e:
        print(e)
        pass
    

    
    # Wait for the job queue to be drained
    queueIn.join()
    print("Queue Empty")
    
    # Stop child processes by sending in kill messages which will be processed last
    for _ in range(processes): queueIn.put((None,None))
    
    
    # Get pointer to data store file
    # We will accumulate all the final output data into an HDF5 file and then from there output a final csv file
    store=pd.HDFStore(output_path_h5)
    
    # Read all child process output data into the HDF5 file as a table
    directory = os.fsencode(output_path_children)
    for file in os.listdir(directory):
        filename = os.path.join(output_path_children, file.decode("utf-8"))
        # Read in the output rows from one child worker process
        dataFrame=pd.read_csv(filename, names=["StockA", "StockB", "Value"])
        # Append rows to table with maximum expected value byte lengths defined
        store.append('table', dataFrame, min_itemsize={'StockA': 32,'StockB': 32,'Value': 32})
        continue
    
    store.close()
    print("HDF5 Updated")
    
    # Now export the table from the HDF5 to a csv file for the final merged output
    # A WHERE clause can be added to the read_hdf if we would like to export only some of the rows...
    df=pd.read_hdf(output_path_h5, 'table')
    df.to_csv(output_path_h5.replace(".h5",".csv"), index=False)
    
    print("Output: " + output_path_h5.replace(".h5",".csv"))
    print('End: ' + str(datetime.now()))

# This if name==main ensures this code only runs in the main process 
# and not in the child processes that also launch this py file
# __name__ is a special python loading variable
if __name__ == '__main__':    
    job_function(jobID=5)    
