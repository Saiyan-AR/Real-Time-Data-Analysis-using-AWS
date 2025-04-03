 # necessary imports
from datetime import datetime
import pandas as pd
import boto3
import time


# This generated a client based on server and region name
def gen_client(ser, reg):
    return boto3.client(ser, region_name=reg)

# Reading the file and returning a dataframe
def get_data(filename, f_type):
    if f_type = 'csv':
        #reading csv
        df = pd.read_csv(filename)
    elif f_type = 'excel':
        #reading excel
        df = pd.read_excel(filename)
    else:
        print('Incorrect File Type')
    return df


# code to prepare data to send to kinesis client
def send_kinesis(client, stream_name, shard_count, df):
    
    # len_rows = length of rows of input data
    # len_columns = length of columns of input data
    # C_bytes = Bytes count
    # shard_counter = shard counter
    # sendKinesis = 1 if we have to send the batch of data, 0 if we do not have to send the data


    kinesisRecords = []
    len_rows = len(df.axes[0])
    len_columns = rows = len(df.axes[1])
    C_bytes = 0
    rowCount = 0
    sendKinesis = 0    
    shard_counter = 1

    #Iterating over all the rows in the dataframe
    for _, row in df.iterrows(): 


        values = '|'.join(str(value) for value in row)

        #encoding he values
        encodedValues = bytes(values, 'utf-8')

        #creating a dictionary (Hashmap) of the data 
        kinesisRecord = {
            "Data": encodedValues, # encoded data
            "PartitionKey": str(shard_counter) # Kinesis shard number to be used with the batch
        }


        kinesisRecords.append(kinesisRecord)
        stringBytes = len(values.encode('utf-8'))
        C_bytes = C_bytes + stringBytes

        # conditions to send the data

        # if the len of dictionary is 15
        if len(kinesisRecords) == 15:
            sendKinesis = 1        

        # if we reach the last record
        if rowCount == len_rows - 1:
            sendKinesis = 1

        #if the byte size is greater than 20000
        if C_bytes > 20000:
            sendKinesis = 1

        # approval to send the data
        if sendKinesis == 1:
            
            # adding the data to kinesis
            response = client.put_records(
                Records=kinesisRecords,
                StreamName = stream_name
            )
            print(kinesisRecords)
            
            #resetting the values
            kinesisRecords = [] 
            sendKinesis = 0 
            C_bytes = 0
            
            # Incrementing the shard counter
            shard_counter += 1
        
            
            # If reached max, reset it to 1
            if shard_counter > shard_count:
                shard_counter = 1

            # If the response is not 200, print the error
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            	print('Error!')

            print(rowCount)

            
        # Incrementing the row counter
        rowCount = rowCount + 1
    
    
    print('Total Records sent to Kinesis: {0}'.format(totalRowCount))


def main():
    
    # Start time
    start = datetime.now()
    
    # setting the client
    kinesis = gen_client('kinesis','us-east-1')
    
    #Reading the data
    data = get_data('C:/Users/Onkar/Downloads/bank-cropped.csv','csv')
    
    send_kinesis(kinesis, "data-stream", 1, data) 
    
    # End time
    end = datetime.now()

    #Calculating the final time
    final_time = (end - start).total_seconds() * 10**3
    
    print("Runtime: " + final_time)
    
if __name__ == "__main__":
    
    main()
