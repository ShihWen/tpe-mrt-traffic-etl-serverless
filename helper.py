
import boto3
import pandas as pd
import os
import awswrangler as wr


def get_existing_file()->list:
    '''
    Get the file from url containing MRT traffic data in monthly basis.

    Args: 
        --
    
    Returns:
        A pandas dataframe with columns namely year-month and data url
    '''
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('online-data-lake-thirty-three')

    bucket_object = bucket.objects.filter(Prefix='mrt-traffic')
    objects = [obj.key.split('_')[1] for obj in bucket_object]

    return '_'.join(objects)


def get_mrt_traffic_data_link()->object:
    '''
    Get the file from url containing MRT traffic data in monthly basis.

    Args: 
        --
    
    Returns:
        A pandas dataframe with columns namely year-month and data url
    '''

    url = 'https://data.taipei/api/dataset/' \
      '63f31c7e-7fc3-418b-bd82-b95158755b4d' \
      '/resource/eb481f58-1238-4cff-8caa-fa7bb20cb4f4/download'

    # 設定每個cell可顯示字串長度
    # default = 50
    # pd.options.display.max_colwidth = 400
    return pd.read_csv(url, dtype = {'年月': str, '資料路徑 ': str})


def get_traffic(data_dt:str)->list:
    '''
    Download new MRT traffic data into S3 by comparing the exsiting datasets (data_dt) in the S3 
    and MRT dataset table from the MRT DataSource(df_source).

    Args:
        data_dt: A string of existing MRT traffic data in S3 bucket simplify by its date 
                 and connected with "_", returned by previous step.
                 For example, if there are 3 dataset currently in the bucket with data year-month as
                 202101, 202102 and 202103, then the recived string will be "202101_202102_202103".
    
    Returns:
        A list of downloaded file presents by year-month.  ex. 202101
    '''
    input_file_dt = []
    df_source = get_mrt_traffic_data_link()

    source_obj = list(df_source['年月'])    
    s3_existing_obj = data_dt.split('_')

    download_list = list(set(source_obj) - set(s3_existing_obj))
    download_df = df_source[df_source['年月'].isin(download_list)]

    # print(f"source_obj={source_obj}")
    # print(f"s3_existing_obj={s3_existing_obj}")
    # print(f"download_list={download_list}")

    df_columns = ['dt','hour','entrance','exit','traffic']
    df_dtype =  {'hour ': int, 
                 'entrance ': 'category', 
                 'exit ': 'category', 
                 'traffic ': int}

    for _, row in download_df.iterrows():
        print(f"processing mrt traffic data {row[0]}...")

        df_traffic = pd.read_csv(row[1], 
                                    skiprows=1, 
                                    names=df_columns, 
                                    parse_dates=['dt'], 
                                    dtype=df_dtype)
        wr.s3.to_parquet(
            df=df_traffic,
            path=os.environ['s3_target_bucket'],
            dataset=True,
            filename_prefix=f'mrt_{row[0]}_',
            database=os.environ['target_db'],
            table=os.environ['target_tbl'],
            mode=os.environ['write_data_operation']
        )
        input_file_dt.append( row[0] )

    return input_file_dt