import os
import pymongo
import pandas as pd
import socket
from dotenv import load_dotenv
from datetime import datetime
# from datetime import timedelta,datetime
from gridfs import GridFS
import json
from src.errors import errorsLog

class cenmigDBMetaData():
    def __init__(self,
            ):
        self.errorsLogFun = errorsLog()
        self.main = os.path.dirname(os.path.realpath(__file__)) + '/'
        with open("config.json", 'r') as f:
            config = json.load(f)
            config = config["cenmigDB"]
        self.keepLog = config["keepLog"]
        self.upsert = config["upsert"]
        self.index_column = config["index_column"]
        self.host = config["host"]
        self.username = config["username"]
        self.password = config["password"]

    def connect_mongodb(self):
        client = pymongo.MongoClient(host=self.host ,username=self.username,password=self.password)
        return client

    def get_update_database(self):
        client = self.connect_mongodb()
        db = client['update_data']
        metadata_database = db["update_date"]
        data = metadata_database.find_one(sort=[('_id', -1)])
        formatted_date = data['dateField'].strftime('%Y/%m/%d')
        return formatted_date
    
    def update_date(self,formatted_date):
        client = self.connect_mongodb()
        db = client['update_data']
        metadata_database = db["update_date"]
        python_datetime = datetime.strptime(formatted_date, "%Y/%m/%d")
        formatted_new_date = python_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        formatted_new_date = datetime.strptime(formatted_new_date, '%Y-%m-%dT%H:%M:%S.%fZ')
        metadata_database.insert_one({'dateField': formatted_new_date})

    def update_metadata_one(self,df,index_,mlst_data,tb_data,resistance_data,point_data):
        try:
            client = self.connect_mongodb()
            db = client['metadata']
            metadata_database = db["bacteria"] 
            data_i = df.iloc[index_]
            dictMain = data_i.dropna(how ='all')
            dictMain = dictMain.astype('string')
            if '_id' in dictMain.index:
                dictMain = dictMain.drop('_id')
            dictMain = dictMain.to_dict()
            if  mlst_data.shape[0] > 0:
                mlst_data = mlst_data[['cenmigID','ST','mlst_run_date']]
                mlst_data = mlst_data.astype('string')
                mlst_data = mlst_data.iloc[0].to_dict()
                dictMain.update(mlst_data)
            if  tb_data.shape[0] > 0:
                tb_data = tb_data[['cenmigID','wg_snp_lineage_assignment','DR_Type','tb_profiler_run_date']]
                tb_data = tb_data.astype('string')
                tb_data = tb_data.iloc[0].to_dict()
                dictMain.update(tb_data)
            if  resistance_data.shape[0] > 0:
                resistance_data = resistance_data.astype('string')
                resistance_data = resistance_data.iloc[0].to_dict()
                dictMain.update(resistance_data)
            if  point_data.shape[0] > 0:
                point_data = point_data.astype('string')
                point_data = point_data.iloc[0].to_dict()
                dictMain.update(point_data)
            metadata_database.update_one({self.index_column : str(dictMain[self.index_column])}, {'$set' : dictMain}, upsert= self.upsert)
        except Exception as e:
            if self.keepLog:
                self.errorsLogFun.error_logs_try("Error in updateMlst_Tb_ResfinderOne : ",e)

    def update_mlst_resistance_one(self,df_all_mlst, df_all_resfinder, df_all_pointfinder,df_all_tb_profiler):
        try:
            client = self.connect_mongodb()
            db = client['metadata']
            
            if df_all_mlst.shape[0] > 0:
                metadata_database_mlst = db["mlst"]
                for _ , row in df_all_mlst.iterrows():
                    mlst_update_dict = row.dropna(how ='all')
                    mlst_update_dict = mlst_update_dict.astype('string').replace({pd.NA: None})
                    if '_id' in mlst_update_dict.index:
                        mlst_update_dict = mlst_update_dict.drop('_id')
                    mlst_update_dict = mlst_update_dict.to_dict()
                    metadata_database_mlst.update_one({self.index_column: str(row[self.index_column])}, {'$set': mlst_update_dict}, upsert=self.upsert)

            if df_all_resfinder.shape[0] > 0:
                df_all_resfinder = df_all_resfinder.astype('string').replace({pd.NA: None})
                metadata_database_resfinder = db["drug_resistance"]
                cenmig_id_resfinder = df_all_resfinder[self.index_column]
                metadata_database_resfinder.delete_many({self.index_column: {'$in': list(set(cenmig_id_resfinder))}})
                df_all_resfinder_dict = df_all_resfinder.to_dict('records')
                metadata_database_resfinder.insert_many(df_all_resfinder_dict)
            
            if df_all_pointfinder.shape[0] > 0:
                df_all_pointfinder = df_all_pointfinder.astype('string').replace({pd.NA: None})
                metadata_database_point = db["point_mutation"]
                cenmig_id_mutation = df_all_pointfinder[self.index_column]
                metadata_database_point.delete_many({self.index_column: {'$in': list(set(cenmig_id_mutation))}})
                df_all_pointfinder_dict = df_all_pointfinder.to_dict('records')
                metadata_database_point.insert_many(df_all_pointfinder_dict)
            
            if df_all_tb_profiler.shape[0] > 0:
                metadata_database_tbpro = db["tb_profiler"]
                df_all_tb_profiler = df_all_tb_profiler.astype('string').replace({pd.NA: None})
                cenmig_id_tbprofiler = df_all_tb_profiler[self.index_column]
                try:
                    metadata_database_tbpro.delete_many({self.index_column: {'$in': list(set(cenmig_id_tbprofiler))}})
                except:
                    pass
                for _, row in df_all_tb_profiler.iterrows():
                    update_dict = row.dropna(how='all')
                    update_dict = update_dict.astype('string').replace({pd.NA: None})
                    if '_id' in update_dict.index:
                        update_dict = update_dict.drop('_id')
                    update_dict = update_dict.to_dict()
                    metadata_database_tbpro.update_one({self.index_column: str(row[self.index_column])}, {'$set': update_dict}, upsert=self.upsert)

        except Exception as e:
            if self.keepLog:
                self.errorsLogFun.error_logs_try("Error in updateMlst_Tb_ResfinderOne : ",e)

    def update_record(self,df_update_metadata):
        client = self.connect_mongodb()
        db = client['metadata']
        collection_names = db.list_collection_names()
        # If the connection is successful, the collection_names will contain the list of collections in the 'test' database
        if collection_names:
            print("Connection to 'cenmigDB' database successful.")
        else:
            print("Connection to 'cenmigDB' database failed.")
        metadata_database = db["bacteria"]
        data = metadata_database.find({}, {'_id': 0})
        data = pd.DataFrame(data)
        cenmigID_update = df_update_metadata['cenmigID'].dropna()
        list_cenmigID_update = list(cenmigID_update)
        new_metadata_old = data[data['cenmigID'].isin(list_cenmigID_update)]
        row_count = df_update_metadata.shape[0]
        print(f"New data to update: {row_count} rows")
        for _ , row in df_update_metadata.iterrows():
            update_dict = row.dropna(how ='all')
            if '_id' in update_dict.index:
                update_dict = update_dict.drop('_id')
            update_dict = update_dict.to_dict()
            metadata_database.update_one({self.index_column : str(row[self.index_column])}, {'$set' : update_dict}, upsert= self.upsert)
        new_metadata_old.to_csv(".old_metadata.csv",index=False)
        print("Old Metadata Saved!")
        print('Update data to MongoDB Completed')

    def del_records_by_csv(self,csv_file_delete):
        client = self.connect_mongodb()
        db = client['metadata']
        collection_names = db.list_collection_names()
        # If the connection is successful, the collection_names will contain the list of collections in the 'test' database
        if collection_names:
            print("Connection to 'cenmigDB' database successful.")
        else:
            print("Connection to 'cenmigDB' database failed.")
        metadata_database = db["bacteria"]
        data = metadata_database.find({}, {'_id': 0})
        cenmigID_update = csv_file_delete['cenmigID'].dropna()
        list_cenmigID_update = list(cenmigID_update)
        new_metadata_old = data[~data['cenmigID'].isin(list_cenmigID_update)]
        row_count = csv_file_delete.shape[0]
        print(f"Deleting Data: {row_count} rows")
        for _ , row in csv_file_delete.iterrows():
            try:
                update_dict = row.dropna(how ='all')
                if '_id' in update_dict.index:
                    update_dict = update_dict.drop('_id')
                update_dict = update_dict.to_dict()
                metadata_database.delete_one({self.index_column : str(row[self.index_column])})
            except Exception as e:
                print("Error! -> :",e)
                print("No {} Record in database".format(row[self.index_column]))
        new_metadata_old.to_csv(".old_for_delete_metadata.csv",index=False)
        print("Deleted Data Completed!")

class cenmigDBGridFS():
    def __init__(self,
        ):
        self.main = os.path.dirname(os.path.realpath(__file__)) + '/'
        with open("config.json", 'r') as f:
            config = json.load(f)
            config = config["cenmigDB"]
        cenmigDB = cenmigDBMetaData()
        self.connect_mongodb = cenmigDB.connect_mongodb()
    
    def update_item_to_db(self,file_name,location):
        client = self.connect_mongodb()
        db = client['sequence']
        fs = GridFS(db)
        pathFile = os.path.join(location,str(file_name))
        with open(pathFile, "rb") as f:
            file_id = fs.put(f, filename=file_name)
        client.close()
        return file_id

    def get_item_from_db(self,file_name,location):
        client = self.connect_mongodb()
        db = client['sequence']
        fs = GridFS(db)
        file_doc = fs.find_one({"filename": file_name})
        fileSave = os.path.join(location,str(file_name))
        if file_doc:
            with fs.get(file_doc._id) as file_data:
                with open(fileSave, "wb") as f:
                    f.write(file_data.read())
            client.close()
            return True
        else:
            client.close()
            return False







