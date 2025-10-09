import os
import json
import pymongo
import pandas as pd
from typing import Any
from pathlib import Path
from gridfs import GridFS
from datetime import datetime
from src.errors import errorsLog

class cenmigDBMetaData:
    def __init__(self):
        self.errorsLogFun = errorsLog()
        self.main = os.path.dirname(os.path.realpath(__file__)) + '/'
        with open("config.json", 'r') as f:
            config = json.load(f)
            config = config["cenmigDB"]
        self.verbose = config["verbose"]
        self.keepLog = config["keepLog"]
        self.upsert = config["upsert"]
        self.index_column = config["index_column"]
        self.host = config["host"]
        self.username = config["username"]
        self.password = config["password"]

    def connect_mongodb(self):
        client = pymongo.MongoClient(host=self.host ,username=self.username,password=self.password)
        return client

    def get_update_database(self) -> str:
        client = self.connect_mongodb()
        db = client['update_data']
        metadata_database = db["update_date"]
        data = metadata_database.find_one(sort=[('_id', -1)])
        if data and 'dateField' in data:
            formatted_date = data['dateField'].strftime('%Y/%m/%d')
            return formatted_date
        else:
            if self.verbose:
                print("No Data Found!")
            return "1999/01/01"
    
    def update_date(self,formatted_date: str) -> None:
        client = self.connect_mongodb()
        db = client['update_data']
        metadata_database = db["update_date"]
        python_datetime = datetime.strptime(formatted_date, "%Y/%m/%d")
        formatted_new_date = python_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        formatted_new_date = datetime.strptime(formatted_new_date, '%Y-%m-%dT%H:%M:%S.%fZ')
        metadata_database.insert_one({'dateField': formatted_new_date})

    def update_metadata_one(self,row: pd.Series,mlst_data: pd.DataFrame,tb_data: pd.DataFrame,resistance_data: pd.DataFrame,point_data: pd.DataFrame) -> None:
        try:
            client = self.connect_mongodb()
            db = client['metadata']
            metadata_database = db["bacteria"] 
            dictMain = row.dropna(how ='all')
            dictMain = dictMain.astype('string')
            if '_id' in dictMain.index:
                dictMain = dictMain.drop('_id')
            dictMain = dictMain.to_dict()
            if  mlst_data.shape[0] > 0:
                mlst_data = mlst_data[['cenmigID','ST','mlst_run_date']]
                mlst_data = mlst_data.astype('string')
                mlst_data = mlst_data.loc[[0]]
                mlst_data_dict = mlst_data.to_dict()
                dictMain.update(mlst_data_dict)
            if  tb_data.shape[0] > 0:
                tb_data = tb_data[['cenmigID','wg_snp_lineage_assignment','DR_Type','tb_profiler_run_date']]
                tb_data = tb_data.astype('string')
                tb_data = tb_data.iloc[[0]]
                tb_data_dict = tb_data.to_dict()
                dictMain.update(tb_data_dict)
            if  resistance_data.shape[0] > 0:
                resistance_data = resistance_data.astype('string')
                resistance_data = resistance_data.iloc[[0]]
                resistance_data_dict = resistance_data.to_dict()
                dictMain.update(resistance_data_dict)
            if  point_data.shape[0] > 0:
                point_data = point_data.astype('string')
                point_data = point_data.iloc[[0]]
                point_data_dict = point_data.to_dict()
                dictMain.update(point_data_dict)
            metadata_database.update_one({self.index_column : str(dictMain[self.index_column])}, {'$set' : dictMain}, upsert= self.upsert)
        except Exception as e:
            if self.verbose:
                print(f"Error in update_metadata_one : {e}",e)
            if self.keepLog:
                self.errorsLogFun.error_logs_try("Error in update_metadata_one : ",e)

    def update_mlst_resistance_one(self,df_all_mlst: pd.DataFrame, df_all_resfinder: pd.DataFrame, df_all_pointfinder: pd.DataFrame,df_all_tb_profiler: pd.DataFrame) -> None:
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
            if self.verbose:
                print(f"Error in update_mlst_resistance_one : {e}",e)
            if self.keepLog:
                self.errorsLogFun.error_logs_try("Error in update_mlst_resistance_one : ",e)

    def update_record_by_csv(self,df_update_metadata: pd.DataFrame) -> None:
        client = self.connect_mongodb()
        db = client['metadata']
        metadata_database = db["bacteria"]
        data = metadata_database.find({}, {'_id': 0})
        data = pd.DataFrame(data)
        cenmigID_update = df_update_metadata['cenmigID'].dropna()
        list_cenmigID_update = list(cenmigID_update)
        new_metadata_old = data[data['cenmigID'].isin(list_cenmigID_update)]
        row_count = df_update_metadata.shape[0]
        if self.verbose:
            print(f"New data to update: {row_count} rows")
        for _ , row in df_update_metadata.iterrows():
            update_dict = row.dropna(how ='all')
            if '_id' in update_dict.index:
                update_dict = update_dict.drop('_id')
            update_dict = update_dict.to_dict()
            metadata_database.update_one({self.index_column : str(row[self.index_column])}, {'$set' : update_dict}, upsert= self.upsert)
        new_metadata_old.to_csv("old_metadata.csv",index=False)
        if self.verbose:
            print("Old Metadata Saved!")

    def del_records_by_csv(self,csv_file_delete: pd.DataFrame) -> None:
        client = self.connect_mongodb()
        db = client['metadata']
        metadata_database = db["bacteria"]
        data_cursor = metadata_database.find({}, {'_id': 0})
        data = pd.DataFrame(data_cursor)
        cenmigID_update = csv_file_delete['cenmigID'].dropna()
        list_cenmigID_update = list(cenmigID_update)
        new_metadata_old = data[~data['cenmigID'].isin(list_cenmigID_update)]
        row_count = csv_file_delete.shape[0]
        if self.verbose:
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
        if self.verbose:
            print("Deleted Data Completed!")

class cenmigDBGridFS:
    def __init__(self):
        self.main = os.path.dirname(os.path.realpath(__file__)) + '/'
        with open("config.json", 'r') as f:
            config = json.load(f)
            config = config["cenmigDB"]
        self.cenmigDB = cenmigDBMetaData()
    
    def update_item_to_db(self,file_name: str,location: Path) -> Any:
        client = self.cenmigDB.connect_mongodb()
        db = client['sequence']
        fs = GridFS(db)
        pathFile = os.path.join(location,str(file_name))
        with open(pathFile, "rb") as f:
            file_id = fs.put(f, filename=file_name)
        client.close()
        return file_id

    def get_item_from_db(self,file_name: str,location: Path) -> bool:
        client = self.cenmigDB.connect_mongodb()
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







