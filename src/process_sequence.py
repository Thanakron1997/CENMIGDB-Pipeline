import os
import json
import shutil
import pandas as pd
import multiprocessing
from tqdm import tqdm
from multiprocessing import Queue as MPQueue
from typing import Callable, Any,List,Tuple
from src.download_sequence import downloadSEQ
from src.find_st import findST
from src.errors import errorsLog
from src.find_resistance  import findResistance
from src.process_cenmigDB import cenmigDBMetaData

def cleanDfList(results: List[List[pd.DataFrame]]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    list_mlst_result = []
    list_resistance_result =[]
    list_pointmutation_result = []
    list_tb_profiler_result = []
    list_resistance_one_line = []
    list_pointmultation_one_line = []
    for list_df_i in results:
        list_mlst_result.append(list_df_i[0])
        list_resistance_result.append(list_df_i[1])
        list_pointmutation_result.append(list_df_i[2])
        list_tb_profiler_result.append(list_df_i[3])
        list_resistance_one_line.append(list_df_i[4])
        list_pointmultation_one_line.append(list_df_i[5])
    try:
        list_mlst_result = [df for df in list_mlst_result if df.shape[0] != 0]
        list_resistance_result = [df for df in list_resistance_result if df.shape[0] != 0]
        list_pointmutation_result = [df for df in list_pointmutation_result if df.shape[0] != 0]
        list_tb_profiler_result = [df for df in list_tb_profiler_result if df.shape[0] != 0]
        list_resistance_one_line = [df for df in list_resistance_one_line if df.shape[0] != 0]
        list_pointmultation_one_line = [df for df in list_pointmultation_one_line if df.shape[0] != 0]
    except:
        pass
    if len(list_mlst_result) >0:
        df_all_mlst_result = pd.concat(list_mlst_result, ignore_index=True) 
    else:
        df_all_mlst_result = pd.DataFrame()
    if len(list_resistance_result) >0:
        df_all_resistance_result = pd.concat(list_resistance_result,ignore_index=True)
    else:
        df_all_resistance_result = pd.DataFrame()
    if len(list_pointmutation_result) > 0:
        df_all_pointmultation_result = pd.concat(list_pointmutation_result,ignore_index=True)
    else: 
        df_all_pointmultation_result = pd.DataFrame()
    if len(list_tb_profiler_result) > 0:
        df_all_tb_profiler_result = pd.concat(list_tb_profiler_result,ignore_index=True)
    else:
        df_all_tb_profiler_result = pd.DataFrame()
    if len(list_resistance_one_line) > 0:
        df_all_resistance_one_line = pd.concat(list_resistance_one_line,ignore_index=True)
    else:
        df_all_resistance_one_line = pd.DataFrame()
    if len(list_pointmultation_one_line) > 0:
        df_all_pointmultation_one_line = pd.concat(list_pointmultation_one_line,ignore_index=True)
    else:
        df_all_pointmultation_one_line = pd.DataFrame()
    return df_all_mlst_result,df_all_resistance_result,df_all_pointmultation_result,df_all_tb_profiler_result,df_all_resistance_one_line,df_all_pointmultation_one_line

class processRawSeqData:
    def __init__(self):
        self.errorsLogFun = errorsLog()
        self.download = downloadSEQ()
        self.findst = findST()
        self.findresistance = findResistance()
        self.cenmigDB = cenmigDBMetaData()
        self.main = os.path.dirname(os.path.realpath(__file__)) + '/'
        with open("config.json", 'r') as f:
            config = json.load(f)
            config = config["processRawData"]
        self.verbose = config["verbose"]
        self.keepLog = config["keepLog"]
        self.removeDir = config["removeDir"]
        self.coreUsed = config["coreUsed"]
        tmpProcessDir = config["tmpProcessDir"]
        self.tmpProcessDir = os.path.join(self.main,tmpProcessDir)
        if not os.path.exists(self.tmpProcessDir):
            os.mkdir(self.tmpProcessDir)


    def process_data(self,args) -> list[pd.DataFrame]:
        try:
            index_i,row = args
            id_i = str(row['Run'])
            organism_i = row['Organism']
            platform_i = row['Platform']
            output_dir_i = os.path.join(self.tmpProcessDir,id_i)
            if pd.isna(platform_i) or platform_i == "":
                platform_i = "illumina"
            if not os.path.exists(output_dir_i):
                os.mkdir(output_dir_i)

            seq_files_list = self.download.download_seq_fastq(id_i,platform_i,output_dir_i)
            df_mlst_result = self.findst.run_mlst_raw_seq(id_i,organism_i,seq_files_list,platform_i,output_dir_i)
            df_resfinder_raw_i,df_pointfinder_raw_i,df_resfinder_line_i,df_pointfinder_line_i,df_tb_profiler = self.findresistance.process_raw_seq(id_i,organism_i,seq_files_list,platform_i,output_dir_i)
            self.cenmigDB.update_metadata_one(row,df_mlst_result,df_tb_profiler,df_resfinder_line_i,df_pointfinder_line_i)
            self.cenmigDB.update_mlst_resistance_one(df_mlst_result, df_resfinder_raw_i, df_pointfinder_raw_i,df_tb_profiler)
            if self.removeDir:
                if os.path.exists(output_dir_i):
                    shutil.rmtree(output_dir_i, ignore_errors = False)
            return [df_mlst_result,df_resfinder_raw_i,df_pointfinder_raw_i,df_tb_profiler,df_resfinder_line_i,df_pointfinder_line_i]
        
        except Exception as e:
            if self.keepLog:
                self.errorsLogFun.error_logs_try("Error in RawSeqData - process_data",e)
            df_mlst_result = pd.DataFrame()
            df_resfinder_raw_i = pd.DataFrame()
            df_pointfinder_raw_i = pd.DataFrame()
            df_tb_profiler = pd.DataFrame()
            df_resfinder_line_i = pd.DataFrame()
            df_pointfinder_line_i = pd.DataFrame()
            return [df_mlst_result,df_resfinder_raw_i,df_pointfinder_raw_i,df_tb_profiler,df_resfinder_line_i,df_pointfinder_line_i]
    
    def process_data_inhouse(self,args) -> list[pd.DataFrame]:
        try:
            index_i,row = args
            id_i = str(row['Run'])
            organism_i = row['Organism']
            platform_i = row['Platform']
            file_name_i = row['file_name']
            output_dir_i = os.path.join(self.tmpProcessDir,id_i)
            if pd.isna(platform_i) or platform_i == "":
                platform_i = "illumina"
            if not os.path.exists(output_dir_i):
                os.mkdir(output_dir_i)
            lstFileName = file_name_i.split(", ")
            seq_files_list = self.download.download_seq_inhouse(id_i,lstFileName,output_dir_i)
            df_mlst_result = self.findst.run_mlst_raw_seq(id_i,organism_i,seq_files_list,platform_i,output_dir_i)
            df_resfinder_raw_i,df_pointfinder_raw_i,df_resfinder_line_i,df_pointfinder_line_i,df_tb_profiler = self.findresistance.process_raw_seq(id_i,organism_i,seq_files_list,platform_i,output_dir_i)
            self.cenmigDB.update_metadata_one(row,df_mlst_result,df_tb_profiler,df_resfinder_line_i,df_pointfinder_line_i)
            self.cenmigDB.update_mlst_resistance_one(df_mlst_result, df_resfinder_raw_i, df_pointfinder_raw_i,df_tb_profiler)
            if self.removeDir:
                if os.path.exists(output_dir_i):
                    shutil.rmtree(output_dir_i, ignore_errors = False)
            return [df_mlst_result,df_resfinder_raw_i,df_pointfinder_raw_i,df_tb_profiler,df_resfinder_line_i,df_pointfinder_line_i]
        
        except Exception as e:
            if self.keepLog:
                self.errorsLogFun.error_logs_try("Error in RawSeqData - process_data",e)
            df_mlst_result = pd.DataFrame()
            df_resfinder_raw_i = pd.DataFrame()
            df_pointfinder_raw_i = pd.DataFrame()
            df_tb_profiler = pd.DataFrame()
            df_resfinder_line_i = pd.DataFrame()
            df_pointfinder_line_i = pd.DataFrame()
            return [df_mlst_result,df_resfinder_raw_i,df_pointfinder_raw_i,df_tb_profiler,df_resfinder_line_i,df_pointfinder_line_i]
        
    def worker_func(self,job_queue: MPQueue,result_queue: MPQueue,processfunction: Callable[[Any], list[pd.DataFrame]]):
        while True:
            job = job_queue.get()
            if job is None:
                break
            result = processfunction(job)
            result_queue.put(result)

    def multi_process_data(self,df: pd.DataFrame, inhouse: bool =False) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        job_queue = multiprocessing.Queue()
        result_queue = multiprocessing.Queue()
        results = []
        if inhouse:
            process_func = self.process_data_inhouse
        else:
            process_func = self.process_data
        pool = multiprocessing.Pool(processes=self.coreUsed, initializer=self.worker_func, initargs=(job_queue, result_queue,process_func)) 
        df_index = df.index.tolist() # Get the DataFrame index as a list
        jobs = [(index_, df.loc[index_]) for index_ in df_index] # Enqueue jobs (each job is a tuple of sra_index and df_sra)
        with tqdm(total=len(jobs), desc="Processing sra data: ", ncols=100 ,colour="#00FF21",leave=True) as pbar:
            for job in jobs: # add job to queue the job will start but can add job
                job_queue.put(job)
            for _ in range(self.coreUsed): # Add sentinel values to signal workers to exit (add last job with None for let process can exit)
                job_queue.put(None)
            
            for _ in range(len(jobs)): # get result by total job 
                result = result_queue.get()
                results.append(result)
                pbar.update(1) # update process bar
        pool.close()
        pool.join()
        df_all_mlst_result,df_all_resistance_result,df_all_pointmultation_result,df_all_tb_profiler_result,df_all_resistance_one_line,df_all_pointmultation_one_line = cleanDfList(results)
        
        return df_all_mlst_result,df_all_resistance_result,df_all_pointmultation_result,df_all_tb_profiler_result,df_all_resistance_one_line,df_all_pointmultation_one_line

class processAssemblyData:
    def __init__(self):
        self.errorsLogFun = errorsLog()
        self.download = downloadSEQ()
        self.findst = findST()
        self.findresistance = findResistance()
        self.cenmigDB = cenmigDBMetaData()
        self.main = os.path.dirname(os.path.realpath(__file__)) + '/'
        with open("config.json", 'r') as f:
            config = json.load(f)
            config = config["processAssemblyData"]
        self.verbose = config["verbose"]
        self.keepLog = config["keepLog"]
        self.removeDir = config["removeDir"]
        self.coreUsed = config["coreUsed"]
        tmpProcessDir = config["tmpProcessDir"]
        self.tmpProcessDir = os.path.join(self.main,tmpProcessDir)
        if not os.path.exists(self.tmpProcessDir):
            os.mkdir(self.tmpProcessDir)
    
    def process_data(self,args) -> list[pd.DataFrame]:
        try:
            index_i,row = args
            id_i = str(row['asm_acc'])
            organism_i = row['Organism']
            output_dir_i = os.path.join(self.tmpProcessDir,id_i)
            if not os.path.exists(output_dir_i):
                os.mkdir(output_dir_i)
            seq_file = self.download.download_seq_assembly(id_i,output_dir_i)
            if seq_file:
                df_mlst_result = self.findst.run_mlst_assembly_seq(id_i,organism_i,seq_file,output_dir_i)
                df_resfinder_raw_i,df_pointfinder_raw_i,df_resfinder_line_i,df_pointfinder_line_i,df_tb_profiler = self.findresistance.process_assembly_seq(id_i,organism_i,[seq_file],output_dir_i)
                self.cenmigDB.update_metadata_one(row,df_mlst_result,df_tb_profiler,df_resfinder_line_i,df_pointfinder_line_i)
                self.cenmigDB.update_mlst_resistance_one(df_mlst_result, df_resfinder_raw_i, df_pointfinder_raw_i,df_tb_profiler)
            else:
                raise Exception("No sequences files found!")
            if self.removeDir:
                if os.path.exists(output_dir_i):
                    shutil.rmtree(output_dir_i, ignore_errors = False)
            return [df_mlst_result,df_resfinder_raw_i,df_pointfinder_raw_i,df_tb_profiler,df_resfinder_line_i,df_pointfinder_line_i]
        except Exception as e:
            if self.keepLog:
                self.errorsLogFun.error_logs_try("Error in AssemblyData - process_data",e)
            df_mlst_result = pd.DataFrame()
            df_resfinder_raw_i = pd.DataFrame()
            df_pointfinder_raw_i = pd.DataFrame()
            df_tb_profiler = pd.DataFrame()
            df_resfinder_line_i = pd.DataFrame()
            df_pointfinder_line_i = pd.DataFrame()
            return [df_mlst_result,df_resfinder_raw_i,df_pointfinder_raw_i,df_tb_profiler,df_resfinder_line_i,df_pointfinder_line_i]
    
    def worker_func(self,job_queue: MPQueue,result_queue: MPQueue):
        while True: # Get a job from the job queue
            job = job_queue.get()
            if job is None: # None is used as a sentinel to signal the worker to exit
                break
            result = self.process_data(job) # Put the result in the result queue
            result_queue.put(result)
            
    def multi_process_data(self,df:pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        job_queue = multiprocessing.Queue()
        result_queue = multiprocessing.Queue()
        results = [] # Collect results as they become available
        pool = multiprocessing.Pool(processes=self.coreUsed, initializer=self.worker_func, initargs=(job_queue, result_queue)) 
        df_index = df.index.tolist() # Get the DataFrame index as a list
        jobs = [(index_, df.loc[index_]) for index_ in df_index] # Enqueue jobs (each job is a tuple of sra_index and df_sra)
        with tqdm(total=len(jobs), desc="Processing Assembly data: ", ncols=100, colour="#00FF21", leave=True) as pbar:
            for job in jobs: # add job to queue the job will start but can add job
                job_queue.put(job)
            for _ in range(self.coreUsed): # Add sentinel values to signal workers to exit (add last job with None for let process can exit)
                job_queue.put(None)
            
            for _ in range(len(jobs)): # get result by total job 
                result = result_queue.get()
                results.append(result)
                pbar.update(1) # update process bar
        pool.close()
        pool.join()
        df_all_mlst_result,df_all_resistance_result,df_all_pointmultation_result,df_all_tb_profiler_result,df_all_resistance_one_line,df_all_pointmultation_one_line = cleanDfList(results)
        
        return df_all_mlst_result,df_all_resistance_result,df_all_pointmultation_result,df_all_tb_profiler_result,df_all_resistance_one_line,df_all_pointmultation_one_line

class processAllSeqData:
    def __init__(self):
        self.errorsLogFun = errorsLog()
        self.processRawSeqData = processRawSeqData()
        self.processAssemblyData = processAssemblyData()
        self.main = os.path.dirname(os.path.realpath(__file__)) + '/'
        with open("config.json", 'r') as f:
            config = json.load(f)
            config = config["processAllSeqData"]
        self.verbose = config["verbose"]
        self.keepLog = config["keepLog"]
        self.cache = config["cache"]
        self.mlstFileResult = config["mlstFileResult"]
        self.resistanceFileResult = config["resistanceFileResult"]
        self.pointMultationFileResult = config["pointMultationFileResult"]
        self.tbProfilerFileResult = config["tbProfilerFileResult"]
        self.resistanceOneLineFileResult = config["resistanceOneLineFileResult"]
        self.pointMultationOneLineFileResult = config["pointMultationOneLineFileResult"]
        saveMetaPath = os.path.join(self.main,"result_metada")
        if not os.path.exists(saveMetaPath):
            os.mkdir(saveMetaPath)
    
    def process(self,df: pd.DataFrame) -> None:
        raw_seq = None
        assembly = None
        inhouse = None
        df_columns = df.columns
        if self.cache:
            from src.process_metadata import processMeta
            process_meta = processMeta()
            _, cenmigID_old,_ =  process_meta.get_old_data()
            df = df[~df['cenmigID'].isin(cenmigID_old)]
        # check Raw sequences
        if 'Run' in df_columns:
            list_raw_sequences = df['Run'].dropna().tolist()
            if len(list_raw_sequences) > 0:
                raw_seq = True
        else:
            list_raw_sequences = []
            if self.verbose:
                print("No Run Columns!")
        # check assembly sequences
        if 'asm_acc' in df_columns and 'Run' in df_columns:
            df_i = df[~df['Run'].isin(list_raw_sequences)]
            list_assembly = df_i['asm_acc'].dropna().tolist()
            if len(list_assembly) > 0:
                assembly = True
        else:
            list_assembly = []
            if self.verbose:
                print("No asm_acc Columns!")
        # check In-House
        if 'cenmigID' in df_columns:
            list_inhouse = df[df['cenmigID'].str.startswith('IH_')]['cenmigID'].tolist()
            if len(list_inhouse) > 0:
                inhouse = True
        else:
            list_inhouse = []
            if self.verbose:
                print("No Run cenmigID!")

        list_mlst = []
        list_resistance =[]
        list_pointmultation = []
        list_tb_profiler = []
        list_resistance_one_line = []
        list_pointmultation_one_line = []
        if raw_seq:
            df_raw_seq = df[df['Run'].isin(list_raw_sequences)].reset_index(drop=True)
            df_raw_seq_all_mlst_result,df_raw_seq_all_resistance_result,df_raw_seq_all_pointmultation_result,df_raw_seq_all_tb_profiler_result,df_raw_seq_all_resistance_one_line,df_raw_seq_all_pointmultation_one_line = self.processRawSeqData.multi_process_data(df_raw_seq)
            list_mlst.append(df_raw_seq_all_mlst_result)
            list_resistance.append(df_raw_seq_all_resistance_result)
            list_pointmultation.append(df_raw_seq_all_pointmultation_result)
            list_tb_profiler.append(df_raw_seq_all_tb_profiler_result)
            list_resistance_one_line.append(df_raw_seq_all_resistance_one_line)
            list_pointmultation_one_line.append(df_raw_seq_all_pointmultation_one_line)
        if assembly:
            df_assembly = df[df['asm_acc'].isin(list_assembly)].reset_index(drop=True)
            df_assembly_all_mlst_result,df_assembly_all_resistance_result,df_assembly_all_pointmultation_result,df_assembly_all_tb_profiler_result,df_assembly_all_resistance_one_line,df_assembly_all_pointmultation_one_line = self.processAssemblyData.multi_process_data(df_assembly)
            list_mlst.append(df_assembly_all_mlst_result)
            list_resistance.append(df_assembly_all_resistance_result)
            list_pointmultation.append(df_assembly_all_pointmultation_result)
            list_tb_profiler.append(df_assembly_all_tb_profiler_result)
            list_resistance_one_line.append(df_assembly_all_resistance_one_line)
            list_pointmultation_one_line.append(df_assembly_all_pointmultation_one_line)
        if inhouse:
            df_inhouse_raw_seq = df[df['cenmigID'].isin(list_inhouse)].reset_index(drop=True)
            df_inhouse_raw_seq_all_mlst_result,df_inhouse_raw_seq_all_resistance_result,df_inhouse_raw_seq_all_pointmultation_result,df_inhouse_raw_seq_all_tb_profiler_result,df_inhouse_raw_seq_all_resistance_one_line,df_inhouse_raw_seq_all_sra_pointmultation_one_line = self.processRawSeqData.multi_process_data(df_inhouse_raw_seq,inhouse=True)
            list_mlst.append(df_inhouse_raw_seq_all_mlst_result)
            list_resistance.append(df_inhouse_raw_seq_all_resistance_result)
            list_pointmultation.append(df_inhouse_raw_seq_all_pointmultation_result)
            list_tb_profiler.append(df_inhouse_raw_seq_all_tb_profiler_result)
            list_resistance_one_line.append(df_inhouse_raw_seq_all_resistance_one_line)
            list_pointmultation_one_line.append(df_inhouse_raw_seq_all_sra_pointmultation_one_line)
            
        list_mlst = [df for df in list_mlst if df.shape[0] != 0]
        list_resistance = [df for df in list_resistance if df.shape[0] != 0]
        list_pointmultation = [df for df in list_pointmultation if df.shape[0] != 0]
        list_tb_profiler = [df for df in list_tb_profiler if df.shape[0] != 0]
        list_resistance_one_line = [df for df in list_resistance_one_line if df.shape[0] != 0]
        list_pointmultation_one_line = [df for df in list_pointmultation_one_line if df.shape[0] != 0]

        df_all_mlst = pd.concat(list_mlst, ignore_index=True)
        df_all_resfinder = pd.concat(list_resistance, ignore_index=True)
        df_all_pointfinder = pd.concat(list_pointmultation,ignore_index=True)
        df_all_one_line_resfinder = pd.concat(list_resistance_one_line,ignore_index=True)
        df_all_one_line_pointfinder = pd.concat(list_pointmultation_one_line,ignore_index=True)
        df_all_mlst.to_csv(os.path.join(self.main,self.mlstFileResult),index=False)
        df_all_resfinder.to_csv(os.path.join(self.main,self.resistanceFileResult),index=False)
        df_all_pointfinder.to_csv(os.path.join(self.main,self.pointMultationFileResult),index=False)
        df_all_one_line_resfinder.to_csv(os.path.join(self.main,self.resistanceOneLineFileResult),index=False)
        df_all_one_line_pointfinder.to_csv(os.path.join(self.main,self.pointMultationOneLineFileResult),index=False)
        if len(list_tb_profiler) > 0:
            df_all_tb_profiler = pd.concat(list_tb_profiler,ignore_index=True)
            df_all_tb_profiler.to_csv(os.path.join(self.main,self.tbProfilerFileResult),index=False)

