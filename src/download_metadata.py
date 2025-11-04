import os
import glob
import time
import json
import random
import shutil
import requests
import subprocess
import pandas as pd
from tqdm import tqdm
from src.errors import errorsLog
from multiprocessing import Pool
from typing import Tuple,List,Set
from bs4 import BeautifulSoup,Tag
from datetime import timedelta,datetime
from src.process_cenmigDB import cenmigDBMetaData

class download_metadata:
    def __init__(self,all_metadata_save_path : str | None = None):
        self.errorsLogFun = errorsLog()
        self.main = os.path.dirname(os.path.realpath(__file__)) + '/'
        if all_metadata_save_path:
            self.all_metadata_save_path = all_metadata_save_path
        else:
            self.all_metadata_save_path = os.path.join(self.main,'raw_metadata/')
        with open("config.json", 'r') as f:
            config = json.load(f)
            config = config["downloadMeta"]
        if config["cleanFolder"]:
            if os.path.exists(self.all_metadata_save_path):
                shutil.rmtree(self.all_metadata_save_path, ignore_errors = False)
        if not os.path.exists(self.all_metadata_save_path):
            os.mkdir(self.all_metadata_save_path)
        self.listSpeciesSraRunTable = config["listSpeciesSraRunTable"]
        self.listSpeciesPathogen = config["listSpeciesPathogen"]
        self.verbose = config["verbose"]
        self.keepLog = config["keepLog"]
        self.reDownload = config["reDownload"]
        self.dateDownload = config["dateDownload"]
        self.coreUsed = config["coreUsed"]
        mainDir = os.getcwd()
        self.esearch = os.path.join(mainDir, config["esearch"])
        self.efetch = os.path.join(mainDir, config["efetch"])
        self.datasetsToolPath = os.path.join(mainDir, config["datasetsToolPath"])
        self.dataformatsToolPath = os.path.join(mainDir, config["dataformatsToolPath"])
        self.cenmigDBMetaData = cenmigDBMetaData()

    def new_version_pathogen(self,url: str) -> str:
        response = requests.get(url)
        soup = BeautifulSoup(response.text,'html.parser')
        links = soup.find_all('a')
        context_list = []
        for link in links:
            if isinstance(link, Tag):
                href = link.get('href')
                if isinstance(href, str) and href.startswith("PDG"):
                    context_list.append(href.replace("/", ""))
        max_ver_path = ""
        max_ver = 0
        for i in context_list:
            ver = i.split('.')[1]
            if int(ver) > max_ver:
                max_ver = int(ver)
                max_ver_path = i
        return max_ver_path
    
    # get date for Download SraRunTable
    def date_to_query(self) -> Tuple[str,str]:
        date_old = self.cenmigDBMetaData.get_update_database()
        python_datetime = datetime.strptime(date_old, "%Y/%m/%d")
        date_new = python_datetime + timedelta(days=self.dateDownload)
        date_new = date_new.strftime('%Y/%m/%d')
        return date_old, date_new

    def download_sraruntable_metadata(self,date_old: str,date_new: str) -> None:
        query_temp = '((("{SPECIES}"[Organism] OR {SPECIES}[All Fields] OR "{SPECIES}"[orgn]) AND ("biomol dna"[Properties] AND "strategy wgs"[Properties]))) AND ({DATE1}[Modification Date] : {DATE2}[Modification Date])'
        sra_run_table = 1
        for i in tqdm(range(len(self.listSpeciesSraRunTable)), desc="Downloading SraRunTables", ncols=70):
            file_name_sratable_i = os.path.join(self.all_metadata_save_path,f"SraRunTable_{str(sra_run_table)}.csv")
            sp_i = self.listSpeciesSraRunTable[i]
            query_i = query_temp.replace('{SPECIES}',sp_i)
            query_i = query_i.replace('{DATE1}',date_old)
            query_i = query_i.replace('{DATE2}',date_new)
            cmd_download_sratable = f"{self.esearch} -db sra -query '{query_i}' | {self.efetch} -format runinfo > {file_name_sratable_i}"
            sra_run_table += 1
            if self.verbose:
                print(f"\n RUN: {cmd_download_sratable}")
            try:
                run_out = subprocess.run(cmd_download_sratable, shell=True,capture_output=True)
                if self.keepLog:
                    self.errorsLogFun.error_logs(cmd_download_sratable,run_out)
            except Exception as e:
                if self.keepLog:
                    self.errorsLogFun.error_logs_try("Error in download_sraruntable_metadata function: ",e)
                if self.verbose:
                    print(f"Error to run: {cmd_download_sratable} \n Error msg: {e}")

    # Download Pathogen metada from FTP
    def download_pathogen_metadata(self) -> None:
        for sp_i in tqdm(range(len(self.listSpeciesPathogen)), desc="Downloading pathogenMetadata", ncols=70):
            path_sp_i = f"https://ftp.ncbi.nlm.nih.gov/pathogen/Results/{self.listSpeciesPathogen[sp_i]}/" 
            file_sp_version = self.new_version_pathogen(path_sp_i)
            pathogen_ftp = f"https://ftp.ncbi.nlm.nih.gov/pathogen/Results/{self.listSpeciesPathogen[sp_i]}/{file_sp_version}/Metadata/{file_sp_version}.metadata.tsv"
            cmd_pathogen = f"wget {pathogen_ftp} -P {self.all_metadata_save_path}"
            if self.verbose:
                print(f"Download pathogen last version -> {self.listSpeciesPathogen[sp_i]}")
                print(f"RUN: {cmd_pathogen}")
            try:
                run_out = subprocess.run(cmd_pathogen, shell=True,capture_output=True)
                if self.keepLog:
                    self.errorsLogFun.error_logs(cmd_pathogen,run_out)
                tsv_sp_i = os.path.join(self.all_metadata_save_path,f"{file_sp_version}.metadata.tsv")
                if os.path.exists(tsv_sp_i):
                    if self.verbose:
                        print(f"Successfully! download {tsv_sp_i} file.")
                    df = pd.read_table(tsv_sp_i, sep='\t',on_bad_lines='skip',low_memory=False)
                    csv_filename_sp_i = tsv_sp_i.replace('.tsv','.csv')
                    df.to_csv(csv_filename_sp_i, index = False)
                    if os.path.exists(tsv_sp_i):
                        os.remove(tsv_sp_i)
            except Exception as e:
                if self.keepLog:
                    self.errorsLogFun.error_logs_try("Error in download_pathogen_metadata function: ",e)
                if self.verbose:
                    print(f"Error to run: {cmd_pathogen} \n Error msg: {e}")

    def process_srainfo(self,bio_name_str: str) -> None:
        try:
            path_bioproject_file = os.path.join(self.all_metadata_save_path,f"{bio_name_str}.srainfo")
            cmd_download_sra = f"pysradb metadata {bio_name_str} --detailed --saveto {path_bioproject_file}"
            if self.verbose:
                print(f"Downloading Bioproject: {bio_name_str} save to -> {path_bioproject_file}")
                print(f"RUN: {cmd_download_sra}")
            result_cmd = subprocess.run(cmd_download_sra, shell=True, capture_output=True)
            if self.keepLog:
                self.errorsLogFun.error_logs(cmd_download_sra,result_cmd)
            if not os.path.exists(path_bioproject_file):
                if self.verbose:
                    print(f"Can't Download : {bio_name_str} file")
                    print(f"Re-download with: 0/{self.reDownload}")
                redownload = 0
                while  redownload < self.reDownload:
                    redownload += 1
                    sec_ran_reload = random.randint(30, 120)
                    time.sleep(sec_ran_reload)
                    result_cmd = subprocess.run(cmd_download_sra, shell=True, capture_output=True)
                    if self.keepLog:
                        self.errorsLogFun.error_logs(cmd_download_sra,result_cmd)
                    if not os.path.exists(path_bioproject_file):
                        if self.verbose:
                            print(f"Can't Download : {bio_name_str} file")
                            print(f"Re-download with: {redownload}/{self.reDownload}")
                    else:
                        break
            else:
                if self.verbose:
                    print(f"Successfully! download {bio_name_str} file.")
        except Exception as e:
            if self.keepLog:
                self.errorsLogFun.error_logs_try("Error in process_srainfo function: ",e)
            if self.verbose:
                print(f'Can not Download Bioproject File Name: {bio_name_str}')

    # Download SRAinfo by pysradb from Bioproject
    def download_srainfo(self) -> None:
        sraruntable_path = self.all_metadata_save_path + 'SraRunTable_*' 
        file_sraruntable_all = glob.glob(sraruntable_path) #get all file in path
        df_ls_sra = []
        for i in file_sraruntable_all:
            try:
                df_i = pd.read_csv(i, encoding="utf-8-sig",on_bad_lines = 'skip', low_memory=False)
                df_ls_sra.append(df_i)
            except Exception as e:
                if self.keepLog:
                    self.errorsLogFun.error_logs_try("Error in extract SraRuntable: ",e)
                if self.verbose:
                    print(f"Can not load data from {i} file. \n Error: {e}")
        df = pd.concat(df_ls_sra, ignore_index= True)
        bioproject_set_new = set(df['BioProject'].dropna())
        self.multi_download_sra(bioproject_set_new)
    
    def multi_download_sra(self,bioproject_set_new: Set) -> None:
        srainfo_path = os.path.join(self.main,'raw_metadata/*.srainfo')
        all_srainfo_file_path = glob.glob(srainfo_path)
        bioproject_old = ['BioProject']
        for srainfo_i in all_srainfo_file_path:
            bio_name_i = os.path.basename(srainfo_i)
            bio_name_i = bio_name_i.replace('.srainfo', '')
            bioproject_old.append(bio_name_i)
        miss_bioproject_file = list(set(bioproject_set_new) - set(bioproject_old))
        if len(miss_bioproject_file) > 0:
            print(f"New SRA Bioproject : {miss_bioproject_file}")
            with Pool(processes=self.coreUsed) as pool:
                # Use tqdm to wrap the imap call for progress tracking
                for _ in tqdm(pool.imap(self.process_srainfo, miss_bioproject_file), total=len(miss_bioproject_file), desc="Downloading srainfo", ncols=70):
                    pass

    def split_list(self,lst, chunk_size):
        return [lst[i:i+chunk_size] for i in range(0, len(lst), chunk_size)]

    def download_sra_by_pathogen(self,list_sra_new_pathogen: List) -> None:
        if self.verbose:
            print(f"SRA from new pathogen {len(list_sra_new_pathogen)} Number")
        if len(list_sra_new_pathogen) > 0:
            missing_list_cut = self.split_list(list_sra_new_pathogen, 500)
            count_missing_list_files = 1
            for missing_list_i in tqdm(missing_list_cut, desc="Processing missing SRA from pathogen files", ncols=70):
                missing_sra_file = os.path.join(self.all_metadata_save_path,f"missing_sra_{str(count_missing_list_files)}.csv")
                count_missing_list_files += 1
                query_missing_sra = ' OR '.join(missing_list_i)
                cmd_missing_sra = f"{self.esearch} -db sra -query '{query_missing_sra}' | {self.efetch} -format runinfo > {missing_sra_file}"
                result_cmd = subprocess.run(cmd_missing_sra, shell=True, capture_output=True)
                if self.keepLog:
                    self.errorsLogFun.error_logs(cmd_missing_sra,result_cmd)
                if not os.path.exists(missing_sra_file): 
                    if self.verbose:
                        print(f"Can't Download Missing SRA: {missing_sra_file}")
                    redownload = 0
                    while  redownload < self.reDownload:
                        redownload += 1
                        sec_ran_reload = random.randint(30, 120)
                        time.sleep(sec_ran_reload)
                        result_cmd = subprocess.run(cmd_missing_sra, shell=True, capture_output=True)
                        if self.keepLog:
                            self.errorsLogFun.error_logs(cmd_missing_sra,result_cmd)
                        if not os.path.exists(missing_sra_file):
                            if self.verbose:
                                print(f"Can't Download : {missing_sra_file} file")
                                print(f"Re-download with: {redownload}/{self.reDownload}")
                        else:
                            break

    def download_metadata_assembly(self,list_bioproject_assembly:List) -> None:
        bio_assembly_file_file = os.path.join(self.main,'raw_metadata/*_assembly.csv')
        all_bio_assembly_file_path = glob.glob(bio_assembly_file_file)
        bioproject_assembly_old = []
        for bio_assembly_i in all_bio_assembly_file_path:
            bio_name_i = os.path.basename(bio_assembly_i)
            bio_name_i = bio_name_i.replace('_assembly.csv', '')
            bioproject_assembly_old.append(bio_name_i)
        new_bio_assembly = set(list_bioproject_assembly) - set(bioproject_assembly_old)
        if len(new_bio_assembly) > 0:
            if self.verbose:
                print(f"New Assembly Bioproject : {new_bio_assembly}")
            for ass_bio_i in new_bio_assembly:
                path_bio_ass_file = os.path.join(self.main,f"raw_metadata",f"{str(ass_bio_i)}_assembly.csv")
                # datasets_tool_f = os.path.expanduser(self.datasetsToolPath) #os.path.join(self.main,self.datasetsToolPath) 
                # dataformats_tool_f = os.path.expanduser(self.dataformatsToolPath) #os.path.join(self.main,self.dataformatsToolPath)
                cmd_download_assembly = f"{self.datasetsToolPath} summary genome accession {str(ass_bio_i)} --as-json-lines | {self.dataformatsToolPath} tsv genome --fields accession,annotinfo-release-date,assminfo-sequencing-tech,assminfo-biosample-ids-db,assminfo-biosample-ids-value > {path_bio_ass_file}"
                result_cmd = subprocess.run(cmd_download_assembly, shell=True, capture_output=True)
                if self.keepLog:
                    self.errorsLogFun.error_logs(cmd_download_assembly,result_cmd)
                if not os.path.exists(path_bio_ass_file):
                    if self.verbose:
                        print(f"Can't Download : {ass_bio_i} file")
                    redownload = 0
                    while  redownload < self.reDownload:
                        redownload += 1
                        result_cmd = subprocess.run(cmd_download_assembly, shell=True, capture_output=True)
                        if self.keepLog:
                            self.errorsLogFun.error_logs(cmd_download_assembly,result_cmd)
                        if not os.path.exists(path_bio_ass_file):
                            if self.verbose:
                                print(f"Can't Download : {ass_bio_i} file")
                        else:
                            break
                else:
                    if self.verbose:
                        print(f"Download Completed : {ass_bio_i} file")

def download_all_metadata() -> tuple[str, str]:
    down_meta = download_metadata()
    date_old, date_new = down_meta.date_to_query()
    print('Download SraRuntable files')
    down_meta.download_sraruntable_metadata(date_old,date_new)
    print('Download SRAINFO files')
    down_meta.download_srainfo()
    print('Download pathogen files')
    down_meta.download_pathogen_metadata()
    print('Completed Download All Metadata Files')
    down_meta.download_srainfo()
    return date_old, date_new
