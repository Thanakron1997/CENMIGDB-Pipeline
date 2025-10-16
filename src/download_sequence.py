import os
import re
import json
import glob
import time
import gzip
import random
import shutil
import zipfile
import subprocess
from typing import List
from src.errors import errorsLog
from src.process_cenmigDB import cenmigDBGridFS

class downloadSEQ:
    def __init__(self):
        self.errorsLogFun = errorsLog()
        self.cenmigDBGFS = cenmigDBGridFS()
        self.main = os.path.dirname(os.path.realpath(__file__)) + '/'
        with open("config.json", 'r') as f:
            config = json.load(f)
            config = config["downloadSEQ"]
        self.verbose = config["verbose"]
        self.keepLog = config["keepLog"]
        self.reDownload  = config["reDownload"]
        self.randomSec = config["randomSec"]
        mainDir = os.getcwd()
        self.sratoolPrefetchPath = os.path.join(mainDir, config["sratoolPrefetchPath"])
        self.sratoolFasterqPath = os.path.join(mainDir,config["sratoolFasterqPath"])
        self.sratoolFastqDumpPath = os.path.join(mainDir,config["sratoolFastqDumpPath"])
        self.datasetsToolPath = os.path.join(mainDir,config["datasetsToolPath"])

    def sort_key(self,filename):
        match = re.search(r'_(\d+)', filename)
        return int(match.group(1)) if match else float('inf')  # no number → goes to the end
    
    def is_gz_file(self,file_path):
        _, file_extension = os.path.splitext(file_path)
        return file_extension.lower() == '.gz'

    def download_seq_fastq(self,id_i,platform_i,output_dir_i):
        sec_ran_reload = random.randint(2, self.randomSec)
        try:
            file_sra_i = os.path.join(output_dir_i, f"{str(id_i)}.sra") 
            cmd_for_download_sra = f"{self.sratoolPrefetchPath} -f yes -o {file_sra_i} {str(id_i)}"
            result_download_sra = subprocess.run(cmd_for_download_sra, shell=True, capture_output=True)
            if self.keepLog:
                self.errorsLogFun.error_logs(cmd_for_download_sra,result_download_sra)
            if not os.path.exists(file_sra_i): # check sra file
                i_loop = 1
                while  i_loop < self.reDownload:
                    i_loop += 1    
                    time.sleep(sec_ran_reload)
                    if self.keepLog:
                        self.errorsLogFun.error_logs(cmd_for_download_sra,result_download_sra)
                    if os.path.exists(file_sra_i):
                        break
            if "pacbio" in platform_i.lower() or 'pacbio_smrt' in platform_i.lower() or "nanopore" in platform_i.lower() or "oxford" in platform_i.lower():
                cmd_fastq_dump = f"{self.sratoolFastqDumpPath} -O {output_dir_i}/ {file_sra_i}"
                result_download_fastq = subprocess.run(cmd_fastq_dump, shell=True, capture_output=True)
                if self.keepLog:
                    self.errorsLogFun.error_logs(cmd_fastq_dump,result_download_fastq)
            else:    
                cmd_fasterq_dump = f"{self.sratoolFasterqPath} -O {output_dir_i}/ {file_sra_i}"
                result_download_fasterq = subprocess.run(cmd_fasterq_dump, shell=True, capture_output=True)
                if self.keepLog:
                    self.errorsLogFun.error_logs(cmd_fasterq_dump,result_download_fasterq)
        except Exception as e:
            if self.keepLog:
                self.errorsLogFun.error_logs_try("Error in download Sequence Data : "+id_i,e)
            time.sleep(sec_ran_reload)
            try:
                cmd_fastq_dump = f"{self.sratoolFastqDumpPath} -O {output_dir_i}/ --split-3 {str(id_i)}"
                result_download_fastq = subprocess.run(cmd_fastq_dump, shell=True, capture_output=True)
                if self.keepLog:
                    self.errorsLogFun.error_logs(cmd_fastq_dump,result_download_fastq)
            except Exception as e:
                if self.keepLog:
                    self.errorsLogFun.error_logs_try("Error in download Sequence Data by fastq-dump : "+id_i,e)
        
        seq_files_i = output_dir_i + '/*.fastq'
        seq_files_list = glob.glob(seq_files_i)
        seq_files_list = sorted(seq_files_list, key=self.sort_key)
        if self.verbose:
            if len(seq_files_list) > 0:
                print(f"Download: {id_i} completed!")
            else:
                print(f"Can't download: {id_i}")
        return seq_files_list

    def download_seq_assembly(self,id_i: str,output_dir_i: str) -> str | None:
        sec_ran_reload = random.randint(2, self.randomSec)
        file_assemly_i = os.path.join(output_dir_i,f"{id_i}.zip")
        cmd_for_download = f"{self.datasetsToolPath} download genome accession {id_i} --include genome --filename {file_assemly_i}"
        result_download_assembly = subprocess.run(cmd_for_download, shell=True,capture_output=True)
        if self.keepLog:
            self.errorsLogFun.error_logs(cmd_for_download,result_download_assembly)
        if not os.path.exists(file_assemly_i):
            time.sleep(sec_ran_reload)
            result_download_assembly = subprocess.run(cmd_for_download, shell=True,capture_output=True)
            if self.keepLog:
                self.errorsLogFun.error_logs(cmd_for_download,result_download_assembly)

        file_fasta_i_move = os.path.join(output_dir_i,f"{id_i}.fna")
        if os.path.exists(file_assemly_i):
            with zipfile.ZipFile(file_assemly_i, 'r') as zip_ref:
                zip_ref.extractall(output_dir_i)
            # os.remove(file_assemly_i)
            file_fasta_i = os.path.join(output_dir_i,f"ncbi_dataset/data/{id_i}/*.fna")
            for i in glob.glob(file_fasta_i):
                shutil.move(i, file_fasta_i_move)
        else:
            id_i_GCF = id_i.replace("GCA", "GCF")
            if self.verbose:
                print("Can't Download : {} file and try to download by {}....".format(id_i,id_i_GCF))
            file_assemly_i_2 = os.path.join(output_dir_i,f"{id_i_GCF}.zip")
            cmd_for_download_2 = f"{self.datasetsToolPath} download genome accession {id_i_GCF} --include genome --filename {file_assemly_i_2}"
            result_download_assembly_by_gcf = subprocess.run(cmd_for_download_2, shell=True,capture_output=True)
            if self.keepLog:
                self.errorsLogFun.error_logs(cmd_for_download_2,result_download_assembly_by_gcf)
            if not os.path.exists(file_assemly_i_2):
                i_loop = 1
                while   i_loop < self.reDownload:
                    i_loop += 1
                    time.sleep(sec_ran_reload)
                    result_download_assembly_by_gcf = subprocess.run(cmd_for_download_2, shell=True,capture_output=True)
                    if self.keepLog:
                        self.errorsLogFun.error_logs(cmd_for_download_2,result_download_assembly_by_gcf)
                    if os.path.exists(file_assemly_i_2):
                        break
                if os.path.exists(file_assemly_i_2):
                    with zipfile.ZipFile(file_assemly_i_2, 'r') as zip_ref:
                        zip_ref.extractall(output_dir_i)
                    file_fasta_i = os.path.join(output_dir_i,f"ncbi_dataset/data/{id_i_GCF}/*.fna")
                    for i in glob.glob(file_fasta_i):
                        shutil.move(i, file_fasta_i_move)
            
        if os.path.exists(file_fasta_i_move):
            if self.verbose:
                print(f"Download: {id_i} completed!")
            return file_fasta_i_move
        else:
            if self.verbose:
                print(f"Can't download: {id_i}")

    def download_seq_inhouse(self,id_i: str,lstFileName: List,output_dir_i: str) -> List[str]:
        """
        Download sequences from inhouse db
        """
        seq_files_list = []
        for fileSeq_i in lstFileName:
            getSeqResult = self.cenmigDBGFS.get_item_from_db(fileSeq_i,output_dir_i)
            seq_file_name_i = os.path.join(output_dir_i,str(fileSeq_i))
            if getSeqResult == True:
                if self.is_gz_file(fileSeq_i):
                    seqFASTQ = os.path.join(output_dir_i,f"{fileSeq_i.split('.')[0]}.fastq")
                    with gzip.open(seq_file_name_i, 'rb') as f_in:
                        with open(seqFASTQ, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    seq_files_list.append(seqFASTQ)
                else:
                    seq_files_list.append(seq_file_name_i)
        seq_files_list = sorted(seq_files_list, key=self.sort_key)
        if self.verbose:
            if len(seq_files_list) > 0:
                print(f"Download: {id_i} completed!")
            else:
                print(f"Can't download: {id_i}")
        return seq_files_list