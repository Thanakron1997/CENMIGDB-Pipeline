import pandas as pd
import datetime
import subprocess
import json
import os
import re
from typing import List,Dict
from src.errors import errorsLog

class findST:
    def __init__(self):
        self.errorsLogFun = errorsLog()
        self.main = os.path.dirname(os.path.realpath(__file__)) + '/'
        with open("config.json", 'r') as f:
            config = json.load(f)
            config = config["findST"]
        self.verbose = config["verbose"]
        self.keepLog = config["keepLog"]
        self.stringMlstDB = config["stringMlstDB"]
        self.krocusDB = config["krocusDB"]
        self.mlstCheckScheme = config["mlstCheckScheme"]
        self.uidDocker = config["uidDocker"]
        self.gidDocker = config["gidDocker"]
        self.schemeList = config["schemeList"]

    def get_scheme(self,organism_i :str) -> Dict:
        scheme_name = ""
        for key, values in self.schemeList.items():
            if any(x.lower() in organism_i.lower() for x in values):
                scheme_name = key
        if scheme_name != "":
            stringmlst_db =   os.path.join(self.main,self.stringMlstDB[scheme_name])
            krocus_db = os.path.join(self.main,self.krocusDB[scheme_name])
            mlst_check_scheme = self.mlstCheckScheme[scheme_name]
        else:
            organism_i_re = organism_i.split(" ")[:2]
            organism_i_re = '_'.join(organism_i_re)
            stringmlst_db = os.path.join(self.main, 'mlst_db', organism_i_re, organism_i_re)
            krocus_db = os.path.join(self.main, 'krocus_db', organism_i_re)
            mlst_check_scheme = organism_i

        scheme = {"stringmlst_db":stringmlst_db,
                  "krocus_db": krocus_db,
                  "mlst_check_scheme":mlst_check_scheme}
        return scheme
    
    def result_mlst(self,program: str | None ,file_mlst_output_i: str,id_i:str) -> pd.DataFrame:
        try:
            df_mlst_raw_i = pd.DataFrame()
            if os.path.isfile(file_mlst_output_i):
                if program == "krocus":
                    dict_result_krocus = {}
                    dict_result_krocus["cenmigID"] = id_i
                    with open(file_mlst_output_i) as f:
                        last_line_result = f.readlines()[-1].strip()
                    last_line_result_spit = last_line_result.split("\t")
                    dict_result_krocus['ST'] = last_line_result_spit[0]
                    if len(last_line_result_spit) < 10 and len(last_line_result_spit) > 2:
                        for i in last_line_result_spit[2:len(last_line_result_spit)-1]:
                            allele_i = re.findall("[a-zA-Z]+",i)[0]
                            allele_locat = re.findall(r"\d+", i)[0]
                            dict_result_krocus[allele_i] = allele_locat
                    elif len(last_line_result_spit) == 10:
                        for i in last_line_result_spit[2:9]:
                            allele_i = re.findall("[a-zA-Z]+",i)[0]
                            allele_locat = re.findall(r"\d+", i)[0]
                            dict_result_krocus[allele_i] = allele_locat
                    df_sra_mlst_raw_i = pd.DataFrame([dict_result_krocus])
                    df_sra_mlst_raw_i['mlst_run_date'] = [datetime.datetime.now() for i in range(len(df_sra_mlst_raw_i))]
                elif program == "stringmlst":
                    df_mlst_raw_i = pd.read_table(file_mlst_output_i,sep='\t')
                    df_mlst_raw_i.insert(0, 'cenmigID', id_i)
                    df_mlst_raw_i = df_mlst_raw_i.drop(['Sample'], axis=1)
                    df_mlst_raw_i['mlst_run_date'] = datetime.datetime.now().date()
                    if len(df_mlst_raw_i) > 1:
                        df_mlst_raw_i = df_mlst_raw_i.iloc[[0]]
                elif program == "mlst_check":
                    file_mlst_output_i = os.path.join(file_mlst_output_i,"mlst_results.allele.csv")
                    df_mlst_raw_i = pd.read_table(file_mlst_output_i, sep = '\t')
                    df_mlst_raw_i.rename(columns={"Isolate": "cenmigID"}, inplace=True)
                    df_mlst_raw_i = df_mlst_raw_i.drop(["New ST","Contamination"], axis='columns')
                    df_mlst_raw_i['mlst_run_date'] = datetime.datetime.now().date()
                else:
                    df_mlst_raw_i = pd.DataFrame([{'cenmigID':id_i}])
                    df_mlst_raw_i['ST'] = "Unknow program"
                    df_mlst_raw_i['mlst_run_date'] = datetime.datetime.now().date()
            else:
                df_mlst_raw_i = pd.DataFrame([{'cenmigID':id_i}])
                df_mlst_raw_i['ST'] = "No MLST-Result-file"
                df_mlst_raw_i['mlst_run_date'] = datetime.datetime.now().date()

            return df_mlst_raw_i
            
        except Exception as e:
            if self.keepLog:
                self.errorsLogFun.error_logs_try(f"Error in get Result MLST : {id_i}",e)
            df_mlst_raw_i = pd.DataFrame([{'cenmigID':id_i}])
            df_mlst_raw_i['ST'] = "Error"
            df_mlst_raw_i['mlst_run_date'] = datetime.datetime.now().date()
            return df_mlst_raw_i

    def run_stringMLST(self,seq_files: List,file_mlst_output_i :str,id_i: str,stringmlst_db: str) -> str | None:
        if len(seq_files) == 1:
            try:
                cmd_mlst_single = f"stringMLST.py --predict -s --prefix {stringmlst_db} -o {file_mlst_output_i} -1 {seq_files[0]}"
                result_string_mlst = subprocess.run(cmd_mlst_single, shell=True,capture_output=True)
                if self.keepLog:
                    self.errorsLogFun.error_logs(cmd_mlst_single,result_string_mlst)
            except Exception as e:
                if self.keepLog:
                    self.errorsLogFun.error_logs_try(f"Error in string MLST : {id_i}",e)
        elif len(seq_files) > 1:
            try:
                cmd_mlst_paired = f"stringMLST.py --predict -p --prefix {stringmlst_db} -o {file_mlst_output_i} -1 {seq_files[0]} -2 {seq_files[1]}"
                result_string_mlst = subprocess.run(cmd_mlst_paired, shell=True,capture_output=True)
                if self.keepLog:
                    self.errorsLogFun.error_logs(cmd_mlst_paired,result_string_mlst)
            except Exception as e:
                if self.keepLog:
                    self.errorsLogFun.error_logs_try(f"Error in string MLST : {id_i}",e)
        
        if os.path.exists(file_mlst_output_i):
            return "stringmlst"
        else:
            return
        
    def run_krocus(self,seq_files: List,file_mlst_output_i: str,id_i: str,krocus_db: str) -> str | None:
        try:
            cmd_mlst_krocus = f"krocus {krocus_db} {seq_files[0]} -o {file_mlst_output_i}"
            result_krocus = subprocess.run(cmd_mlst_krocus, shell=True,capture_output=True)
            if self.keepLog:
                    self.errorsLogFun.error_logs(cmd_mlst_krocus,result_krocus)
        except Exception as e:
            if self.keepLog:
                self.errorsLogFun.error_logs_try(f"Error in Krocus : {id_i}",e)

        if os.path.exists(file_mlst_output_i):
            return "krocus"
        else:
            return
   
    def run_mlst_check(self,seq_file: str, file_mlst_output_i: str, id_i: str,mlst_check_scheme: str) -> str | None:
        try:
            cmd_mlst_fasta = f'docker run --rm --user {self.uidDocker}:{self.gidDocker} -v '+ "${HOME}:${HOME} -w ${PWD} " + f'sangerpathogens/mlst_check get_sequence_type -s "{mlst_check_scheme}" -o {file_mlst_output_i} {seq_file}'
            result_mlst_check = subprocess.run(cmd_mlst_fasta,shell=True,capture_output=True)
            if self.keepLog:
                    self.errorsLogFun.error_logs(cmd_mlst_fasta,result_mlst_check)
        except Exception as e:
            if self.keepLog:
                self.errorsLogFun.error_logs_try(f"Error in mlst_check : {id_i}",e)
        
        if os.path.exists(file_mlst_output_i):
            return "mlst_check"
        else:
            
            return
    
    def run_mlst_raw_seq(self,id_i: str,organism_i : str,seq_files: List,platform_i: str,output_dir_i: str) -> pd.DataFrame:
        """
        Run MLST by krocus or stringMLST
        """

        scheme = self.get_scheme(organism_i)
        file_mlst_output_i =os.path.join(output_dir_i,f"mlst_result_{id_i}.txt")
        program = ""
        if "pacbio" in platform_i.lower() or 'PACBIO_SMRT' in platform_i.lower() or "nanopore" in platform_i.lower() or "oxford" in platform_i.lower():
            program = self.run_krocus(seq_files=seq_files,file_mlst_output_i=file_mlst_output_i,id_i=id_i,krocus_db=scheme["krocus_db"])
        else:
            program = self.run_stringMLST(seq_files=seq_files,file_mlst_output_i=file_mlst_output_i,id_i=id_i,stringmlst_db=scheme["stringmlst_db"])
        df_mlst_result = self.result_mlst(program=program,file_mlst_output_i=file_mlst_output_i,id_i=id_i)
        return df_mlst_result
    
    def run_mlst_assembly_seq(self,id_i: str,organism_i:str,seq_file: str,output_dir_i: str) -> pd.DataFrame:
        """
        Run MLST by MLST Check
        """

        file_mlst_output_i = os.path.join(output_dir_i,f"mlst_result_{id_i}")
        scheme = self.get_scheme(organism_i)
        program = self.run_mlst_check(seq_file=seq_file,file_mlst_output_i=file_mlst_output_i,id_i=id_i,mlst_check_scheme=scheme["mlst_check_scheme"])
        df_mlst_result = self.result_mlst(program=program,file_mlst_output_i=file_mlst_output_i,id_i=id_i)
        return df_mlst_result

