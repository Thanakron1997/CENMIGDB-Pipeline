import os
import json
import subprocess
import pandas as pd
import datetime
import glob
from pathlib import Path
from typing import List,Dict,Tuple
from src.errors import errorsLog

class findResistance():
    def __init__(self):
        self.errorsLogFun = errorsLog()
        self.main = os.path.dirname(os.path.realpath(__file__)) + '/'
        with open("config.json", 'r') as f:
            config = json.load(f)
            config = config["findResistance"]
        self.verbose = config["verbose"]
        self.keepLog = config["keepLog"]
        self.uidDocker = config["uidDocker"]
        self.gidDocker = config["gidDocker"]
        self.schemeList = config["schemeList"]
        self.tbprofilerVer = config["tbprofilerVer"]
        self.resfinderVer = config["resfinderVer"]
        phenotypesFiles = config["phenotypesFiles"]
        drugClassTB =  config["drugClassTB"]
        self.tbp_drug_name = pd.read_csv(os.path.join(self.main,drugClassTB),low_memory=False)
        phenotypesData = pd.read_csv(os.path.join(self.main,phenotypesFiles),low_memory=False,sep='\t')
        phenotypesData = phenotypesData[['Class','Phenotype']]
        self.phenotypesData = phenotypesData.drop_duplicates()
    
    def get_scheme(self,organism_i: str) -> str:
        scheme_name = organism_i.lower()
        for key, values in self.schemeList.items():
            if any(x.lower() in organism_i.lower() for x in values):
                scheme_name = key
        return scheme_name

    def result_resfinder(self,output_dir: Path,id_i: str) -> Tuple[pd.DataFrame, str, str]:
        try:
            file_sra_resfinder_raw_i = os.path.join(output_dir ,'ResFinder_results_tab.txt')
            if os.path.isfile(file_sra_resfinder_raw_i):
                df_sra_resfinder_raw_i = pd.read_csv(file_sra_resfinder_raw_i,low_memory=False,sep='\t')
                if df_sra_resfinder_raw_i.shape[0] > 0:
                    df_sra_resfinder_raw_i.insert(0, 'cenmigID', id_i)
                    df_sra_resfinder_raw_i['resfinder_run_date'] = [datetime.datetime.now().date() for i in range(len(df_sra_resfinder_raw_i))] 
                    try:
                        js_file = glob.glob(os.path.join(output_dir,'/*.json'))
                        file_js_select = js_file[0]
                        with open(file_js_select) as file:
                            resfinder_detail = json.load(file)
                        software_version = resfinder_detail["software_version"]
                        list_db = list(resfinder_detail['databases'].keys())
                        resfinder_db_version = resfinder_detail['databases'][list_db[0]]['key']
                        df_sra_resfinder_raw_i['resfinder_version'] = software_version
                        df_sra_resfinder_raw_i['resfinder_db_version'] = resfinder_db_version 
                    except:
                        software_version = "NA"
                        resfinder_db_version = "NA"
                        df_sra_resfinder_raw_i['resfinder_version'] = software_version
                        df_sra_resfinder_raw_i['resfinder_db_version'] = resfinder_db_version 
                else:
                    df_sra_resfinder_raw_i = pd.DataFrame([{'cenmigID':id_i}])
                    df_sra_resfinder_raw_i['resfinder_run_date'] = [datetime.datetime.now().date() for i in range(len(df_sra_resfinder_raw_i))]
                    software_version = "NA"
                    resfinder_db_version = "NA"
                    df_sra_resfinder_raw_i['resfinder_version'] = software_version
                    df_sra_resfinder_raw_i['resfinder_db_version'] = resfinder_db_version
                return df_sra_resfinder_raw_i,software_version,resfinder_db_version
            else:
                df_sra_resfinder_raw_i = pd.DataFrame([{'cenmigID':id_i}])
                df_sra_resfinder_raw_i['resfinder_run_date'] = [datetime.datetime.now().date() for i in range(len(df_sra_resfinder_raw_i))]
                software_version = "NA"
                resfinder_db_version = "NA"
                df_sra_resfinder_raw_i['resfinder_version'] = software_version
                df_sra_resfinder_raw_i['resfinder_db_version'] = resfinder_db_version
                return df_sra_resfinder_raw_i,software_version,resfinder_db_version
            
        except Exception as e:
            if self.keepLog:
                self.errorsLogFun.error_logs_try(f"Error in get Result Resfinder  {id_i}",e)
            df_sra_resfinder_raw_i = pd.DataFrame()
            software_version = "NA"
            resfinder_db_version = "NA"
            return df_sra_resfinder_raw_i,software_version,resfinder_db_version

    def result_pointfinder(self,output_dir: Path,id_i: str) -> Tuple[pd.DataFrame, str]:
        try:
            file_sra_pointfinder_raw_i = os.path.join(output_dir,'PointFinder_results.txt')
            if os.path.isfile(file_sra_pointfinder_raw_i):
                df_sra_pointfinder_raw_i = pd.read_csv(file_sra_pointfinder_raw_i,sep = '\t')
                if df_sra_pointfinder_raw_i.shape[0] > 0:
                    df_sra_pointfinder_raw_i.insert(0, 'cenmigID', id_i)
                    df_sra_pointfinder_raw_i['pointfinder_run_date'] = [datetime.datetime.now().date() for i in range(len(df_sra_pointfinder_raw_i))]
                    try:
                        js_file = glob.glob(os.path.join(output_dir,'/*.json'))
                        file_js_select = js_file[0]
                        with open(file_js_select) as file:
                            resfinder_detail = json.load(file)
                        list_db = list(resfinder_detail['databases'].keys())
                        point_db_version = resfinder_detail['databases'][list_db[1]]['key']
                        df_sra_pointfinder_raw_i['pointfinder_db_version'] = point_db_version 
                    except:
                        point_db_version = "NA"
                        df_sra_pointfinder_raw_i['pointfinder_db_version'] = point_db_version
                else:
                    df_sra_pointfinder_raw_i = pd.DataFrame([{'cenmigID':id_i}])
                    df_sra_pointfinder_raw_i['pointfinder_run_date'] = [datetime.datetime.now().date() for i in range(len(df_sra_pointfinder_raw_i))]
                    point_db_version = "NA"
                    df_sra_pointfinder_raw_i['pointfinder_db_version'] = point_db_version
                return df_sra_pointfinder_raw_i,point_db_version
            else:
                df_sra_pointfinder_raw_i = pd.DataFrame([{'cenmigID':id_i}])
                df_sra_pointfinder_raw_i['pointfinder_run_date'] = [datetime.datetime.now().date() for i in range(len(df_sra_pointfinder_raw_i))]
                point_db_version = "NA"
                df_sra_pointfinder_raw_i['pointfinder_db_version'] = point_db_version
                return df_sra_pointfinder_raw_i,point_db_version
            
        except Exception as e:
            if self.keepLog:
                self.errorsLogFun.error_logs_try(f"Error in get Result PointFinder : {id_i}",e)
            df_sra_pointfinder_raw_i = pd.DataFrame()
            point_db_version = "NA"
            return df_sra_pointfinder_raw_i,point_db_version
    
    def to_one_line_resfinder_result(self,df_resfinder_raw_i: pd.DataFrame,df_pointfinder_raw_i: pd.DataFrame,id_i: str,software_version: str,resfinder_db_version: str,point_db_version)-> Tuple[pd.DataFrame, pd.DataFrame]:
        try:
            if 'Phenotype' in df_resfinder_raw_i.columns:
                df_resfinder_line_i = pd.merge(df_resfinder_raw_i,self.phenotypesData,on='Phenotype',how='left')
                df_resfinder_line_i['resistance_gene_iden'] = df_resfinder_line_i['Resistance gene'] +"|("+ df_resfinder_line_i['Identity'].astype('string') +")" + "|(" + df_resfinder_line_i['Phenotype'] + ")|"+ df_resfinder_line_i['Accession no.']
                df_resfinder_line_i = df_resfinder_line_i.groupby('Class').agg({'resistance_gene_iden': ', '.join}).transpose()
                df_resfinder_line_i = df_resfinder_line_i.reset_index(drop=True)
                df_resfinder_line_i = df_resfinder_line_i.add_suffix('_drug_class_by_resfinder')
                df_resfinder_line_i = df_resfinder_line_i.rename(columns=lambda x: x.replace(' ', '_'))
                df_resfinder_line_i.insert(0, 'cenmigID', id_i)
                df_resfinder_line_i['resfinder_version'] = software_version
                df_resfinder_line_i['resfinder_db_version'] = resfinder_db_version
            else:
                df_resfinder_line_i = pd.DataFrame([{'cenmigID':id_i}])
                df_resfinder_line_i['resfinder_version'] = software_version
                df_resfinder_line_i['resfinder_db_version'] = resfinder_db_version
        except Exception as e:
            if self.keepLog:
                self.errorsLogFun.error_logs_try(f"Error in process to_one_line_resfinder_result resfinder result : {id_i}",e)
            df_resfinder_line_i = pd.DataFrame()
        try: # for point_mutation
            if 'Resistance' in df_pointfinder_raw_i.columns:
                df_pointfinder_line_i = df_pointfinder_raw_i.groupby('Resistance').agg({'Mutation': ', '.join}).transpose()
                df_pointfinder_line_i = df_pointfinder_line_i.add_suffix('_point_mutation_by_resfinder')
                df_pointfinder_line_i = df_pointfinder_line_i.rename(columns=lambda x: x.replace(' ', '_'))
                df_pointfinder_line_i.insert(0, 'cenmigID', id_i)
                df_pointfinder_line_i['pointfinder_db_version'] = point_db_version
            else:
                df_pointfinder_line_i =  pd.DataFrame([{'cenmigID':id_i}])
                df_pointfinder_line_i['pointfinder_db_version'] = point_db_version

        except Exception as e:
            if self.keepLog:
                self.errorsLogFun.error_logs_try(f"Error in process to_one_line_resfinder_result pointfinder result : {id_i}",e)
            df_pointfinder_line_i = pd.DataFrame()
        return df_resfinder_line_i, df_pointfinder_line_i
    
    def result_tbprofiler(self,output_dir: Path,id_i: str) -> pd.DataFrame:
        try:
            tb_profiler_js_file_select = os.path.join(output_dir,"results/tbprofiler.results.json")
            if os.path.isfile(tb_profiler_js_file_select):
                with open(tb_profiler_js_file_select) as file:
                    resfinder_detail = json.load(file)
                main_lin = ""
                sublin = ""
                dict_tbprofiler_result = {}
                dict_tbprofiler_result['cenmigID'] = id_i
                dict_tbprofiler_result['tbprofiler_version'] = resfinder_detail['pipeline']['software_version']
                main_lin = resfinder_detail['main_lineage']
                sublin = resfinder_detail['sub_lineage']
                if sublin == '':
                    if main_lin == '':
                        wg_snp_lineage_assignment = 'Not Found'
                    else:
                        wg_snp_lineage_assignment = main_lin
                else:
                    wg_snp_lineage_assignment = sublin
                dict_tbprofiler_result['wg_snp_lineage_assignment'] = wg_snp_lineage_assignment
                dict_tbprofiler_result['DR_Type'] = resfinder_detail['drtype']
                dict_tbprofiler_result['tb_profiler_run_date'] = resfinder_detail['timestamp']
                dr_variants = resfinder_detail['dr_variants']
                for drug_i in self.tbp_drug_name['Drug']:
                    list_gene = []
                    for tb_result_i in dr_variants:
                        if drug_i.lower() in [entry["drug"] for entry in tb_result_i["drugs"]]:
                            tbp_gene = tb_result_i['gene_name']
                            tbp_nuc = tb_result_i['nucleotide_change']
                            tbp_dep = tb_result_i['depth']
                            tbp_freq = tb_result_i['freq']
                            tbp_who = "-"
                            for drug_info in tb_result_i["annotation"]:
                                if drug_info["drug"] == drug_i:
                                    tbp_who = drug_info["confidence"]
                            if tbp_who == "":
                                tbp_who = "-"
                            list_gene.append(str(tbp_gene)+'|'+str(tbp_nuc)+'|'+str(tbp_dep)+'|'+str(tbp_freq)+'|'+str(tbp_who))
                    if len(list_gene) > 0:
                        dict_tbprofiler_result[drug_i+"_tbp_result"] = list_gene
                dict_tbprofiler_result['TB_raw_result'] = resfinder_detail
                df_tb_profiler_raw_i = pd.DataFrame([dict_tbprofiler_result])
                return df_tb_profiler_raw_i
            else:
                df_tb_profiler_raw_i = pd.DataFrame([{'cenmigID':id_i}])
                return df_tb_profiler_raw_i
            
        except Exception as e:
            if self.keepLog:
                self.errorsLogFun.error_logs_try(f"Error in get Result TB profiler :{id_i}",e)
            df_tb_profiler_raw_i = pd.DataFrame()
            return df_tb_profiler_raw_i
    
    def runResfinder(self,id_i: str,seq_files: List,output_dir: Path,scheme: str,platform: str,raw_seq: bool) -> None:
            try:
                if raw_seq:
                    base_cmd = f'docker run --rm --user {self.uidDocker}:{+self.gidDocker} -v '+ "${HOME}:${HOME} -w ${PWD} " + f'{self.resfinderVer} -s "{scheme}" -o {output_dir} -l 0.6 -t 0.8 -acq --point --ignore_missing_species'
                    if len(seq_files) == 1:
                        if "nanopore" in platform.lower() or "oxford" in platform.lower():
                            base_cmd += " --nanopore"
                        cmd_resfinder = f'{base_cmd} -ifq {seq_files[0]}'
                    elif len(seq_files) > 1:
                        cmd_resfinder = f'{base_cmd} -ifq {" ".join(seq_files[:2])}'
                    else:
                        raise KeyError("No sequence files found!")
                else:
                    cmd_resfinder = f'docker run --rm --user {self.uidDocker}:{+self.gidDocker} -v '+ "${HOME}:${HOME} -w ${PWD} " + f'{self.resfinderVer} -s "{scheme}" -o {output_dir} -l 0.6 -t 0.8 -acq --point --ignore_missing_species -ifa {seq_files}'

                result_out = subprocess.run(cmd_resfinder, shell=True,capture_output=True)
                if self.keepLog:
                    self.errorsLogFun.error_logs(cmd_resfinder,result_out)

            except Exception as e:
                if self.keepLog:
                    self.errorsLogFun.error_logs_try(f"Error in Resfinder : {id_i}",e)
    
    def runTbProfiler(self,platform: str,seq_files: List,output_dir: Path,id_i: str) -> None:
        try:
            if "pacbio" in platform.lower() or 'PACBIO_SMRT' in platform.lower():
                tb_pro_platform = 'pacbio'
            if "nanopore" in platform.lower() or "oxford" in platform.lower():
                tb_pro_platform = 'nanopore'
            else:
                tb_pro_platform = 'illumina'
            if len(seq_files) == 1:
                cmd_tb_profiler = f'docker run --rm --user {self.uidDocker}:{+self.gidDocker} -v '+ "${HOME}:${HOME} -w ${PWD} " + f'{self.tbprofilerVer} profile -t 4 --ram 5 --depth 10,10 --af 0.05,0.1 --sv_depth 10,10 --logging CRITICAL --platform {tb_pro_platform}  -1 {seq_files[0]} --dir {output_dir}'
            elif len(seq_files) > 1:
                cmd_tb_profiler = f'docker run --rm --user {self.uidDocker}:{+self.gidDocker} -v '+ "${HOME}:${HOME} -w ${PWD} " + f'{self.tbprofilerVer} profile -t 4 --ram 5 --depth 10,10 --af 0.05,0.1 --sv_depth 10,10 --logging CRITICAL --platform {tb_pro_platform}  -1 {seq_files[0]} -2 {seq_files[0]} --dir {output_dir}'
            else:
                raise KeyError("No sequence files found!")
            tbprofilerOut = subprocess.run(cmd_tb_profiler, shell=True,capture_output=True)
            if self.keepLog:
                self.errorsLogFun.error_logs(cmd_tb_profiler,tbprofilerOut)

        except Exception as e:
            if self.keepLog:
                self.errorsLogFun.error_logs_try(f"Error in TB-Profiler: {id_i}",e)
   
    def process_raw_seq(self,id_i: str,organism_i: str,seq_files_list: List,platform_i: str,output_dir_i: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        raw_seq = True
        scheme = self.get_scheme(organism_i)
        self.runResfinder(id_i=id_i,seq_files=seq_files_list,output_dir=output_dir_i,scheme=scheme,platform=platform_i,raw_seq=raw_seq)
        df_resfinder_raw_i,software_version,resfinder_db_version = self.result_resfinder(output_dir_i,id_i)
        df_pointfinder_raw_i,point_db_version = self.result_pointfinder(output_dir_i,id_i)
        df_resfinder_line_i, df_pointfinder_line_i = self.to_one_line_resfinder_result(df_resfinder_raw_i,df_pointfinder_raw_i,id_i,software_version,resfinder_db_version,point_db_version)
        if scheme == "mycobacterium tuberculosis":
            self.runTbProfiler(platform_i,seq_files_list,output_dir_i,id_i)
            df_tb_profiler_raw_i = self.result_tbprofiler(output_dir_i,id_i)
        else:
            df_tb_profiler_raw_i = pd.DataFrame()
        return df_resfinder_raw_i,df_pointfinder_raw_i,df_resfinder_line_i,df_pointfinder_line_i,df_tb_profiler_raw_i
    
    def process_assembly_seq(self,id_i: str,organism_i: str,seq_files_list: List,output_dir_i: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        raw_seq = False
        platform_i = ""
        scheme = self.get_scheme(organism_i)
        self.runResfinder(id_i,seq_files_list,output_dir_i,scheme,platform_i,raw_seq)
        df_resfinder_raw_i,software_version,resfinder_db_version = self.result_resfinder(output_dir_i,id_i)
        df_pointfinder_raw_i,point_db_version = self.result_pointfinder(output_dir_i,id_i)
        df_resfinder_line_i, df_pointfinder_line_i = self.to_one_line_resfinder_result(df_resfinder_raw_i,df_pointfinder_raw_i,id_i,software_version,resfinder_db_version,point_db_version)
        df_tb_profiler_raw_i = pd.DataFrame()
        return df_resfinder_raw_i,df_pointfinder_raw_i,df_resfinder_line_i,df_pointfinder_line_i,df_tb_profiler_raw_i