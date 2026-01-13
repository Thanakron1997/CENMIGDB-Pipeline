import os
import re
import glob
import time
import json
import difflib
import hashlib
import pycountry
import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime 
from multiprocessing import Pool
from src.errors import errorsLog
from typing import Dict, Any,List,Tuple
from src.download_metadata import download_metadata
from src.process_cenmigDB import cenmigDBMetaData, cenmigDBGridFS

class metadataSra:
    def __init__(self):
        self.errorsLogFun = errorsLog()
        self.main = os.path.dirname(os.path.realpath(__file__)) + '/'
        self.sraruntable_path = os.path.join(self.main,'raw_metadata/SraRun*')
        self.save_missingsra_path = os.path.join(self.main,'raw_metadata/')
        with open("config.json", 'r') as f:
            config = json.load(f)
            config = config["processMetadata"]
        self.verbose = config["verbose"]
        self.keepLog = config["keepLog"]
        self.columnDict = config["columnDict"]
        self.coreUsed = config["coreUsed"]
        self.reDownload = config["reDownload"]

    def metadata_from_sraruntable(self) -> Tuple[pd.DataFrame, Any | List]:
        file_srarun_all = glob.glob(self.sraruntable_path)
        columns_select_for_srarun = ['Run','ReleaseDate','AssemblyName','Experiment','LibraryStrategy','LibrarySelection','LibrarySource','LibraryLayout','Platform','Model','BioProject','BioSample','ScientificName','SampleName','CenterName']
        df_srarun_list = []
        for i in file_srarun_all:
            try:
                df_i = pd.read_csv(i, encoding="utf-8-sig",low_memory=False) #SraRunTable.txt อยู่ใน format csv เลยใช้ pandas อ่าน
                df_srarun_list.append(df_i)
            except Exception as e:
                if self.keepLog:
                    self.errorsLogFun.error_logs_try("Error in metadata_from_sraruntable function:  ",e)
                if self.verbose:
                    print(f"Can not load data from {i} file.")
        df_all_srarun = pd.concat(df_srarun_list, ignore_index= True)
        try:
            df_all_srarun = df_all_srarun[df_all_srarun['Run'] != 'Run']
        except Exception as e:
            if self.keepLog:
                self.errorsLogFun.error_logs_try("Error in Drop Row Run:  ",e)
            if self.verbose:
                print(f"IDK.")
        df_all_srarun = df_all_srarun[columns_select_for_srarun] # select columns
        list_sra_run_new = df_all_srarun['Run'].values.tolist()

        return (df_all_srarun,list_sra_run_new)
    
    def combine_colnames_ignorecase(self,df: pd.DataFrame) -> pd.DataFrame: #ถ้าเจอ columns name คล้ายกันให้มาเพิ่มในนี้
        df_out = pd.DataFrame()
        regex_list = []
        for key in self.columnDict.keys():
            value = self.columnDict[key]
            value = set(value)
            for i in value: 
                i = '^' + i + '$' # create RegEx for find column like  ^ = Starts with , $  = End with
                i = i.replace('_', '.*?')  ## ? mean there is one or none
                i = i.replace(' ', '.')  # . mean Any character (except newline character)
                i = i.replace('(', '.')
                i = i.replace(')', '.')
                regex_list.append(i)
        # Join regex to one str
        regex_query = '|'.join(regex_list)
        # Convert to series for str.contains method
        df_colname = df.columns
        col_series = pd.Series(df_colname)
        col_series = col_series[col_series.str.contains(regex_query, case = False, regex = True)]
        # Extract all column that in dict value
        df_x = df[list(set(col_series))] # get data in column that match in col_series
        df_x = df_x.fillna('')  # df_x.fillna('', inplace=True)
        col_df_x = df_x.columns
        # Merge col in each key into one col
        for col in self.columnDict.keys():
            rex = []
            col_list = self.columnDict[col]
            for i in col_list:
                i = '^' + i + '$'
                i = i.replace('_', '.*?')
                i = i.replace(' ', '.')
                i = i.replace('(', '.')
                i = i.replace(')', '.')
                rex.append(i)
            rex_query = '|'.join(rex) ## create RegEx form value in dict for get values in raw df
            combine_target = col_df_x[col_df_x.str.contains(rex_query, case = False, regex =True)]
            ## Hard code since host*age can match hostXXXXstage
            if str(col) == 'host_age':
                combine_target = combine_target[~combine_target.str.contains('.*stage$',case = False, regex =True)]        
            combine_target = list(combine_target)
            df_out[str(col)] = df_x[combine_target].astype(str).agg(lambda x: ' '.join(x.unique()), axis=1) # Combine unique value in group data match in combine target
        df_out = df_out.replace(to_replace= r'\\', value= '', regex=True)
        return df_out
    
    def process_srainfo_file(self,file_path) -> pd.DataFrame:
        df = pd.read_table(file_path, sep = '\t',low_memory=False)
        df = self.combine_colnames_ignorecase(df)
        return df
    
    # def merge_srainfo(self) -> pd.DataFrame:
    #     srainfo_path = os.path.join(self.main,'.raw_metadata/*.srainfo')
    #     all_srainfo_file_path = glob.glob(srainfo_path)
    #     with Pool(processes=self.coreUsed) as pool:
    #         # Wrap pool.map with tqdm for progress bar
    #         list_all_srainfo = list(tqdm(pool.imap(self.process_srainfo_file, all_srainfo_file_path), total=len(all_srainfo_file_path), desc="merging srainfo", ncols=70))
    #     df_all_srainfo = pd.concat(list_all_srainfo, ignore_index=True)
    #     df_all_srainfo.drop_duplicates(subset=['Run'], keep='last', inplace=True)
    #     df_all_srainfo.reset_index(drop=True, inplace=True)
    #     return df_all_srainfo
    # More memory-efficient?
    def merge_srainfo(self) -> pd.DataFrame:
        srainfo_path = os.path.join(self.main, 'raw_metadata/*.srainfo')
        all_srainfo_file_path = glob.glob(srainfo_path)
        if len(all_srainfo_file_path) == 0:
            if self.verbose:
                print("No SRA info Found!")
            return pd.DataFrame({'Run':[]})
        def gen():
            with Pool(processes=self.coreUsed) as pool:
                for df in tqdm(pool.imap(self.process_srainfo_file, all_srainfo_file_path),
                            total=len(all_srainfo_file_path), desc="merging srainfo", ncols=70,colour="#00FF21",leave=True):
                    yield df

        df_all_srainfo = pd.concat(gen(), ignore_index=True)
        df_all_srainfo.drop_duplicates(subset=['Run'], keep='last', inplace=True)
        df_all_srainfo.reset_index(drop=True, inplace=True)
        return df_all_srainfo
    
    def clean_missing_files(self) -> None:
        all_file_missing_sra_path = os.path.join(self.save_missingsra_path,'missing_sra_*')
        file_missing_sra_all = glob.glob(all_file_missing_sra_path)
        if len(file_missing_sra_all) >0:
            for f in file_missing_sra_all:
                os.remove(f)

    # download sra metadata from pathogen metadata
    def update_new_sra_from_pathogen(self,list_sra_new_pathogen,df_new_assembly_n_sra_from_metadata) -> pd.DataFrame:
        if len(list_sra_new_pathogen) > 0:
            all_file_missing_sra_path = os.path.join(self.save_missingsra_path,'missing_sra_*') # save_missingsra_path + 'missing_sra_*'
            file_missing_sra_all = glob.glob(all_file_missing_sra_path)
            if len(file_missing_sra_all) >0:
                try:
                    df_missing_sra_list = []
                    for i in file_missing_sra_all:
                        file_size = os.path.getsize(i)
                        if file_size > 0:
                            df_i = pd.read_csv(i, encoding="utf-8-sig",low_memory=False) #SraRunTable.txt อยู่ใน format csv เลยใช้ pandas อ่าน
                            df_missing_sra_list.append(df_i)
                        else:
                            if self.verbose:
                                print(f"file {i} is empty {file_size} bytes")
                    df_new_sra_from_pathogen = pd.concat(df_missing_sra_list, ignore_index= True)
                    # df_new_sra_from_pathogen = pd.read_csv(missing_sra_file,encoding="utf-8-sig", engine='python')
                    try:
                        df_new_sra_from_pathogen = df_new_sra_from_pathogen[df_new_sra_from_pathogen.Run != 'Run']
                    except Exception as e:
                        if self.keepLog:
                            self.errorsLogFun.error_logs_try("Error in Delete row == Run:  ",e)
                    columns_select_for_missing_sra = ['Run','ReleaseDate','Experiment','LibraryStrategy','LibrarySelection','LibrarySource','LibraryLayout','Platform','Model','BioProject','BioSample','ScientificName','SampleName','CenterName']
                    df_new_sra_from_pathogen = df_new_sra_from_pathogen[columns_select_for_missing_sra]
                    if df_new_sra_from_pathogen['Run'].isin(df_new_assembly_n_sra_from_metadata['Run'].values.tolist()).any(): # check if sra number from assembly in missing sra 
                        df_new_assembly_for_addsembly = df_new_assembly_n_sra_from_metadata[['Run','asm_acc']] # add assembly number to sra
                        df_new_assembly_for_addsembly = df_new_assembly_for_addsembly.rename(columns = {'asm_acc':'AssemblyName'}) ####
                        df_new_sra_from_pathogen = df_new_sra_from_pathogen.merge(df_new_assembly_for_addsembly,how='left', on='Run')
                except Exception as e:
                    if self.keepLog:
                            self.errorsLogFun.error_logs_try("Can not add data from sra pathogen: ",e)
                    if self.verbose:
                        print(f'Can not add data from sra pathogen - Error -> {e}')
            else:
                df_new_sra_from_pathogen = pd.DataFrame()
        else:
            df_new_sra_from_pathogen = pd.DataFrame()
            if self.verbose:
                    print("No missing data found!")
        return df_new_sra_from_pathogen
    
class metadataPathogen:
    def __init__(self):
        self.errorsLogFun = errorsLog()
        self.main = os.path.dirname(os.path.realpath(__file__)) + '/'
        self.pathogen_metadata_path = os.path.join(self.main,'raw_metadata/*metadata.csv')
        with open("config.json", 'r') as f:
            config = json.load(f)
            config = config["processMetadata"]
        self.verbose = config["verbose"]
        self.keepLog = config["keepLog"]
        self.columnDict = config["columnDict"]
        self.coreUsed = config["coreUsed"]
        self.listSpecies = config["listSpecies"]
        self.pathogenSelect = config["pathogenSelect"]

    # founction for multiprocess
    def process_pathogen_metada(self,pathogen_metadata_i: str) -> pd.DataFrame:
        df_i = pd.read_csv(pathogen_metadata_i, encoding="utf-8-sig", on_bad_lines='skip',low_memory=False)
        return df_i

    def merge_pathogen_metada(self) -> pd.DataFrame: 
        df_pathogen_metada = pd.DataFrame()
        file_pathogen_i = glob.glob(self.pathogen_metadata_path)
        with Pool(processes=self.coreUsed) as pool:
            list_all_pathogen_metadata = list(tqdm(pool.imap(self.process_pathogen_metada, file_pathogen_i), total=len(file_pathogen_i), desc="merging pathogen", ncols=70,colour="#00FF21",leave=True))
        df_pathogen_metada = pd.concat(list_all_pathogen_metadata, ignore_index=True)
        df_pathogen_metada = df_pathogen_metada.reset_index(drop=True)
        df_pathogen_no_dup = df_pathogen_metada.drop_duplicates(keep= 'first')
        regex_query = []
        for sp in self.listSpecies:
            sp = sp.replace(' ', '.')
            sp = sp + '*'
            regex_query.append(sp)
        regex_query = '|'.join(regex_query)
        df_pathogen_select_sp = df_pathogen_no_dup[df_pathogen_no_dup['scientific_name'].str.contains(regex_query, case=False, na=False)]
        df_pathogen_select_sp = df_pathogen_select_sp.reset_index(drop=True)
        df_pathogen_select_sp = df_pathogen_select_sp[self.pathogenSelect]
        return df_pathogen_select_sp

    def all_assembly_metata(self,df_new_assembly: pd.DataFrame) -> Tuple[pd.DataFrame, List]:

        list_bioproject_assembly = df_new_assembly['bioproject_acc'].values.tolist()
        new_col_name = {'bioproject_acc':'BioProject','biosample_acc':'BioSample','collected_by':'Center_Name','collection_date': 'Collection_date','geo_loc_name':'geo_loc_name_country','scientific_name':'Organism','serovar':'Serovar','strain':'Strain'}
        df_new_assembly_rename = df_new_assembly.rename(columns = new_col_name)
        return df_new_assembly_rename,list_bioproject_assembly

    # merge bio assembly metadata from bioproject
    def merge_bio_assembly(self) -> pd.DataFrame:
        bio_assembly_file = os.path.join(self.main,'raw_metadata/*_assembly.csv')
        all_bioproject_assembly_file_path = glob.glob(bio_assembly_file)
        list_all_bio_assembly = []
        if len(all_bioproject_assembly_file_path) > 0:
            for i in all_bioproject_assembly_file_path:
                file_size = os.path.getsize(i)
                if file_size > 0:
                    df_i = pd.read_table(i, sep = '\t',low_memory=False)
                    list_all_bio_assembly.append(df_i)
                else:
                    if self.verbose:
                        print(f"file {i} is empty {file_size} bytes")
            df_all_bioproject_assembly = pd.concat(list_all_bio_assembly, ignore_index=True)
            df_all_bioproject_assembly.drop_duplicates(subset = ['Assembly Accession'] ,keep='first', inplace=True)
            df_all_bioproject_assembly = df_all_bioproject_assembly.reset_index(drop=True) # Reset index before filterout
            df_all_bio_ass_col = ['Assembly Accession','Annotation Release Date','Assembly Sequencing Tech','Assembly BioSample Sample Identifiers Value']
            df_all_bioproject_assembly = df_all_bioproject_assembly[df_all_bio_ass_col]
            df_all_bioproject_assembly = df_all_bioproject_assembly.rename(columns = {'Assembly Accession':'asm_acc','Annotation Release Date':'ReleaseDate','Assembly Sequencing Tech':'Platform','Assembly BioSample Sample Identifiers Value':'BiosampleIdentifiers'})
        else:
            df_all_bioproject_assembly = pd.DataFrame({'asm_acc':[]})
        return df_all_bioproject_assembly

class processMeta:
    def __init__(self):
        self.errorsLogFun = errorsLog()
        self.cenmigDB = cenmigDBMetaData()
        self.cenmigDBGridFS = cenmigDBGridFS()
        self.main = os.path.dirname(os.path.realpath(__file__)) + '/'
        with open("config.json", 'r') as f:
            config = json.load(f)
            config = config["processMetadata"]
        self.verbose = config["verbose"]
        self.keepLog = config["keepLog"]
        self.coreUsed = config["coreUsed"]
        self.reDownloadSra  = config["reDownloadSra"]
        self.removeCENMIGID = config["removeCENMIGID"]
        self.geoFile = config["geoFile"]
        self.saveMetadataFile = config["saveMetadataFile"]
        self.saveInhouseFile = config["saveInhouseFile"]
        self.inhouseSeqDir = config["inhouseSeqDir"]
        self.DownloadSRAPathogen = config["DownloadSRAPathogen"]
        self.listOrganism = config["listOrganism"]
        saveMetaPath = os.path.join(self.main,"result_metada")
        if not os.path.exists(saveMetaPath):
            os.mkdir(saveMetaPath)

    def process_date(self,date):
        try:
            current_year = datetime.now().year
            lst_year = list(range(1800, current_year + 1))
            if date and isinstance(date, (str)):
                year_match = re.search(r'\d{4}', date)
                if year_match:
                    year_str = year_match.group()
                    if int(year_str) in lst_year:
                        return int(year_str)
                    else:
                        return None
                else:
                    return None
            elif date in lst_year:
                return int(date)
            else:
                return None
        except:
            return None
        
    # connect CENMIG Database
    def get_old_data(self) -> Tuple[List,List,List]:
        client = self.cenmigDB.connect_mongodb()
        db = client['metadata']
        metadata_database = db["bacteria"]
        try:
            data = metadata_database.find({}, {'_id': 0 ,'Run' : 1, 'asm_acc' : 1, 'cenmigID' : 1})
            data = pd.DataFrame(list(data))
            data = data[['Run', 'cenmigID', 'asm_acc']]
            cenmigID_old = data['cenmigID'].dropna()
            run_old = data['Run'].dropna()
            asmacc_old = data['asm_acc'].dropna()
            print('Retriving metdata completed')
        except Exception as e:
            if self.keepLog:
                self.errorsLogFun.error_logs_try('Cant find Database and retrive old id',e)
            if self.verbose:
                print('Cant find Database and retrive old id')
            cenmigID_old = list()
            run_old = list()
            asmacc_old = list()
        client.close()
        return list(run_old), list(cenmigID_old), list(asmacc_old)
    
    def updateDatatoMongodb(self,x: str) -> str:
        re_id = []
        lstFiles = x.split(", ")
        for i in lstFiles:
            try:
                id_i =  self.cenmigDBGridFS.update_item_to_db(i,self.inhouseSeqDir)
                re_id.append(str(id_i))
            except Exception as e:
                if self.keepLog:
                    self.errorsLogFun.error_logs_try(f"Can't update file: {i} ",e)
        if len(re_id) > 0:
            return ", ".join(re_id)
        else:
            if self.verbose:
                print(f"Can't update file: {x} to database!")
            return "Can't Update"
            
    # split data when have semicolon and get only text brfore semicolon
    def split_semicolon(self,i : Any) -> str:
        if type(i) == str:
            if ':' in i:
                txt = i.split(':')[0]
            elif ' :' in i:
                txt = i.split(' :')[0]
            elif ': ' in i:
                txt = i.split(': ')[0]
            elif ' : ' in i:
                txt = i.split(' : ')[0]
            else:
                return i
            return txt
        else:
            return ''
    
    def addCENMIGID(self,x: pd.Series) -> str:
        samName = x['Sample_Name']
        id_ = samName + str(time.time())
        idd = hashlib.sha1(id_.encode('utf-8')).hexdigest()
        return "IH_" +str(idd)

    #add cenmigID to data
    def cenmigID_assigner(self,df: pd.DataFrame) -> pd.DataFrame:
        df_cenmig = df[['Run', 'asm_acc']]
        df_cenmig = df_cenmig.copy()
        df_cenmig.loc[:, 'cenmigID'] = np.where(df['Run'].notnull(), df['Run'], df['asm_acc'].combine_first(df['Run']))
        cenmigID = df_cenmig['cenmigID']
        df.insert(0, 'cenmigID', cenmigID)
        return df

    # get sub region from UNGEO csv file
    def ungeo_subregion(self,df_all_new_metadata_update_country: pd.DataFrame) -> pd.DataFrame:
        ungeo_file = os.path.join(self.main,self.geoFile)
        cn_geo = pd.read_csv(ungeo_file,low_memory=False)
        cn_geo = cn_geo[['Country','Sub-region Name']]
        cn_geo.columns = ['geo_loc_name_country_fix','sub_region']   
        df_all_new_metadata_update_country_subregion = pd.merge(df_all_new_metadata_update_country, cn_geo, on='geo_loc_name_country_fix',how='left', indicator=False)
        return df_all_new_metadata_update_country_subregion

    # Use pycountry for get correct country name and replace name is not correct
    def dict_for_correct_country(self,countryname_fix: List) -> Dict:
        countryname_set = set(countryname_fix)
        country_names = [country.name.upper() for country in pycountry.countries]  # type: ignore
        country_dict = {}
        for data in countryname_set:
            if type(data) != float:
                match_countries = difflib.get_close_matches(data.upper(), country_names)  # type: ignore
                if len(match_countries) > 0:
                    match = match_countries[0]
                elif len(match_countries) == 0:
                    match = 'Missing'
                # Fix with hard code
                if 'KOREA' in data.upper():
                    match = 'KOREA, REPUBLIC OF'
                elif 'LAOS' in data.upper():
                    match = "LAO PEOPLE'S DEMOCRATIC REPUBLIC"
                elif 'TAIWAN' in data.upper():
                    match = 'TAIWAN, PROVINCE OF CHINA'
                elif 'SYRIA' in data.upper():
                    match = 'SYRIAN ARAB REPUBLIC'
                elif 'TANZANIA' in data.upper():
                    match = 'TANZANIA, UNITED REPUBLIC OF'
                elif 'RUSSIA' in data.upper():
                    match = 'RUSSIAN FEDERATION'
                elif 'VENEZUELA' in data.upper():
                    match = 'VENEZUELA, BOLIVARIAN REPUBLIC OF'
                elif 'PALESTINE' in data.upper():
                    match = 'PALESTINE, STATE OF'
                elif 'SPAIN' in data.upper():
                    match = 'SPAIN'
                add_dict = {str(data) : str(match)}
                country_dict.update(add_dict)
        return country_dict
       
    # add pathogen metadata to sra
    def add_pathogen_to_sra_metadata(self,update_df_all_sra_metada: pd.DataFrame,df_new_pathogen_metada: pd.DataFrame) -> pd.DataFrame:
        pathogem_col = ['Run','asm_level','asm_stats_contig_n50','asm_stats_length_bp','asm_stats_n_contig','assembly_method','AST_phenotypes','AMR_genotypes','AMR_genotypes_core','stress_genotypes','amrfinder_version','refgene_db_version','amrfinder_analysis_type']
        df_pathogen_select_col = df_new_pathogen_metada[pathogem_col]
        df_sra_add_pathogen = pd.merge(update_df_all_sra_metada,df_pathogen_select_col,how='left', on='Run')
        df_sra_rename_col = {'AssemblyName':'asm_acc','ScientificName' : 'Organism','CenterName' : 'Center_Name','SampleName':'Sample_Name'}
        df_sra_add_pathogen = df_sra_add_pathogen.rename(columns = df_sra_rename_col)
        return df_sra_add_pathogen  
    
    # clean country name
    def update_country_all_metadata(self,df_all_new_metadata: pd.DataFrame) -> pd.DataFrame:
        df_all_new_metadata['geo_loc_name_country'] =  df_all_new_metadata['geo_loc_name_country'].apply(self.split_semicolon)
        df_all_new_metadata['geo_loc_name_country_fix'] = df_all_new_metadata['geo_loc_name_country'].replace({r'\d+': np.nan, 'nan': np.nan, 'USA.*' : 'United states','United Kingdom.*': 'United Kingdom','Brazil.*': 'Brazil','Australia,*' : 'Australia'}, regex=True)
        # Fix country name 2nd
        correct_dict = self.dict_for_correct_country(df_all_new_metadata['geo_loc_name_country_fix'].to_list())
        #replace correct country name to metadata dataframe 
        df_all_new_metadata['geo_loc_name_country_fix'] = df_all_new_metadata['geo_loc_name_country_fix'].replace(correct_dict)
        return df_all_new_metadata

    def process(self) -> pd.DataFrame:
        run_old, cenmigID_old,asmacc_old = self.get_old_data()
        meta_sra = metadataSra()
        meta_pathogen = metadataPathogen()
        download_meta = download_metadata()
        df_all_srarun,list_sra_run_new = meta_sra.metadata_from_sraruntable()
        print('--New SRA from sraRunTable--')
        print(df_all_srarun)
        print('Combine Pathogen Metadata Files.....')
        df_pathogen_select_sp = meta_pathogen.merge_pathogen_metada()
        df_new_pathogen_metada = df_pathogen_select_sp[~df_pathogen_select_sp['asm_acc'].isin(asmacc_old)] 
        # delete run id that we have in database from new metadata pathogen
        df_new_pathogen_metada = df_new_pathogen_metada[~df_new_pathogen_metada['Run'].isin(run_old)] 
        # delete run id that we have in sraruntable fron new metadata pathogen
        df_new_pathogen_metada = df_new_pathogen_metada[~df_new_pathogen_metada['Run'].isin(list_sra_run_new)]
        # select only new assembly
        df_new_assembly_n_sra_from_metadata = df_new_pathogen_metada.dropna(subset=['asm_acc'])
        #new sra from metadata pathogen
        new_sra_from_pathogen_metadata = df_new_pathogen_metada.dropna(subset=['Run'])
        list_sra_new_pathogen = new_sra_from_pathogen_metadata['Run'].values.tolist()
        if self.DownloadSRAPathogen:
            meta_sra.clean_missing_files()
            download_meta.download_sra_by_pathogen(list_sra_new_pathogen)
        df_new_sra_from_pathogen = meta_sra.update_new_sra_from_pathogen(list_sra_new_pathogen,df_new_assembly_n_sra_from_metadata)
        print('--SRA from Pathogen File--')
        print(df_new_sra_from_pathogen)
        # combine sra from two source 
        df_all_sra_metada = pd.concat([df_all_srarun,df_new_sra_from_pathogen], ignore_index=True)
        df_all_sra_metada.drop_duplicates(subset = ['Run'] ,keep='first', inplace=True)
        list_all_new_sra = df_all_sra_metada['Run'].values.tolist()
        df_new_assembly = df_new_assembly_n_sra_from_metadata[~df_new_assembly_n_sra_from_metadata['Run'].isin(list_all_new_sra)]
        print('--New assembly metada--')
        if df_new_assembly.shape[0] > 0 :
            print(df_new_assembly)
        else: 
            print('No New Assembly Metadata')
        # get bioproject from new sra for download srainfo
        if self.reDownloadSra:
            print('Re-Download SRAINFO')
            bioproject_set_new = set(df_all_sra_metada['BioProject'].dropna())
            download_meta.multi_download_sra(bioproject_set_new)
        df_all_srainfo = meta_sra.merge_srainfo()
        # Merge sra metadata and srainfo data
        update_df_all_sra_metada = pd.merge(df_all_sra_metada,df_all_srainfo,how='left', on='Run')
        # Add pathogen data to sra metadata
        df_all_sra_add_pathogen = self.add_pathogen_to_sra_metadata(update_df_all_sra_metada,df_new_pathogen_metada)
        print('--All SRA Metada (Update Pathogen)--')
        print(df_all_sra_add_pathogen)
        if df_new_assembly.shape[0] > 0 :
            # Select columns what we want in assembly metadata and download more infromation for assembly (new assembly ที่เลือก columns แล้ว)
            df_new_assembly_select_col,list_bioproject_assembly = meta_pathogen.all_assembly_metata(df_new_assembly)
            download_meta.download_metadata_assembly(list_bioproject_assembly)
            # Combline bioproject assembly metadata
            df_all_bioproject_assembly = meta_pathogen.merge_bio_assembly()
            # Merge new assembly data and bioproject assembly metadata
            df_new_complete_assembly = pd.merge(df_new_assembly_select_col,df_all_bioproject_assembly,how='left', on='asm_acc')
            if "Platform_x" in df_new_complete_assembly:
                df_new_complete_assembly['Platform'] = (df_new_complete_assembly['Platform_x'].fillna(df_new_complete_assembly['Platform_y']))
                df_new_complete_assembly.drop(df_new_complete_assembly.filter(regex='_(x|y)$').columns,axis=1,inplace=True)
            # Combine new sra metadata and assembly metadata
            df_all_new_metadata = pd.concat([df_all_sra_add_pathogen,df_new_complete_assembly],ignore_index=True)
        else: 
            df_all_new_metadata = df_all_sra_add_pathogen
        # clean country name
        df_all_new_metadata_update_country = self.update_country_all_metadata(df_all_new_metadata)
        # add Subregion
        df_all_new_metadata_update_country_subregion = self.ungeo_subregion(df_all_new_metadata_update_country)
        # Add column & cenmigID
        df_all_new_metadata_all_update = self.cenmigID_assigner(df_all_new_metadata_update_country_subregion)
        ## remove cenmigID which has in DB
        if self.removeCENMIGID:
            df_all_new_metadata_all_update = df_all_new_metadata_all_update.loc[~df_all_new_metadata_all_update['cenmigID'].isin(cenmigID_old)]
        print('--All new metada for update--')
        print(df_all_new_metadata_all_update)
        df_all_new_metadata_all_update = df_all_new_metadata_all_update.copy()
        pattern = "|".join(map(re.escape, self.listOrganism))
        df_all_new_metadata_all_update = df_all_new_metadata_all_update[df_all_new_metadata_all_update['Organism'].str.contains(pattern, na=False)]
        df_all_new_metadata_all_update['Collection_years'] = df_all_new_metadata_all_update['Collection_date'].apply(self.process_date)
        df_all_new_metadata_all_update['Collection_date'] = df_all_new_metadata_all_update['Collection_date'].astype('string')
        df_all_new_metadata_all_update.to_csv(self.saveMetadataFile,index=False)
        return df_all_new_metadata_all_update
    
    def process_inhouse(self,file_name_inhouse: str) -> pd.DataFrame:
        df_metadata_inhouse = pd.read_csv(file_name_inhouse,encoding="utf-8",low_memory=False)
        # add CENMIG ID
        df_metadata_inhouse['cenmigID'] = df_metadata_inhouse.apply(self.addCENMIGID,axis=1)
        # df_metadata_inhouse['file_name'] = df_metadata_inhouse['Sample_Name'].apply(getFileSequenceName)
        # update sequnce to mongodb
        df_metadata_inhouse['file_id'] = df_metadata_inhouse['file_name'].apply(self.updateDatatoMongodb)
        df_metadata_inhouse = self.update_country_all_metadata(df_metadata_inhouse)
        df_metadata_inhouse = self.ungeo_subregion(df_metadata_inhouse)
        df_metadata_inhouse = df_metadata_inhouse.astype('string')
        df_metadata_inhouse['Collection_years'] = df_metadata_inhouse['Collection_date'].apply(self.process_date)
        print('--All Metadata from In-House--')
        print(df_metadata_inhouse)
        df_metadata_inhouse.to_csv(self.saveInhouseFile,index=False)
        return df_metadata_inhouse