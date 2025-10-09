#!/usr/bin/env python
import pandas as pd
import argparse
import time
import os
import json 
import sys
from argparse import RawTextHelpFormatter
# import os, sys
# currentdir = os.path.dirname(os.path.realpath(__file__))
# packagesdir = os.path.join(currentdir, "cenmigdb_function")
# sys.path.append(packagesdir)

# =============================================================================
# Module 
# =============================================================================

from src.download_metadata import download_all_metadata
from src.process_metadata import processMeta
from src.process_cenmigDB import cenmigDBMetaData
from src.process_sequence import processAllSeqData
from src.update_prog_db import updateResfinder,updateStringMLSTDB,updateKrocus
# =============================================================================
# Function
# =============================================================================

def inhouse_metadata(file_inhouse,option):
    print("Start Add New In-House Metadata")
    if option.lower() == "all":
        process_meta = processMeta()
        process_seq = processAllSeqData()
        df_metadata_inhouse = process_meta.process_inhouse(file_inhouse)
        process_seq.process(df_metadata_inhouse)
    elif run_metadata == "process_meta":
        process_meta = processMeta()
        df_metadata_inhouse = process_meta.process_inhouse(file_inhouse)
        print("Files Metadata will save at -> result_metada folder")

    elif run_metadata.lower() == "process_seq":
        process_seq = processAllSeqData()
        df_metadata_inhouse = pd.read_csv(file_inhouse,low_memory=False)
        process_seq.process(df_metadata_inhouse)
    else:
        print("Error at argument -> pass_metadata")
    print("Finish Add New In-House Metadata")
        
def metadata_ncbi(option,file_meta = None):
    print("Start Add New NCBI Metadata")
    if option.lower() == "all":
        process_meta = processMeta()
        cenmigDB = cenmigDBMetaData()
        process_seq = processAllSeqData()
        _ , date_new = download_all_metadata()
        cenmigDB.update_date(date_new)
        df_metadata_ncbi = process_meta.process()
        process_seq.process(df_metadata_ncbi)
        
    elif option.lower() == "process_seq":
        process_seq = processAllSeqData()
        with open("config.json", 'r') as f:
            config = json.load(f)
            config = config["processMetadata"]
        saveMetadataFile = config["saveMetadataFile"]
        if os.path.exists(saveMetadataFile):
            df_metadata_ncbi = pd.read_csv(saveMetadataFile,low_memory=False)
        else:
            print(f"No metadata file : {saveMetadataFile}")
            if file_meta:
                if os.path.exists(file_meta):
                    df_metadata_ncbi = pd.read_csv(saveMetadataFile,low_memory=False)
            else:
                sys.exit("No metadata file!")   
        process_seq.process(df_metadata_ncbi)

    elif option.lower() == "download":
        process_meta = processMeta()
        cenmigDB = cenmigDBMetaData()
        process_seq = processAllSeqData()
        _ , date_new = download_all_metadata()
        cenmigDB.update_date(date_new)
        print("Files raw metadata will save at -> raw_metadata folder")
        
    elif option.lower() == "process_meta":
        process_meta = processMeta()
        _ = process_meta.process()
        print("Files Metadata will save at -> result_metada folder")
    else:
        print("No Options argument")
    print("Finish Add New NCBI Metadata")

def update_cenmig_database(csv_file_in):
    print("Start Update Metadata")
    df_update_metadata = pd.read_csv(csv_file_in,engine='python',encoding="utf-8")
    print(df_update_metadata)
    confirm_metadata = input('This Data are Data that you Want to update? Y/N :')
    cenmigDB = cenmigDBMetaData()
    if confirm_metadata.lower() == 'y':
        cenmigDB.update_record_by_csv(df_update_metadata)
        print("Finish Update metadata to CENMIGDB")
    else:
        sys.exit("Not update metadata!") 

def delete_metadata_in_cenmigdb(csv_file_delete):
    print("Start Delete Metadata")
    df_del_metadata = pd.read_csv(csv_file_delete,engine='python',encoding="utf-8")
    print(df_del_metadata)
    confirm_metadata = input('This Data are Data that you Want to Delete? Y/N :')
    cenmigDB = cenmigDBMetaData()
    if confirm_metadata.lower() == 'y':
        cenmigDB.del_records_by_csv(df_del_metadata)
        print("Finish delete metadata in CENMIGDB")
    else:
        sys.exit("Not update metadata!") 

def update_prog_db():
    print("Start update StringMLST database!")
    updateStringMLSTDB().update()
    print("Start update resfinder database!")
    updateResfinder().update()
    print("Start update krocus database!")
    updateKrocus().update()
    print("All update competed!")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='This program is using for interact with the CENMIGDB via command-line \n'
                                    'For Download metadata : http://10.9.63.33:8080/',formatter_class=RawTextHelpFormatter)
    ## Build Sub-parser for each function
    subparsers = parser.add_subparsers(dest='command') # dest = 'command' > specify object name command
    
    ### Add New Metadata from NCBI
    des_add_new_ncbi = 'Download and add new metadata from NCBI to CENMIGDB - No optional arguments'
    add_new_ncbi = subparsers.add_parser('add_new_ncbi', help='Download and add new metadata from NCBI to CENMIGDB', description = des_add_new_ncbi)
    add_new_ncbi.add_argument("--options_metadata", "-m", help="Run all pipeline only download or use downloaded data option: all/download_only/use_downloaded/make_meta") 

    ### Add New Metadata from Inhouse
    des_add_new_inhouse = 'Add new metadata from In-house to CENMIGDB\n*Please check columns name in CSV file before run'
    add_new_inhouse = subparsers.add_parser('add_new_inhouse', help='Add new metadata from In-House to CENMIGDB\n'
                                            'Options:\n'
                                            '--csv_in_filename, -i\t Specific csv input filename can use dirct path or only filename\n'
                                            '--csv_out_filename, -o\t Specific csv output filename can use dirct path or only filename\n ', description = des_add_new_inhouse)
    add_new_inhouse.add_argument("--csv_in_filename", "-i", help="Specific csv input filename can use dirct path or only filename") 
    # add_new_inhouse.add_argument("--csv_out_filename","-o",help="Specific csv output filename can use dirct path or only filename")
    add_new_inhouse.add_argument("--pass_metadata","-m",help="Pass function for add cenmigID and fixing metadata option: yes/no")

    ### Update_DB
    update_db_help = 'Update metadata in CENMIGDB\n *Please Check your data that you want to update before run'
    update_metadata = subparsers.add_parser('update_metadata', help='Update metadata in CENMIGDB\n'
                                'Options:\n'
                                    '--csv_in_filename, -i\t Specific csv input filename can use dirct path or only filename\n ',description = update_db_help )
    update_metadata.add_argument("--csv_in_filename", "-i", help="Specific csv input filename can use dirct path or only filename", default= True) 

    ### Delete_DB
    delete_db_help = 'Delete metadata in CENMIGDB\n*Please check your data that you want to delete before run'
    delete_metadata = subparsers.add_parser('delete_metadata', help='Delete metadata in CENMIGDB\n'
                                'Options:\n'
                                    '--csv_in_filename, -i\t Specific csv input filename can use dirct path or only filename\n ',description = delete_db_help )
    delete_metadata.add_argument("--csv_in_filename", "-i", help="Specific csv input filename can use dirct path or only filename", default= True)

    ### Update_database_mlst
    update_database_mlst_help = 'Update MLST database\n*Please check your data that you want to delete before run'
    up_db_mlst = subparsers.add_parser('update_mlst_resfinder_database', help='Update mlst database\n',description = update_database_mlst_help )
    args = parser.parse_args()

    # =============================================================================
    #  Link argument to execute function
    # =============================================================================

    if args.command == 'add_new_ncbi':
        not_download = args.options_metadata
        start = time.time()
        add_new_ncbi_metadata(not_download)
        end = time.time()
        total_time = round((end - start)/(60*60),2)
        print("Time taken: {} hours".format(total_time))
        
    elif args.command == 'add_new_inhouse':
        start = time.time()
        csv_file_in = args.csv_in_filename
        run_metadata = args.pass_metadata
        if csv_file_in == None:
            print('No CSV Argument Please in add file name...')
        else:
            add_new_inhouse_metadata(csv_file_in,run_metadata)
        end = time.time()
        total_time = round((end - start)/(60*60),2)
        print("Time taken: {} hours".format(total_time))

    elif args.command == 'update_metadata':
        start = time.time()
        csv_file_in = args.csv_in_filename
        if csv_file_in == None:
            print('No CSV Argument Please in add file name...')
        else:
            update_cenmig_database(csv_file_in)
        end = time.time()
        total_time = round((end - start)/(60*60),2)
        print("Time taken: {} hours".format(total_time))

    elif args.command == 'delete_metadata':
        start = time.time()
        csv_file_delete = args.csv_in_filename
        if csv_file_delete == None:
            print('No CSV Argument Please in add file name...')
        else:
            delete_metadata_in_cenmigdb(csv_file_delete)
        end = time.time()
        total_time = round((end - start)/(60*60),2)
        print("Time taken: {} hours".format(total_time))

    elif args.command == 'update_mlst_resfinder_database':
        start = time.time()
        end = time.time()
        total_time = round((end - start)/(60*60),2)
        print("Time taken: {} hours".format(total_time))
    else:
        print('No subcommand specified')
