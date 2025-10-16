import pandas as pd
import argparse
import time
import os
import json 
import sys
from argparse import RawTextHelpFormatter

# =============================================================================
# Module 
# =============================================================================

from src.download_metadata import download_all_metadata
from src.process_metadata import processMeta
from src.process_cenmigDB import cenmigDBMetaData
from src.process_sequence import processAllSeqData
from src.prog import checkprograms, downloadPrograms
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
        if file_meta:
            if os.path.exists(file_meta):
                df_metadata_ncbi = pd.read_csv(file_meta,low_memory=False)
        elif os.path.exists(saveMetadataFile):
            df_metadata_ncbi = pd.read_csv(saveMetadataFile,low_memory=False)
        else:
            print(f"No metadata file : {saveMetadataFile}")
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
        df_metadata_ncbi = process_meta.process()
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

def setupProgram():
    checkprograms().check(install=True)
    download = downloadPrograms()
    download.downloadEsearch()
    download.downloadSRATools()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "This program is a pipeline for management data in cenmigDB.\n\n"
            "For more information or to download metadata, visit:\n"
            "  http://10.9.63.33:8080/"
        ),
        formatter_class=RawTextHelpFormatter
    )

    # ----------------------------------------------------------------------
    # Create Subparsers
    # ----------------------------------------------------------------------
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # ----------------------------------------------------------------------
    # 1. Add New Metadata from NCBI
    # ----------------------------------------------------------------------
    des_add_new_ncbi = (
        "Download and add new metadata from NCBI to CENMIGDB.\n\n"
        "Options:\n"
        "  --option, -m          Specify which part of the pipeline to run:\n"
        "                        all           Run the full pipeline\n"
        "                        process_meta  Process only metadata\n"
        "                        process_seq   Process only sequence data\n"
        "                        download      Only download raw files\n\n"
        "  --csv_in_filename, -i Provide a manual metadata CSV input file (optional)"
    )

    add_new_ncbi = subparsers.add_parser(
        "ncbi",
        help="Download and add new metadata from NCBI to CENMIGDB",
        description=des_add_new_ncbi,
        formatter_class=RawTextHelpFormatter
    )
    add_new_ncbi.add_argument(
        "--option", "-m",
        help="Pipeline mode: all | process_meta | process_seq | download"
    )
    add_new_ncbi.add_argument(
        "--csv_in_filename", "-i",
        help="Specify manual metadata CSV input file"
    )

    # ----------------------------------------------------------------------
    # 2. Add New Metadata from In-house
    # ----------------------------------------------------------------------
    des_add_new_inhouse = (
        "Add new metadata from In-house to CENMIGDB.\n"
        "*Please check column names in the CSV file before running.\n\n"
        "Options:\n"
        "  --csv_in_filename, -i   Specify CSV input filename (path or name)\n"
        "  --option, -m            all          Run full process\n"
        "                            process_seq  Skip creating new CENMIG IDs for existing metadata"
    )

    add_new_inhouse = subparsers.add_parser(
        "inhouse",
        help="Add new metadata from In-house to CENMIGDB",
        description=des_add_new_inhouse,
        formatter_class=RawTextHelpFormatter
    )
    add_new_inhouse.add_argument(
        "--csv_in_filename", "-i",
        help="Specify CSV input filename (path or name)"
    )
    add_new_inhouse.add_argument(
        "--option", "-m",
        help="Option: all | process_seq"
    )

    # ----------------------------------------------------------------------
    # 3. Update Database
    # ----------------------------------------------------------------------
    update_db_help = (
        "Update metadata in CENMIGDB.\n"
        "*Please check your data carefully before running.\n\n"
        "Options:\n"
        "  --csv_in_filename, -i   Specify CSV input file (path or name)"
    )

    update_metadata = subparsers.add_parser(
        "update",
        help="Update metadata in CENMIGDB",
        description=update_db_help,
        formatter_class=RawTextHelpFormatter
    )
    update_metadata.add_argument(
        "--csv_in_filename", "-i",
        help="Specify CSV input filename (path or name)",
        default=True
    )

    # ----------------------------------------------------------------------
    # 4. Delete from Database
    # ----------------------------------------------------------------------
    delete_db_help = (
        "Delete metadata from CENMIGDB.\n"
        "*Please verify your data before running.\n\n"
        "Options:\n"
        "  --csv_in_filename, -i   Specify CSV input file (path or name)"
    )

    delete_metadata = subparsers.add_parser(
        "delete",
        help="Delete metadata in CENMIGDB",
        description=delete_db_help,
        formatter_class=RawTextHelpFormatter
    )
    delete_metadata.add_argument(
        "--csv_in_filename", "-i",
        help="Specify CSV input filename (path or name)",
        default=True
    )

    # ----------------------------------------------------------------------
    # 5. Update MLST Database
    # ----------------------------------------------------------------------
    update_database_mlst_help = "Update MLST database used in the pipeline."

    up_db_mlst = subparsers.add_parser(
        "updatedb",
        help="Update MLST database",
        description=update_database_mlst_help
    )

    # ----------------------------------------------------------------------
    # 6. Setup Dependencies
    # ----------------------------------------------------------------------
    setup_help = "Install required external programs for the CENMIG pipeline."

    setup_parser = subparsers.add_parser(
        "setup",
        help="Install required programs for the pipeline",
        description=setup_help
    )

    # ----------------------------------------------------------------------
    # Parse arguments
    # ----------------------------------------------------------------------
    args = parser.parse_args()


    # =============================================================================
    #  Link argument to execute function
    # =============================================================================

    if args.command == 'ncbi':
        option = args.option
        csv_file_in = args.csv_in_filename
        start = time.time()
        if csv_file_in == None:
            metadata_ncbi(option)
        else:
            metadata_ncbi(option,csv_file_in)
        end = time.time()
        total_time = round((end - start)/(60*60),2)
        print("Time taken: {} hours".format(total_time))
        
    elif args.command == 'inhouse':
        start = time.time()
        csv_file_in = args.csv_in_filename
        run_metadata = args.option
        if csv_file_in == None:
            sys.exit('No CSV Argument Please in add file name...')
        else:
            inhouse_metadata(csv_file_in,run_metadata)
        end = time.time()
        total_time = round((end - start)/(60*60),2)
        print("Time taken: {} hours".format(total_time))

    elif args.command == 'update_metadata':
        start = time.time()
        csv_file_in = args.csv_in_filename
        if csv_file_in == None:
            sys.exit('No CSV Argument Please in add file name...')
        else:
            update_cenmig_database(csv_file_in)
        end = time.time()
        total_time = round((end - start)/(60*60),2)
        print("Time taken: {} hours".format(total_time))

    elif args.command == 'delete_metadata':
        start = time.time()
        csv_file_delete = args.csv_in_filename
        if csv_file_delete == None:
            sys.exit('No CSV Argument Please in add file name...')
        else:
            delete_metadata_in_cenmigdb(csv_file_delete)
        end = time.time()
        total_time = round((end - start)/(60*60),2)
        print("Time taken: {} hours".format(total_time))

    elif args.command == 'updatedb':
        start = time.time()
        end = time.time()
        update_prog_db()
        total_time = round((end - start)/(60*60),2)
        print("Time taken: {} hours".format(total_time))
    elif args.command == 'setup':
        start = time.time()
        end = time.time()
        setupProgram()
        total_time = round((end - start)/(60*60),2)
        print("Time taken: {} hours".format(total_time))
    else:
        print('No subcommand specified')
