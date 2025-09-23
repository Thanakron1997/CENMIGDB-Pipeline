import os
import json
import shutil
import subprocess
import requests

class updateStringMLSTDB():
    def __init__(self):
        self.main = os.path.dirname(os.path.realpath(__file__))
        with open("config.json", 'r') as f:
                config = json.load(f)
                config = config["updateMLSTDB"]
        self.verbose = config["verbose"]
        self.mlstConfig = os.path.join(self.main,config["fileConfig"])
        if not os.path.exists(os.path.join(self.main,"mlst_db")):
            os.mkdir(os.path.join(self.main,"mlst_db"))

    def download_file_alleles(self,url_download,file_name):
        file_name = os.path.join(self.main,file_name)
        response_profile = requests.get(url_download)
        if self.verbose:
                print(f"\n wget: {url_download}")
        response_profile.raise_for_status()
        with open(file_name, 'wb') as file:
            file.write(response_profile.content)
        if self.verbose:
            print(f"Download {file_name} Completed!")

    def update(self):
        with open(self.mlstConfig, "r") as f:
            mlstRun = json.load(f)
        for i in mlstRun:
            list_download = mlstRun[i]["download"]
            for lst in list_download:
                self.download_file_alleles(lst['url'],lst['file_name'])
            cmd = f"stringMLST.py --buildDB --config {mlstRun[i]["configFile"]} -k {mlstRun[i]["ker"]} -P {mlstRun[i]["path"]}"
            if self.verbose:
                 print(f"Run: {cmd}")
            subprocess.run(cmd, shell=True,cwd="src/")
                 
class updateResfinder():
    def __init__(self):
        self.main = os.path.dirname(os.path.realpath(__file__))
        with open("config.json", 'r') as f:
            config = json.load(f)
            config = config["updateResfinder"]
        self.phenotypes_file = os.path.join(self.main,config["phenotypesFile"])
        self.phenotypes_url = config["phenotypesUrl"]
        self.verbose = config["verbose"]

    def update(self):
        response = requests.get(self.phenotypes_url)
        with open(self.phenotypes_file, 'wb') as file:
            file.write(response.content)
        if self.verbose:
             print("Download Phenotypes file for Resfinder Compeleted!")

class updateKrocus():
    def __init__(self,
            ):
        self.main = os.path.dirname(os.path.realpath(__file__))
        with open("config.json", 'r') as f:
                config = json.load(f)
                config = config["updateKrocusDB"]
        self.verbose = config["verbose"]
        self.mlstConfig = os.path.join(self.main,config["fileConfig"])
        if not os.path.exists(os.path.join(self.main,"krocus_db")):
                os.mkdir(os.path.join(self.main,"krocus_db"))
    
    def update(self):
        with open(os.path.join(self.mlstConfig), "r") as f:
            mlstRun = json.load(f)
        for i in mlstRun:
            path_ = os.path.join(self.main,mlstRun[i]["path"])
            if os.path.exists(path_):
                shutil.rmtree(path_, ignore_errors = False)
            cmd = f"krocus_database_downloader  --species {i} --output_directory {path_}"
            if self.verbose:
                print(f"Run: {cmd}")
            subprocess.run(cmd, shell=True,cwd="src/")



