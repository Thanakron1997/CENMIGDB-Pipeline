import subprocess
import os
import sys
class checkprograms:
    def __init__(self):
        self.listLibUsed = ['stringMLST','krocus']
        self.listPrograms = ['prog/edirect/esearch','~/down_sea/sratoolkit/bin/prefetch','~/down_sea/sratoolkit/bin/fasterq-dump','~/down_sea/sratoolkit/bin/fastq-dump','~/down_sea/datasets','~/down_sea/dataformat']

    def is_program_installed(self, package: str) -> bool:
        try:
            __import__(package)
            return True
        except ImportError:
            return False
            

    def is_program_available(self, program_path):
        return os.path.exists(program_path)
        
    def check(self, install: bool = False):
        for program_name in self.listLibUsed:
            if self.is_program_installed(program_name):
                print(f"'{program_name}' is already installed.")
            else:
                print(f"{program_name} is not installed.")
                if install:
                    subprocess.check_call([sys.executable, "-m", "pip", "install", program_name])

        for program_path in self.listPrograms:
            if self.is_program_available(program_path):
                pass
            else:
                print(f"{program_path} is not installed.")

class downloadPrograms:
    def __init__(self):
        pass

    def downloadEsearch(self):
        os.system("source prog/esearch.sh")

    def downloadSRATools(self):
        os.system("source prog/sratool.sh")
