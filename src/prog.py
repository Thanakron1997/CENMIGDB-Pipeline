import subprocess
import os
import sys

class checkprograms:
    def __init__(self):
        self.listLibUsed = ['stringMLST.py','krocus']

        self.listPrograms = ['prog/edirect/esearch','prog/sratoolkit/bin/prefetch','prog/sratoolkit/bin/fasterq-dump','prog/sratoolkit/bin/fastq-dump',
                             'prog/datasets','prog/dataformat']

    def is_program_installed(self, package: str) -> bool:
        try:
            subprocess.run(["which", package], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return True  # Run the "which" command to check if the program is in the system's PATH
        except subprocess.CalledProcessError:
            return False  # The "which" command will return a non-zero exit status if the program is not found
                
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
        os.system("prog/esearch.sh")

    def downloadSRATools(self):
        os.system("prog/sratool.sh")
