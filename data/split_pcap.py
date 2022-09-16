import os
from multiprocessing import Process, Manager
import glob
from os import system as cmd
import time

file_size = 200000

if __name__ == "__main__":
    
    list_files = glob.glob("./*.pcap")
    
    print("N. Files:", len(list_files))
    original_pcap_file = list_files[0]
    
    sub_dir = "SplitRead/"

    # Try to create folder for the splitted data
    try:
        os.mkdir("./" + sub_dir)
    except OSError:
        print("Creation of the directory %s failed" % sub_dir)
    else:
        print("Successfully created the directory %s" % sub_dir)
    
    cmd(f'editcap -c {file_size} ' + original_pcap_file + " ./" + sub_dir + "__mini.pcap")

    #Change directory
    os.chdir("./"+sub_dir)
    
    splitted_file = sorted(glob.glob("*.pcap"))
    
    print("N. Splitted Files:", len(list_files))

