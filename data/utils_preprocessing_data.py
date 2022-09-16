import pyshark
import pandas as pd
import glob
from dask.distributed import Client, LocalCluster
import os
from tqdm import tqdm
from multiprocessing import Process, Manager
import time


def parse_pcap(filename):
    print(f'Parsing: {filename}')
    pcap = pyshark.FileCapture(filename)

    length_arr = []
    src_arr = []
    dst_arr = []
    time_arr = []

    for packet in tqdm(pcap):
        if 'IP' in packet:
            length_arr.append(int(packet.ip.len))
            src_arr.append(str(packet.ip.src))
            dst_arr.append(str(packet.ip.dst))
            time_arr.append(packet.sniff_time)

    df = pd.DataFrame({
        'len': length_arr,
        'src': src_arr,
        'dst': dst_arr,
        'time': time_arr
    })

    print(df)

    df.to_csv(filename+'.csv', index=False)
    return 0


if __name__ == '__main__':

    pcap_files = [file for file in glob.glob("./SplitRead/*.pcap") if not os.path.exists(file+'.csv')]

    print(f'Total Number of Files: {len(glob.glob("./SplitRead/*.pcap"))}')
    print(f'Number of Remaining Files: {len(pcap_files)}')

    # client = Client(processes=False)
    # print(client)
    # l = client.map(parse_pcap, pcap_files[:1])
    #
    # print(client.gather(l))

    # parse_pcap(pcap_files[2])

    manager = Manager()

    start_time = time.time()

    processes = []

    i = 0
    batch_size = 8
    while i * batch_size < len(pcap_files):
        print(f'i: {i}/{round(len(pcap_files) / batch_size)}')
        for file in pcap_files[i * batch_size: (i+1) * batch_size]:
            # Append the new process in the processes list
            p1 = Process(target=parse_pcap, args=(file,))
            processes.append(p1)
            p1.start()

        # Wait until the computation are complete
        for process in processes[i * batch_size: (i+1) * batch_size]:
            process.join()

        i += 1

    print("--- %s seconds ---" % (time.time() - start_time))