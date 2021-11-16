from asyncworker.celery import app
from boto3_connector import Boto3ConnectorLogs

import os
import sys
import shutil
import heapq
import random


class Main(object):
    def __init__(self, files, path, final_file_name):
        self.files = files
        self.path = path + str(random.randint(0,2000))
        if os.path.isdir(self.path):
            shutil.rmtree(self.path, ignore_errors=False, onerror=None)
        os.mkdir(self.path)
        self.output_chunks = []
        self.final_file = self.path + "/{}.txt".format(final_file_name)
        self.chunks = {}
        self.final_file_list = []
        self.chunks_size = 64
        self.open_chunks_size = 128

        count = 0
        for file in self.files:
            self.create_chunks(file, count)
            count += 1

    def create_chunks(self, file, random_value):
        self.chunks[file] = []
        count = 0
        with open(file, "rb") as file_object:
            while True:
                data = file_object.read(self.open_chunks_size)
                if not data:
                    break
                temp_file = self.path + "/temp{0}_{1}.txt".format(random_value,count)
                with open(temp_file, "w+") as temp_file_object:
                    temp_file_object.write(data)
                    self.chunks[file].append(temp_file)
                count += 1

    def sort_pair_of_files(self, file1, file2):
        """
        This method sort the 2 files
        :return:
        """
        sorted_file_1 = list(open(file1).read().strip().split('\n'))
        sorted_file_2 = list(open(file2).read().strip().split('\n'))
        full_heap = sorted_file_1 + sorted_file_2
        heapq.heapify(full_heap)
        temp_list = []
        while sys.getsizeof(temp_list) <= self.chunks_size + 64:  # size of []=64
            if len(full_heap) > 0:
                item = heapq.heappop(full_heap)
            else:
                break
            temp_list.append(item)
        heapq.heappush(full_heap, temp_list.pop(-1))
        temp_list_1 = []
        while len(full_heap) > 0:
            temp_list_1.append(heapq.heappop(full_heap))
        with open(file1, 'w+') as obj:
            obj.writelines('\n'.join(temp_list) + '\n')
        with open(file2, 'w+') as obj:
            obj.writelines('\n'.join(temp_list_1) + '\n')
        return

    def merge_into_one(self):
        """
        This method merges the files.
        :return:
        """
        for file, chunks in self.chunks.items():
            for chunk in chunks:
                if len(self.final_file_list)>0:
                    for sorted_file in self.final_file_list:
                        self.sort_pair_of_files(sorted_file, chunk)
                self.final_file_list.append(chunk)

        with open(self.final_file, 'w+') as obj:
            for file in self.final_file_list:
                obj.write(open(file).read())


class S3Tasks(object):
    def __init__(self, start, end, path):
        self.boto_conn = Boto3ConnectorLogs()
        self.start = start
        self.end = end
        self.pattern = None
        self.set_pattern()
        self.files_to_be_merged = []
        self.path = path
        self.final_file_name = self.start + self.end

    def type(self, order):
        return int(''.join(order.split("-")[::-1]))

    def set_pattern(self):
        self.pattern = [self.type(self.start), self.type(self.end)]

    def is_match(self, name):
        status = False
        if "-" in name:
            pattern = name.split("-")
            if len(pattern)<3:
                return status
            if self.type(pattern) in range(self.pattern[0], self.pattern[1]+1):
                status = True
        return status

    def prepare_files(self):
        files = self.boto_conn.get_all_files()
        for file in files:
            if self.is_match(file):
                self.files_to_be_merged.append(file)

    def download_files(self):
        for file in self.files_to_be_merged:
            self.boto_conn.download(file, self.path)
        return

    def merge_files(self):
        main_obj = Main(self.files_to_be_merged, self.path, self.final_file_name)
        main_obj.merge_into_one()
        return

    def upload_sorted_file(self):
        self.merge_files()
        self.boto_conn.upload(self.final_file_name, self.path)
        return self.final_file_name


def temp_folder(cleanup=True):
    path = os.getcwd() + "/tmp"
    if os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=False, onerror=None)
    if not cleanup:
        os.mkdir(path)
    return path

@app.task
def merge_s3_files(start, end):
    """Async task to download files from S3, merge them and upload the result.

    The lines in the merged file should be ordered.
    """
    try:
        path = temp_folder(cleanup=False)
        s3 = S3Tasks(start, end, path)
        s3.prepare_files()
        s3.download_files()
        file_id = s3.upload_sorted_file()
    except Exception as exp:
        raise exp
    return file_id

@app.task
def download_file(file_name):
    try:
        s3 = Boto3ConnectorLogs()
        s3.download(file_name)
    except Exception as exp:
        raise exp
    return True
