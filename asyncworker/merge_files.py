# Merge Files
import os
import sys
import shutil
import heapq


class Main(object):
    def __init__(self, files):
        self.files = files
        self.path = os.getcwd() + "/tmp"
        if os.path.isdir(self.path):
            shutil.rmtree(self.path, ignore_errors=False, onerror=None)
        os.mkdir(self.path)
        self.output_chunks = []
        self.buffer_file = self.path + "/temp.txt"
        self.final_file = self.path + "/final.txt"
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
        # TODO Optimize
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

def check_main():
    # pick up files
    files_list = [os.getcwd() + "/" + file_name for file_name in ["test_file1.txt", "test_file2.txt", "test_file3.txt"]]
    m = Main(files_list)
    m.merge_into_one()
    print(m.final_file_list)
check_main()