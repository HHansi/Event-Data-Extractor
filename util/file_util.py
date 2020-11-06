# Created by Hansi at 10/19/2020
import os


def create_folder_if_not_exist(path, is_file_path=False):
    if is_file_path:
        folder_path = os.path.dirname(os.path.abspath(path))
    else:
        folder_path = path
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)