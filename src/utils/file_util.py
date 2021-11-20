import os, shutil
import zipfile

def clean_txt_file_content(file_dir: str):
    try:
        with open(file_dir, 'r+') as file:
            file.truncate(0)
    except Exception as e:
        raise e

def create_dir(dir: str):
    try:
        os.makedirs(dir)
    except Exception as e:
        raise e

def remove_dir(dir: str):
    try:
        if os.path.isfile(dir):
            os.unlink(dir)
        else:
            shutil.rmtree(dir)
    except Exception as e:
        raise e

def clean_dir(dir: str):
    for content_dir in os.listdir(dir):
        full_dir = dir + '/' + content_dir

        if os.path.isfile(full_dir):
            os.unlink(full_dir)
        elif os.path.isdir(full_dir):
            shutil.rmtree(full_dir)

def extract_zip(src_dir: str, dist_dir: str):
    try:
         with zipfile.ZipFile(src_dir) as archive:
            archive.extractall(dist_dir)
    except Exception as e:
        raise e
