import os, shutil

from utils.common_util import log_msg

def clean_file_content(file_dir: str):
    try:
        with open(file_dir, 'r+') as file:
            file.truncate(0)
    except Exception as e:
        raise e

def create_dir(dir: str):
    try:
        os.makedirs(dir)
        log_msg(f'Create Directory: {dir}')
    except Exception as e:
        raise e

def clean_dir(dir: str):
    try:
        if os.path.isfile(dir):
            os.unlink(dir)
            log_msg(f'Delete Directory: {dir}')
        else:
            for content_dir in os.listdir(dir):
                full_dir = dir + '/' + content_dir

                if os.path.isfile(full_dir):
                    os.unlink(full_dir)
                elif os.path.isdir(full_dir):
                    shutil.rmtree(full_dir)
                log_msg(f'Delete Directory: {full_dir}')
    except Exception as e:
        raise e