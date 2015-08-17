#!/usr/bin/env python
###############################################################################
 #  Copyright (c) 2015 Jamis Hoo
 #  Project: 
 #  Filename: traverse_dir.py 
 #  Version: 1.0
 #  Author: Jamis Hoo
 #  E-mail: hoojamis@gmail.com
 #  Date: Aug  4, 2015
 #  Time: 12:22:42
 #  Description: traverse over a root directory 
###############################################################################

from __future__ import print_function
import re
import os


def traverse(config, job_queue, skip, (fileid_format_pattern, fileid_format_replace)):
    # check config
    mandatory_options = [ 
                          ("local", "local.image_root_path"), 
                        ] 

    for section, option in mandatory_options:
        if section not in config or option not in config[section]:
            print("Error: Option", section + "." + option, "is required. ")
            exit(1)

    if not os.path.isabs(config["local"]["local.image_root_path"]):
        print("Error: Image root path", config["local"]["local.image_root_path"], "is not absolute path")
        exit(1)
    
    # only filenames matching this regex will be uploaded, others would be ignored
    #filename_pattern = re.compile(".*\.(?:jpg|jpeg|png|gif|bmp|webp)$", re.IGNORECASE)
    # use this to match all filenames
    filename_pattern = None

    
    image_root_path = os.path.abspath(os.path.expanduser(config["local"]["local.image_root_path"]))

    # number of submited = totoal
    # number of skipped = already uploaded last time 
    num_submited = 0
    num_skipped = 0
    # traverse dir and submit job to job queue
    for dirpath, dirs, files in os.walk(image_root_path, followlinks = True):
        for filename in files:
            if filename_pattern and not filename_pattern.match(filename):
                continue
            full_name = os.path.join(dirpath, filename)
            fileid = full_name[len(image_root_path) + 1: ]
            
            if fileid_format_pattern and fileid_format_replace:
                fileid = re.sub(fileid_format_pattern, fileid_format_replace, fileid)

            num_submited += 1
            if fileid not in skip:
                job_queue.inqueue(0, full_name, fileid)
            else:
                num_skipped += 1

    return (num_submited, num_skipped)
    
