#!/usr/bin/env python
###############################################################################
 #  Copyright (c) 2015 Jamis Hoo
 #  Project: 
 #  Filename: traverse_urllist.py 
 #  Version: 1.0
 #  Author: Jamis Hoo
 #  E-mail: hoojamis@gmail.com
 #  Date: Aug  4, 2015
 #  Time: 12:28:25
 #  Description: 
###############################################################################

import os

def traverse(config):
    from job_queue import JobQueue
    import urlparse

    mandatory_options = [ 
                          ("MigrateInfo", "migrate.type"),
                          ("UrlList", "url.url_list_file_path"),
                          ("AppInfo", "appinfo.appid"), 
                          ("AppInfo", "appinfo.secretid"),
                          ("AppInfo", "appinfo.secretkey"),
                          ("AppInfo", "appinfo.bucket"),
                          ("ToolConfig", "concurrency")
                        ] 

    for section, option in mandatory_options:
        if section not in config or option not in config[section]:
            print("Error: Option", section + "." + option, "is required. ")
            exit(1)

    if not os.path.isabs(config["UrlList"]["url.url_list_file_path"]):
        print("Error: URL list file path", config["UrlList"]["url.url_list_file_path"], "is not absolute path")
        exit(1)

    urllist_root_path = os.path.abspath(os.path.expanduser(config["UrlList"]["url.url_list_file_path"]))

    job_queue = JobQueue(
                         int(config["ToolConfig"]["concurrency"]),
                         config["AppInfo"]["appinfo.appid"],
                         config["AppInfo"]["appinfo.bucket"],
                         config["AppInfo"]["appinfo.secretid"],
                         config["AppInfo"]["appinfo.secretkey"]
                        )

    # treverse list
    with open(urllist_root_path) as f:
        for url in f:
            fileid = urlparse.urlparse(url).path
            if len(fileid) and fileid[0] == '/':
                fileid = fileid[1: ]
            #print(url)
            #print(fileid)
            job_queue.inqueue(2, url, fileid)

    job_queue.finish()
     
