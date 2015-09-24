#!/usr/bin/env python
###############################################################################
 #  Copyright (c) 2015 Tencent Inc.
 #  Distributed under the MIT license 
 #  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 #  
 #  Project: Cloud Image Migration Tool
 #  Filename: master.py
 #  Version: 2.0
 #  Author: Jamis Hoo
 #  E-mail: hoojamis@gmail.com
 #  Date: Sep  7, 2015
 #  Time: 14:29:44
###############################################################################

from __future__ import print_function
import os
import signal
import re
import math
import time
import multiprocessing
import sqlite3
from collections import deque

class Master(object):
    """
    This module is only used in uploading procedure.
    This is the master process who controls uploading procedure.
    This process creates slave processes, loads jobs from database, distributes
    jobs to slaves, gathers logs from slaves and write logs to database.

    Attributes:
        mandatory_options: Configuration options required by this class. This is
            a list of tuples each of which contains two strings, section name and 
            property name, both of which are case-insensitive.

        config: Copy of configuration

        SlaveClass: Slave class type
 
        UploaderClass: Uploader class type

        db_connect: Connecter to sqlite3 database

        db_cursor: Cursor of db_connect
        
        ignore_error_if: Compiled regex expression if Advanced.error.ignore.if is
            provided in config, none otherwise.

        no_more_jobs: Boolean value, indicates whether there're undone jobs in database.

        db_commit_interval: Interval of committing logs to database. Uncommited 
            log will be lost if program is killed.

        db_last_commit: POSIX time the last commit to database

        slaves: All slave process objects.

        log_queue: Queue used to gather logs from slaves to master. Slave processes 
            put logs into this queue and master process fetches logs from this queue.

        job_queue: Queue used to distribute jobs to slaves. Master process puts
            jobs into this queue and slaves fetch jobs from it.

        job_queue_size: Current total number of elements in job queue and log queue.

        job_queue_max_size: Capacity of job_queue and log_queue. Number of elements
            in job queue, number of elements in log queue and number of elements 
            int both cannot exceed this value. This is usually used to prevent 
            backlog in log queue.

        job_queue_reload_size: Fill job_queue from job_queue_buffer when number 
            of elements in job_queue is less than this value.

        job_queue_buffer: Retrieve a centain amount of jobs from database to this
            buffer every time making a SQL SELECT query. This is used to avoid 
            frequent SQL queries.

        job_queue_buffer_max_size: Maximum size of job_queue_buffer. This is used
            to control memory usage.
    """

    mandatory_options = [
        ("paths", "job_db_path"),
        ("toolconfig", "concurrency"),
        ("toolconfig", "jobqueue.capacity"),
        ("toolconfig", "jobqueue.reload.threshold"),
        ("toolconfig", "buffer.size"),
        ("toolconfig", "db.commit.interval"),
                        ]
    
    def __init__(self, config, SlaveClass, UploaderClass):
        """
        Initialize some attributes. 
        """
        self.config = config
        self.SlaveClass = SlaveClass
        self.UploaderClass = UploaderClass

        self.db_connect = None
        self.db_cursor = None

        if "advanced" in config and "error.ignore.if" in config["advanced"]:
            self.ignore_error_if = re.compile(config["advanced"]["error.ignore.if"])
        else:
            self.ignore_error_if = None

    # There's a bug on OS X when using urllib with multiprocessing and sqlite3
    # So this function cannot be called before create_slaves
    # references:
    # http://bugs.python.org/issue9405
    # http://bugs.python.org/issue13829
    def init_db(self):
        """
        Initialize database. 
        """
        db_path = self.config["paths"]["job_db_path"]

        self.db_connect = sqlite3.connect(db_path)
        self.db_connect.text_factory = str
        self.db_cursor = self.db_connect.cursor()

        # if all jobs have already been traversed, set the cursor to start
        # so as to retry all failed jobs
        # else set cursor to the first job not attempted yet
        self.db_cursor.execute(
        """UPDATE metadata SET value = 
            (SELECT 
                COALESCE(
                    (SELECT serial FROM jobs WHERE status = 0 ORDER BY serial LIMIT 1), 
                    1) 
                - 1)
            WHERE key = 'last_selected'
        """
        )
        self.db_connect.commit()

        self.no_more_jobs = False
        self.db_commit_interval = float(self.config["toolconfig"]["db.commit.interval"])
        self.db_last_commit = 0

    def __del__(self):
        """
        Commit transaction and close database.
        """
        if self.db_connect:
            self.db_connect.commit()
            self.db_connect.close()

    @staticmethod
    def check_config(config):
        """
        Check whether all required options are provided. 
        Also check the validity of some options.

        Args:
            config: configuration dict

        Returns:
            Returns string containing error message if there are some errors.
            Returns none otherwise.
        """
        for section, option in Master.mandatory_options:
            if section not in config or option not in config[section]:
                return "Error: Option %s.%s is required. " % (section, option)

        if not config["toolconfig"]["concurrency"].isdigit() or int(config["toolconfig"]["concurrency"]) <= 0:
            return "Error: Minumums of ToolConfig.concurrency is 1. "

        if not config["toolconfig"]["jobqueue.capacity"].isdigit() or int(config["toolconfig"]["jobqueue.capacity"]) <= 0:
            return "Error: Minimums of ToolConfig.jobqueue.capacity is 1. "

        try:
            reload_th = float(config["toolconfig"]["jobqueue.reload.threshold"])
            if reload_th <= 0 or reload_th > 1:
                return "Error: ToolConfig.jobqueue.reload.threshold should within range (0, 1]. "
        except ValueError:
            return "Error: ToolConfig.jobqueue.reload.threshold should within range (0, 1]. "

        try:
            db_commit_interval = float(config["toolconfig"]["db.commit.interval"])
            if db_commit_interval <= 0 or math.isnan(db_commit_interval):
                return "Error: Invalid value for ToolConfig.db.commit.interval. "
        except ValueError:
            return "Error: Invalid value for ToolConfig.db.commit.interval. "
        
        if not config["toolconfig"]["buffer.size"].isdigit() or int(config["toolconfig"]["buffer.size"]) <= 0:
            return "Error: Minimums of ToolConfig.buffer.sizeis 1. "

        if "advanced" in config and "error.ignore.if" in config["advanced"]:
            try:
                re.compile(config["advanced"]["error.ignore.if"])
            except re.error:
                return "Error: Regex syntax error for Advanced.error.ignore.if. "

    def create_slaves(self):
        """
        Create slave processes.
        """
        num_slaves = int(self.config["toolconfig"]["concurrency"])
        self.slaves = []
        self.job_queue = multiprocessing.Queue()
        self.log_queue = multiprocessing.Queue()


        for _ in range(num_slaves):
            slave_class = self.SlaveClass(self.config, self.UploaderClass)
            slave = multiprocessing.Process(target = slave_class.start, args = (self.job_queue, self.log_queue))
            slave.daemon = True
            slave.start()

            self.slaves.append(slave)

    def write_log(self, (serial, fileid, old_status, status, log)):
        """
        Write log of finished job to database.
        Update metadata.
        Commit transaction if needed.
        
        Args:
            log: a tuple of (index, fileid, old_status, new_status, log)
                index: integer, serial number of this job
                fileid: string, new fileid of this job
                old_status: integer, old status of this job
                new_status: integer, new status of the job
                log: log in string

        """
        try:
            self.db_cursor.execute(
                "UPDATE jobs SET status = ?, fileid = ?, log = ? WHERE serial = ?", (status, fileid, log, serial)
            )
        except sqlite3.IntegrityError:
            self.db_cursor.execute(
                "UPDATE jobs SET status = ?, fileid = ?, log = ? WHERE serial = ?", (status, None, log, serial)
            )

        if status == 1 and old_status == 0:
            update_values = [ (1, "successful") ]
        elif status == 1 and old_status == 2:
            update_values = [ (1, "successful"), (-1, "failed") ]
        elif status == 2 and old_status == 0:
            update_values = [ (1, "failed") ]
        elif status == 2 and old_status == 2:
            update_values = [ ]
        else:
            print("Error: should not happen")

        self.db_cursor.executemany(
            "UPDATE metadata SET value = value + ? WHERE key = ?", update_values
        )

        if self.db_last_commit + self.db_commit_interval < time.time():
            try:
                self.db_connect.commit()
                self.db_last_commit = time.time()
            except sqlite3.Error:
                pass 

    def load_job(self):
        """
        Load jobs from database to job_queue_buffer.
        """

        if self.job_queue_buffer: return
        if self.no_more_jobs: return

        self.db_cursor.execute(
            """SELECT serial, fileid, status, src, log
                FROM jobs 
                WHERE status <> 1 AND 
                      serial > (SELECT value FROM metadata WHERE key = 'last_selected') 
                ORDER BY serial 
                LIMIT ?""", (self.job_queue_buffer_max_size,)
        )
        
        last_selected = None
        for job in self.db_cursor.fetchall():
            job_serial = job[0]
            job_log = job[4]
            job_info = job[: -1]

            last_selected = job_serial

            if self.ignore_error_if and job_log and self.ignore_error_if.match(job_log):
                continue

            self.job_queue_buffer.append(job_info)

        if last_selected is not None:
            self.db_cursor.execute(
                "UPDATE metadata SET value = ? where key = 'last_selected'", (last_selected,)
            )
        else:
            self.job_queue_buffer.extend([ "no more jobs" ] * len(self.slaves))
            self.no_more_jobs = True


    def fill_job_queue(self):
        """
        Fill job_queue from job_queue_buffer.
        Fill buffer if buffer is clear.
        """
        for _ in range(self.job_queue_max_size - self.job_queue_size):
            while not self.no_more_jobs and not self.job_queue_buffer:
                self.load_job()

            if not self.job_queue_buffer:
                break

            self.job_queue.put(self.job_queue_buffer.popleft())
            self.job_queue_size += 1

    def start(self):
        """ 
        Start uploading procedure.
        Create slaves, distribute jobs and gather logs.
        """
        if not os.path.isfile(self.config["paths"]["job_db_path"]):
            print("Job database not found, nothing to do. ")
            return

        def interrupt_subprocesses(signum, frame):
            for process in self.slaves:
                os.kill(process.pid, signal.SIGINT)

        signal.signal(signal.SIGINT, interrupt_subprocesses)

        self.create_slaves()

        self.init_db()

        self.job_queue_size = 0
        self.job_queue_max_size = int(self.config["toolconfig"]["jobqueue.capacity"])
        self.job_queue_buffer = deque()
        self.job_queue_buffer_max_size = int(self.config["toolconfig"]["buffer.size"])
        self.job_queue_reload_size = self.job_queue_max_size * float(self.config["toolconfig"]["jobqueue.reload.threshold"])

        num_quit = 0
        
        while True:
            # fill job queue
            if self.job_queue_size < self.job_queue_reload_size:
                self.fill_job_queue() 
            # fetch a log
            log = self.log_queue.get()
            # handle log
            if log == "quit":
                num_quit += 1
                if num_quit == len(self.slaves):
                    break
            else:
                self.write_log(log)
            self.job_queue_size -= 1

        self.db_connect.commit()

        # If slaves is terminated, there are still jobs in job queue which should
        # be discarded.
        self.job_queue.cancel_join_thread()

        for slave in self.slaves:
            slave.join()
        
