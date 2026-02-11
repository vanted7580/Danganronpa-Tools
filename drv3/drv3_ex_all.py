# -*- coding: utf-8 -*-

################################################################################
# Copyright © 2016-2017 BlackDragonHunt
# This work is free. You can redistribute it and/or modify it under the
# terms of the Do What The Fuck You Want To But It's Not My Fault Public
# License, Version 1, as published by Ben McGinnes. See the COPYING file
# for more details.
################################################################################

import os
import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from util import list_all_files
from spc_ex import spc_ex
from rsct_ex import rsct_ex
from srd_ex import srd_ex
from stx_ex import stx_ex
from wrd_ex import wrd_ex
from logutil import setup_logging, get_logger

OUT_DIR = "dec"
LOG_FILE_NAME = "drv3_ex_all.log"
DEFAULT_WORKERS = min(8, os.cpu_count() or 4)

class DirLockManager(object):
  
  def __init__(self):
    self._locks = {}
    self._guard = threading.Lock()
  
  def get_lock(self, dirname):
    key = os.path.abspath(dirname or ".")
    
    with self._guard:
      lock = self._locks.get(key)
      
      if lock is None:
        lock = threading.Lock()
        self._locks[key] = lock
    
    return lock

class ProgressReporter(object):
  
  def __init__(self, stage_name, total):
    self.stage_name = stage_name
    self.total = total
    self.completed = 0
    self._isatty = sys.stdout.isatty()
    self._active_line = False
    self._last_len = 0
    self._lock = threading.Lock()
    
    self._render()
  
  def _format_message(self):
    if self.total <= 0:
      pct = 100.0
    else:
      pct = (self.completed * 100.0) / self.total
    
    return "[%s] %d/%d (%.1f%%)" % (self.stage_name, self.completed, self.total, pct)
  
  def _render(self):
    msg = self._format_message()
    
    if self._isatty:
      padding = ""
      if self._last_len > len(msg):
        padding = " " * (self._last_len - len(msg))
      
      sys.stdout.write("\r" + msg + padding)
      sys.stdout.flush()
      self._active_line = True
      self._last_len = len(msg)
    
    else:
      print(msg, flush = True)
  
  def advance(self):
    with self._lock:
      self.completed = min(self.total, self.completed + 1)
      self._render()
  
  def newline_if_needed(self):
    with self._lock:
      if self._isatty and self._active_line:
        sys.stdout.write("\n")
        sys.stdout.flush()
        self._active_line = False
        self._last_len = 0
  
  def finish(self):
    self.newline_if_needed()

def make_counters():
  return {"total": 0, "succeeded": 0, "failed": 0}

def merge_counters(total, partial):
  total["total"] += partial["total"]
  total["succeeded"] += partial["succeeded"]
  total["failed"] += partial["failed"]

def run_step(logger, step, filename, fn, *args, progress_reporter = None, **kwargs):
  
  try:
    fn(*args, **kwargs)
    return {"total": 1, "succeeded": 1, "failed": 0}
  
  except Exception:
    if progress_reporter is not None:
      progress_reporter.newline_if_needed()
    logger.exception("%s failed: %s", step, filename)
    return {"total": 1, "succeeded": 0, "failed": 1}

def run_spc_job(logger, lock_manager, filename, out_file, progress_reporter = None):
  logger.info("Extracting SPC: %s", filename)
  
  lock = lock_manager.get_lock(out_file)
  with lock:
    return run_step(logger, "SPC extract", filename, spc_ex, filename, out_file,
                    progress_reporter = progress_reporter)

def run_extract_job(logger, lock_manager, filename, ex_dir, txt_file, crop, progress_reporter = None):
  ext = os.path.splitext(filename)[1].lower()
  counters = make_counters()
  
  lock = lock_manager.get_lock(ex_dir)
  with lock:
    logger.info("Extracting data: %s", filename)
    
    if ext == ".rsct":
      merge_counters(counters, run_step(logger, "RSCT extract", filename, rsct_ex, filename, txt_file,
                                        progress_reporter = progress_reporter))
    
    if ext == ".wrd":
      merge_counters(counters, run_step(logger, "WRD extract", filename, wrd_ex, filename, txt_file,
                                        progress_reporter = progress_reporter))
    
    if ext == ".stx":
      merge_counters(counters, run_step(logger, "STX extract", filename, stx_ex, filename, txt_file,
                                        progress_reporter = progress_reporter))
    
    # Because we have the same extensions used for multiple different formats.
    if ext == ".srd" or ext == ".stx":
      merge_counters(counters, run_step(logger, "SRD extract", filename, srd_ex, filename, ex_dir, crop = crop,
                                        progress_reporter = progress_reporter))
  
  return counters

def run_jobs(parallel_mode, workers, jobs, worker_fn, logger, stage_name):
  counters = make_counters()
  progress = ProgressReporter(stage_name, len(jobs))
  
  if parallel_mode == "thread" and workers > 1:
    with ThreadPoolExecutor(max_workers = workers) as executor:
      futures = [executor.submit(worker_fn, *job, progress_reporter = progress) for job in jobs]
      
      for future in as_completed(futures):
        try:
          result = future.result()
          merge_counters(counters, result)
        
        except Exception:
          progress.newline_if_needed()
          logger.exception("Worker crashed unexpectedly.")
          counters["total"] += 1
          counters["failed"] += 1
        
        finally:
          progress.advance()
  
  else:
    for job in jobs:
      try:
        result = worker_fn(*job, progress_reporter = progress)
        merge_counters(counters, result)
      
      except Exception:
        progress.newline_if_needed()
        logger.exception("Worker crashed unexpectedly.")
        counters["total"] += 1
        counters["failed"] += 1
      
      finally:
        progress.advance()
  
  progress.finish()
  
  return counters

if __name__ == "__main__":
  import argparse
  
  parser = argparse.ArgumentParser(description = "Extracts data used in New Danganronpa V3.")
  parser.add_argument("input", metavar = "<input dir>", nargs = "+", help = "An input directory.")
  parser.add_argument("-o", "--output", metavar = "<output dir>", help = "The output directory.")
  parser.add_argument("--log-file", metavar = "<log file>", help = "Override log file path.")
  parser.add_argument("--verbose", action = "store_true", help = "Show INFO logs in the console.")
  parser.add_argument("-p", "--parallel", choices = ["none", "thread"], default = "thread", help = "Parallel execution mode.")
  parser.add_argument("-w", "--workers", type = int, default = DEFAULT_WORKERS, help = "Worker threads for parallel mode.")
  parser.add_argument("--no-crop", dest = "crop", action = "store_false", help = "Don't crop srd textures to their display dimensions.")
  args = parser.parse_args()
  
  if args.workers < 1:
    parser.error("--workers must be at least 1.")
  
  console_level = logging.INFO if args.verbose else logging.ERROR
  grand_total = make_counters()
  
  for in_path in args.input:
    
    if os.path.isdir(in_path):
      base_dir = os.path.normpath(in_path)
      files = list(list_all_files(base_dir) or [])
    else:
      continue
    
    if args.output:
      out_dir = os.path.normpath(args.output)
      out_dir = os.path.join(out_dir, os.path.basename(base_dir))
    else:
      split = os.path.split(base_dir)
      out_dir = os.path.join(split[0], OUT_DIR, split[1])
    
    log_file = args.log_file or os.path.join(out_dir, LOG_FILE_NAME)
    logger = setup_logging(log_file, console_level = console_level, file_level = logging.ERROR)
    counters = make_counters()
    lock_manager = DirLockManager()
    
    logger.info("*****************************************************************")
    logger.info("* New Danganronpa V3 extractor, written by BlackDragonHunt.      ")
    logger.info("*****************************************************************")
    logger.info("Input directory: %s", base_dir)
    logger.info("Output directory: %s", out_dir)
    logger.info("Log file: %s", log_file)
    logger.info("Parallel mode: %s (%d workers)", args.parallel, args.workers)
  
    if out_dir == base_dir:
      logger.error("Input and output directories are the same: %s", out_dir)
      logger.error("Continuing will cause the original data to be overwritten.")
      s = input("Continue? y/n: ")
      if not s[:1].lower() == "y":
        continue
    
    # Extract the SPC files.
    spc_jobs = []
    
    for filename in files:
      out_file = os.path.join(out_dir, filename[len(base_dir) + 1:])
      
      if not os.path.splitext(filename)[1].lower() == ".spc":
        continue
      
      spc_jobs.append((logger, lock_manager, filename, out_file))
    
    merge_counters(counters, run_jobs(args.parallel, args.workers, spc_jobs, run_spc_job, logger, "SPC"))
    
    # Now extract all the data we know how to from inside the SPC files.
    ex_jobs = []
    
    for filename in list(list_all_files(out_dir) or []):
      ext = os.path.splitext(filename)[1].lower()
      
      if not ext in [".rsct", ".wrd", ".stx", ".srd"]:
        continue
      
      ex_dir, basename = os.path.split(filename)
      ex_dir   = out_dir + "-ex" + ex_dir[len(out_dir):]
      txt_file = os.path.join(ex_dir, os.path.splitext(basename)[0] + ".txt")
      
      ex_jobs.append((logger, lock_manager, filename, ex_dir, txt_file, args.crop))
    
    merge_counters(counters, run_jobs(args.parallel, args.workers, ex_jobs, run_extract_job, logger, "Extract"))
    
    summary_msg = "Summary for %s | total=%d success=%d failed=%d"
    summary_args = (base_dir, counters["total"], counters["succeeded"], counters["failed"])
    if counters["failed"] > 0:
      logger.error(summary_msg, *summary_args)
    else:
      logger.info(summary_msg, *summary_args)
    
    merge_counters(grand_total, counters)
  
  final_logger = get_logger(__name__)
  if grand_total["failed"] > 0:
    final_logger.error("Overall summary | total=%d success=%d failed=%d",
                      grand_total["total"], grand_total["succeeded"], grand_total["failed"])
  else:
    final_logger.info("Overall summary | total=%d success=%d failed=%d",
                      grand_total["total"], grand_total["succeeded"], grand_total["failed"])
  input("Press Enter to exit.")

### EOF ###
