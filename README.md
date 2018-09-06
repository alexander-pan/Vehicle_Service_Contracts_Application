# To Update
- Scripts to automate data at any specificed scheduled time
- The data be compatible with app in case anything changed (current issue)
- crontab on server/machine to schedule scripts to run (need to talk to Joe)

# Sunpath
This document contains notes for the operation and technical details of
the dashboard and application.

For datasets,
  - Data is stored in /static/data
  - Datasets will be updated periodically

Caching
  - The dashboard will use a "filesystem" caching and flash_caching module
  - this will insure that expensive computations in the future will
  take less time. Future sessions will use the pre-computed values

On App startup
  - All callbacks are run
  - Initially the Inputs: Funder, Seller, Date Ranges, and others will be null on startup

General
  - callbacks are run when an input is modified
  - if an input is modified all values associated
  with the dashboard will also be modified reactively

How to Run:
-primary app to run is "index.py"
-python index.py to get the entire app to start running

Requirements
-requirements.txt includes all the modules and dependencies required for the
-flask app to run

