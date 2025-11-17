#!/bin/bash

nohup bash import_all_tbl_optimized.sh >import.log 2>&1 &
disown