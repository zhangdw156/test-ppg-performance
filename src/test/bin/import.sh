#!/bin/bash

nohup bash import_all_data.sh >import.log 2>&1 &
disown