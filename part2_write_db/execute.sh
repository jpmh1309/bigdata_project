#! /bin/bash
clear
spark-submit \
  --driver-class-path postgresql-42.2.14.jar \
  --jars postgresql-42.2.14.jar \
  write_to_db.py
