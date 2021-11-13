#!/bin/bash
docker build --tag bigdata_project .
docker run -p 8888:8888 -i -t bigdata_project /bin/bash