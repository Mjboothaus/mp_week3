#import pandas as pd
#import numpy as np

from os import system
import os.path

# Download and parse the incoming data

# Start by downloading a chunk of the the dataset from Amazon S3:

test_filename = 'part-00026.xml.bz2'

# The dataset is available (pre-chunked) on S3: s3://dataincubator-course/mrdata/simple/

aws_get_file_cmd = u"aws s3 sync s3://dataincubator-course/mrdata/simple . --exclude '*' --include " + test_filename

if not os.path.exists("./" + test_filename):
    system(aws_get_file_cmd)

