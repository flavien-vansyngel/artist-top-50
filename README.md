# TOP 50

AIM
---
Send to each user the top 50 of their country as well as the top 50 of their own listening on the last 7 days.


PREREQUISITES
-------------
The solution contained in this directory is written in python3 and makes use of the following libraries:
 - pandas version 0.24.2
 - numpy  version 1.15.0
This choice is made for compatibility reason: the numpy and pandas libraries are very common libraries. The Spark engine is less common and used mainly on clusters and would have been a good alternative in this case. In addition, with Spark we can set memory usage very conveniently.

A file containing country ISO codes such as the "isoCodes.csv" in this direcotry must be in the working directory.


USAGE
-----

Donwload all three python files in the same direcotry. The scripts create files in the directories, at least 7 GB of disk space is needed.

To run the solution of this directory you need first to generate a dataset:
   $ python3 createDataSet.py
This will create the files listen-YYYYMMDD.log for 8 days (YYYY=2012 MM=09 DD=01,...,08) in a directory $PWD/dataset.

Then intermediary tops are computed and written to files:
   $ python3 topDaily.py
The files listen-YYYYMMDD.log are read by chunks in order to minimize the use of the RAM. It can be easily tuned to balance between use of memory and speed of execution.
The intermediary tops are written to files because they will be accessed multiple times since the top 50 of a given day takes the last 7 days into account. This strategy does not work for the top 50 of users, see the IMPROVEMENTS section for more details. 
The intermediary files are written in $PWD/dataset/interm and its subdirectories.

Then the top 50 for 2 days (07 and 08) is computed using the intermediary tops:
   $ python3 top50.py
The intermediary tops are merged and combined one after the other to make the global top 50.
The files containing the tops will be written in the working direcotry.


IMPROVEMENTS (TODO list)
------------------------
 - The intermediary files strategy works for the top by coutry but does not work for top by users because there are too many intermediary files to write and they saturate the inodes. For the users, an intermediary top must be recomputed each time it is needed to compute a global top 50, i.e. in the top50.py file, the read_csv at line 48 must replaced by the computation of the chunk top as done in the topDaily.py file.
 - Run all three parts from the same script and centralise the hard coded variables such as the number of users and the path to intermediary files
 - Documentation
