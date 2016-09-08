#!/usr/bin/python

import sys, getopt, os, fnmatch, glob, time, operator

def get_oldest_file(files, _invert=False):
    """ Find and return the oldest file of input file names.
    Only one wins tie. Values based on time distance from present.
    Use of `_invert` inverts logic to make this a youngest routine,
    to be used more clearly via `get_youngest_file`.
    """
    gt = operator.lt if _invert else operator.gt
    # Check for empty list.
    if not files:
        return "File doesn't exist"
    # Raw epoch distance.
    now = time.time()
    # Select first as arbitrary sentinel file, storing name and age.
    oldest = files[0], now - os.path.getctime(files[0])
    # Iterate over all remaining files.
    for f in files[1:]:
        age = now - os.path.getctime(f)
        if gt(age, oldest[1]):
            # Set new oldest.
            oldest = f, age
    # Return just the name of oldest file.
    return oldest[0]

def get_youngest_file(files):
    return get_oldest_file(files, _invert=True)

def find(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result

def main(argv):
   inputfile = ' '
   try:
      opts, args = getopt.getopt(argv,"hi:o:",["ifile="])
   except getopt.GetoptError:
      print 'test.py -i <search_string>'
      sys.exit(2)
   for opt, arg in opts:
      if opts == '-h':
         print 'test.py -i <search_string>'
         sys.exit()
      elif opt in ("-i", "--ifile"):
         if (arg == "cmf"):
             print "please check /etc/cloudera-scm-agent/conf for cmf.kaytab file"
             sys.exit()
         inputfile = arg
   print 'Keytab file to be searched for:-', inputfile
   op = find( inputfile+'*.keytab','/var/run/cloudera-scm-agent/process/')
   try:
       print get_youngest_file(op)
   except Exception:
       print "file doesn't exist"
       raise Exception("1")

if __name__ == "__main__":
   main(sys.argv[1:])
