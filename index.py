#!/usr/bin/env python
# encoding: utf-8

# Index a file

import sys
import os


def index(fn,fn1,fn2,fn3):
	t1 = dict()
	t2 = dict()
	t12 = set()

	t1_cnt = 0
	t2_cnt = 0

	fin = open(fn, 'r')
	fout = open(fn1, 'w')
	fout1 = open(fn2, 'w')
	fout2 = open(fn3, 'w')

	for line in fin:
		vals = line.split()
		if(len(vals) < 3):
			continue

		#s = vals[0] + "\t" + vals[1]
		#if( s in t12):
			#print "ERROR ON: " + s
			#continue
		#else:
			#t12.add(s)

		if(vals[0] not in t1):
			t1[vals[0]] = t1_cnt
			fout1.write(vals[0] + "\t" + str(t1_cnt) + "\n")
			t1_cnt = t1_cnt+1

		if(vals[1] not in t2):
			t2[vals[1]] = t2_cnt
			fout2.write(vals[1] + "\t" + str(t2_cnt) + "\n")
			t2_cnt = t2_cnt+1

		vals[0] = t1[vals[0]]
		vals[1] = t2[vals[1]]

			

		fout.write(str(vals[0]) + "\t" + str(vals[1]) + "\t" + vals[2] + "\n")

	fin.close()
	fout.close()
	fout1.close()
	fout2.close()

	print "N:" + str(t1_cnt)
	print "M:" + str(t2_cnt)


def copyPart(fn,fn2):
	fin = open(fn, 'r')
	fout = open(fn2, 'w')
	for line in fin:
		vals = line.split()
		if(len(vals) >= 12):
			fout.write(vals[11] + "\t" + vals[3] + "\t" + vals[0] + "\n")

	fin.close()
	fout.close()



def main():

	print sys.argv
	if len(sys.argv) > 4:
		fn = sys.argv[1]
		fn1 = sys.argv[2]
		fn2 = sys.argv[3]
		fn3 = sys.argv[4]
	else:
		print "No file specified"
		return

	index(fn,fn1,fn2,fn3)





if __name__ == '__main__':
	main()


