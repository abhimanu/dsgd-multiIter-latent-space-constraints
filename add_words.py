#!/usr/bin/env python
import sys
import math


def addWords(rank,datadir,datadir2,lookup1):
	d1 = { } #np.array(np.zeros(N), dtype=int)


	print 'Index Lookup 1'
	with open(lookup1) as f:
		cnt = 1
		for line in f:
			fields = line.split('\t')
			#fields = shlex.split(line)
			d1[cnt] = fields[0]
			#d1[int(fields[len(fields)-1])] = cnt
			cnt = cnt+1


	for i in range(rank):
		f = open(datadir + "col-" + str(i) + ".txt")
		fout = open(datadir2 + "col-" + str(i) + ".txt", 'w')
		for line in f:
			fields = line.split()
			verb = 'NONE FOUND'
			if (int(fields[0]) in d1):
				verb = d1[int(fields[0])]
			fout.write(verb + '\t' + fields[1] + '\n');
		f.close()
		fout.close()


def main():

	rank = 50
	datadir = '/home/abeutel/DSGD/docterm-2-U-Sorted/'
	datadir2 = '/home/abeutel/DSGD/docterm-2-U-Sorted-Words/'
	lookup = 'lookup2.randomized.txt'
	addWords(rank,datadir,datadir2,lookup)

	#datadir = '/home/abeutel/DSGD/docterm-1-V-Sorted/'
	#datadir2 = '/home/abeutel/DSGD/docterm-1-V-Sorted-Words/'
	#lookup = 'lookup1.randomized.txt'
	#addWords(rank,datadir,datadir2,lookup)

	#datadir = '/home/abeutel/DSGD/Tensor-L1-14-W-Sorted/'
	#datadir2 = '/home/abeutel/DSGD/Tensor-L1-14-W-Sorted-Words/'
	#lookup = 'lookup3.randomized.txt'
	#addWords(rank,datadir,datadir2,lookup)


if __name__ == '__main__':
	main()

