#!/usr/bin/env python
import sys
import math


def normalize(rank,datadir,fout):

	eigenvals = []

	for i in range(rank):
		f = open(datadir + "col-" + str(i) + ".txt")
		s = 0
		for line in f:
			fields = line.split('\t')
			s = s + pow(float(fields[1]),2)
		f.close()
		s = pow(s,0.5)
		eigenvals.append((i,s))

	sortedEigen = sorted(eigenvals, key=lambda eigen: eigen[1], reverse=True) 
	print sortedEigen

	return eigenvals


def main():

	rank = 50

	datadir = '/home/abeutel/DSGD/docterm-2-U-Sorted-Words/'
	eigen1 = normalize(rank,datadir,'')

	datadir = '/home/abeutel/DSGD/docterm-2-V-Sorted/'
	eigen2 = normalize(rank,datadir,'')

	#datadir = '/home/abeutel/DSGD/Tensor-L1-14-W-Sorted-Words/'
	#eigen3 = normalize(rank,datadir,'')

	eigenvals = []
	for i in range(rank):
		#print eigen1[i]
		#print eigen2[i]
		#print eigen3[i]
		#eigenvals.append((i,(eigen1[i][1]*eigen2[i][1]*eigen3[i][1])))
		eigenvals.append((i,(eigen1[i][1]*eigen2[i][1])))

	print "Final:"
	sortedEigen = sorted(eigenvals, key=lambda eigen: eigen[1], reverse=True) 
	print sortedEigen


if __name__ == '__main__':
	main()


