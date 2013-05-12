#!/usr/bin/env python
# encoding: utf-8

# Generator a fake graph using Kronecker products

import sys
import os

def runProduct(fin,fout,generator,n):
	for line in fin:
		vals = line.split()
		for term in generator:
			for i in range(0,3):
				v = int(term[i]) * int(n[i]) + int(vals[i])
				fout.write(str(v) + "\t")
			fout.write("\n")

	fin.close()
	fout.close()

def makeMinSize(fn,g,nx,ny,nz,mx,my,mz):

	temp = fn.split(".")
	fnt = temp[0]
	na = fnt.split("_")
	print na

	while ( int(na[0]) <= mx or int(na[1]) <= my or int(na[2]) <= mz ):
		fin = open(fn, 'r')

		fn = str(int(na[0])*nx) + "_" + str(int(na[1])*ny) + "_" + str(int(na[2])*nz) + ".txt" 
		fout = open(fn, 'w')

		runProduct(fin,fout,g,na)

		temp = fn.split(".")
		fnt = temp[0]
		na = fnt.split("_")
		print na


def runOnce(fn,g,nx,ny,nz):
	fin = open(fn, 'r')

	temp = fn.split(".")
	fn = temp[0]
	na = fn.split("_")
	print na

	fn_out = str(int(na[0])*nx) + "_" + str(int(na[1])*ny) + "_" + str(int(na[2])*nz) + ".txt"
	fout = open(fn_out, 'w')

	runProduct(fin,fout,g,na)


def main():

	nx = 6
	ny = 5
	nz = 4
	generator = [ ]
	generator.append([0,1,3])
	generator.append([0,4,2])
	generator.append([1,2,0])
	generator.append([1,3,1])
	generator.append([2,0,3])
	generator.append([2,4,0])
	generator.append([3,1,3])
	generator.append([3,2,2])
	generator.append([3,4,1])
	generator.append([4,0,2])
	generator.append([4,3,3])
	generator.append([5,2,0])
	generator.append([5,3,2])


	print sys.argv
	if len(sys.argv) > 1:
		fn = sys.argv[1]
	else:
		print "No file specified"
		return

	#runOnce(fn,generator,nx,ny,nz)
	makeMinSize(fn,generator,nx,ny,nz,10000,10000,5)




if __name__ == '__main__':
	main()


