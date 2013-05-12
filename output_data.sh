

function runMyFunc {
	M=$1
	dN=$2
	num=$3
	dir="/user/abeutel/DSGDout/docterm-$num/run5/iter9/"
	rank=50
	d=10 
	output="/home/abeutel/DSGD/docterm-$num-$M/"

	rm -rf $output
	mkdir $output

	#for i in {0..$d}
	for (( i=0; i < $d; i++ ))
	do
		#echo "$dir$M/$M$i.0"
		echo "/opt/hadoop/bin/hadoop fs -cat ${dir}${M}/${M}${i}.0 | java SaveColumns $rank $dN $i $output"
		/opt/hadoop/bin/hadoop fs -cat ${dir}${M}/${M}${i}.0 | java SaveColumns $rank $dN $i $output

	done
	#/opt/hadoop/bin/hadoop fs -cat /user/abeutel/DSGDout/docterm-2/run5/iter9/U/U5.1 | java SaveColumns 50 49421 0 /home/abeutel/DSGD/docterm-2-U/
	/opt/hadoop/bin/hadoop fs -cat /user/abeutel/DSGDout/docterm-2/run5/iter9/V/V4.1 | java SaveColumns 50 49421 0 /home/abeutel/DSGD/docterm-2-V/
	#/opt/hadoop/bin/hadoop fs -cat /user/abeutel/DSGDout/docterm-1/run10/iter9/V/V1.1 | java SaveColumns 50 49421 0 /home/abeutel/DSGD/docterm-1-V/
	#/opt/hadoop/bin/hadoop fs -cat /user/abeutel/DSGDout/docterm-1/run10/iter9/V/V9.1 | java SaveColumns 50 49421 0 /home/abeutel/DSGD/docterm-1-V/

	output2="/home/abeutel/DSGD/docterm-$num-$M-Sorted/"

	rm -rf $output2
	mkdir $output2

	for (( i=0; i < $rank; i++ ))
	do

		#echo "Sort col $i"
		#echo "sort -k2gr -t $'\t' $output/col-$i.txt  > $output2/col-$i.txt"
		#sort -k2gr -t $'\t' $output/col-$i.txt  > $output2/col-$i.txt
		mv $output/col-$i.txt  $output2/col-$i.txt

	done

	#rm -rf $output
}

num=2
#runMyFunc 'U' 98841 $num 
#runMyFunc 'V' 476735 $num 
#runMyFunc 'W' 468825 $num 

#runMyFunc 'U' 49421 $num 
#runMyFunc 'V' 238368 $num 
#runMyFunc 'W' 234413 $num 

#runMyFunc 'U' 49421 $num 
runMyFunc 'V' 1000000 $num 


#rm -rf /home/abeutel/DSGD/docterm-$num-U-Sorted-Words/
#rm -rf /home/abeutel/DSGD/docterm-$num-V-Sorted-Words/
#rm -rf /home/abeutel/DSGD/Tensor-L1-$num-W-Sorted-Words/

#mkdir /home/abeutel/DSGD/docterm-$num-U-Sorted-Words/
#mkdir /home/abeutel/DSGD/docterm-$num-V-Sorted-Words/
#mkdir /home/abeutel/DSGD/Tensor-L1-$num-W-Sorted-Words/

#python add_words.py

python normalize.py


#runMyFunc 'V' 238368 $num 
#runMyFunc 'W' 234413 $num 

#python add_words.py
#python normalize.py


##dN=49421
##dN=238368
#dN=234413

