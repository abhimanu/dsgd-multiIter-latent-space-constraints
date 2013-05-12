hadoop fs -rmr /user/abeutel/DSGDout/*
hadoop fs -mkdir /user/abeutel/DSGDout/paired
hadoop fs -mkdir /user/abeutel/DSGDout/matrix
hadoop fs -mkdir /user/abeutel/DSGDout/tensor

#hadoop fs -rm DSGDtest-tensor/*.txt
#hadoop fs -rm DSGDtest-matrix/*.txt
#hadoop fs -put ../test-tensor.txt DSGDtest-tensor
#hadoop fs -put ../test-matrix.txt DSGDtest-matrix



#params=" -D dsgd.N=100 -D dsgd.M0=100 -D dsgd.P0=100 -D dsgd.M1=100 -D dsgd.rank=1 -D mapred.reduce.tasks=1 -D dsgd.stepSize=0.00001 1 3 2 pairedTensor /user/epapalex/DSGDtest-tensor /user/epapalex/DSGDtest-matrix "
params=" -D dsgd.N=100 -D dsgd.M0=100 -D dsgd.P0=100 -D dsgd.M1=100 -D dsgd.rank=1 -D mapred.reduce.tasks=3 3 3 2 pairedTensor /user/epapalex/DSGDtest-tensor /user/epapalex/DSGDtest-matrix "
output_dir="/user/abeutel/DSGDout/paired"
last_iter=8


echo "Iteration 0"
step="0.01"
#time hadoop jar DSGD.jar DSGD $params ${output_dir}/run0
time hadoop jar DSGD.jar DSGD -D dsgd.stepSize=$step $params ${output_dir}/run0

last=0
for i in {1..50}
do
	step=$(echo "scale=5; 0.1 / ($i + 1)" | bc -l)
	step=$(echo 0.0001 $step | awk '{if ($1 < $2) print $2; else print $1}')

	echo "Iteration ${i}"
	#time hadoop jar DSGD.jar DSGD $params ${output_dir}/run$i ${output_dir}/run${last}/iter${last_iter}
	time hadoop jar DSGD.jar DSGD -D dsgd.stepSize=$step $params ${output_dir}/run$i ${output_dir}/run${last}/iter${last_iter}

	echo "Loss Iteration ${i}"
	time hadoop jar Frobenius.jar Frobenius $params ${output_dir}-loss/run$i ${output_dir}/run${last}/iter${last_iter}

	last=$i
done


#params=" -D dsgd.N=100 -D dsgd.M0=100 -D dsgd.rank=1 -D mapred.reduce.tasks=5 -D dsgd.stepSize=0.00001 5 2 1 justMatrix  /user/epapalex/DSGDtest-matrix "
#output_dir="/user/epapalex/DSGDout/matrix"
#last_iter=4


#echo "Iteration 0"
#time hadoop jar DSGD.jar DSGD $params ${output_dir}/run0

#last=0
#for i in {1..50}
#do
        #echo "Iteration ${i}"
        #time hadoop jar DSGD.jar DSGD $params ${output_dir}/run$i ${output_dir}/run${last}/iter${last_iter}
        #last=$i
#done


##params=" -D dsgd.N=100 -D dsgd.M0=100 -D dsgd.P0=100 -D dsgd.rank=1 -D mapred.reduce.tasks=3 -D dsgd.stepSize=0.000001 3 3 1 justTensor /user/epapalex/DSGDtest-tensor "
#params=" -D dsgd.debug=1 -D dsgd.N=100 -D dsgd.M0=100 -D dsgd.P0=100 -D dsgd.rank=1 -D mapred.reduce.tasks=3 3 3 1 justTensor /user/epapalex/DSGDtest-tensor "
#output_dir="/user/abeutel/DSGDout/tensor"
#last_iter=8


#echo "Iteration 0"
#step="0.1"
##time hadoop jar DSGD.jar DSGD $params ${output_dir}/run0
#time hadoop jar DSGD.jar DSGD -D dsgd.stepSize=$step $params ${output_dir}/run0

#last=0
#for i in {1..15}
#do
		#step=$(echo "scale=5; 0.1 / ($i + 1)" | bc -l)
		#step=$(echo 0.001 $step | awk '{if ($1 < $2) print $2; else print $1}')

        #echo "Iteration ${i}"
        ##time hadoop jar DSGD.jar DSGD $params ${output_dir}/run$i ${output_dir}/run${last}/iter${last_iter}
		#time hadoop jar DSGD.jar DSGD -D dsgd.stepSize=$step $params ${output_dir}/run$i ${output_dir}/run${last}/iter${last_iter}

		#echo "Loss Iteration ${i}"
		#time hadoop jar Frobenius.jar Frobenius $params ${output_dir}-loss/run$i ${output_dir}/run${last}/iter${last_iter}

        #last=$i
#done

#rm -r ../DSGDout/
#hadoop fs -copyToLocal DSGDout /h/abeutel

