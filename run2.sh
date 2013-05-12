
#params=" -D dsgd.N=26043115 -D dsgd.M=26043115 -D dsgd.P=48282275 -D dsgd.rank=5 -D mapred.reduce.tasks=20 5 3 NELL /user/abeutel/NELL "
#output_dir="/user/abeutel/nell_test"
#last_iter=24

#params=" -D dsgd.N=14545 -D dsgd.M=14545 -D dsgd.P=28818 -D dsgd.rank=5 -D mapred.reduce.tasks=20 5 3 1 NELL2 /user/abeutel/NELL "
#output_dir="/user/abeutel/nell_test2"
#last_iter=24

#params=" -D dsgd.N=1392873 -D dsgd.M0=4710 -D dsgd.rank=5 -D mapred.reduce.tasks=30 30 2 1 tencent4 /user/abeutel/tencent "
#output_dir="/user/abeutel/tencent_test4"
#last_iter=29

#0.0000000238
#params=" -D dsgd.N=60 -D dsgd.M0=77775 -D dsgd.P0=9 -D dsgd.rank=15 -D mapred.reduce.tasks=30 -D dsgd.stepSize=0.000001 3 3 1 brain4 /user/abeutel/brain "
#output_dir="/user/abeutel/brain_tensor_test4"
#last_iter=8

#params=" -D dsgd.N=50 -D dsgd.M0=50 -D dsgd.P0=50 -D dsgd.rank=10 -D mapred.reduce.tasks=20 -D dsgd.stepSize=0.000008 2 3 1 diag2 /user/abeutel/diag "
#output_dir="/user/abeutel/diag_tensor_test2"
#last_iter=3


#Netflix N = 17692, M = 480189
#params=" -D dsgd.debug=1 -D dsgd.N=17771 -D dsgd.M0=480190 -D dsgd.rank=10 -D mapred.reduce.tasks=30 30 2 1 netflix7 /user/abeutel/netflix "
params=" -D dsgd.sparse=1 -D dsgd.N=17771 -D dsgd.M0=480190 -D dsgd.rank=10 -D mapred.reduce.tasks=20 20 2 1 netflix9 /user/abeutel/netflix "
#params=" -D dsgd.debug=1 -D dsgd.N=17692 -D dsgd.M0=480189 -D dsgd.rank=10 -D dsgd.stepSize=0.0000997 -D mapred.reduce.tasks=30 30 2 1 netflix4 /user/abeutel/netflix "
output_dir="/user/abeutel/netflix_test9"
last_iter=29

echo "Iteration 0"
step="0.001"
time hadoop jar DSGD.jar DSGD -D dsgd.stepSize=$step $params ${output_dir}/run0

last=0
for i in {1..15}
do
	step=$(echo "scale=5; 0.001 / ($i + 1)" | bc -l)

	echo "Iteration ${i}"
	echo "Step ${step}"
	time hadoop jar DSGD.jar DSGD -D dsgd.stepSize=$step $params ${output_dir}/run$i ${output_dir}/run${last}/iter${last_iter}

	echo "Loss Iteration ${i}"
	time hadoop jar Frobenius.jar Frobenius $params ${output_dir}-loss/run$i ${output_dir}/run${last}/iter${last_iter}

	last=$i
done


#last=0
#for i in {0..14}
#do
	#echo "Loss Iteration ${i}"
	#time hadoop jar Frobenius.jar Frobenius $params ${output_dir}-loss/run$i ${output_dir}/run${last}/iter${last_iter}
	#last=$i
#done
