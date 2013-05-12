#hadoop fs -mkdir /user/abeutel/DSGDout/nell-svo

#hadoop fs -rm DSGDtest-tensor/*.txt
#hadoop fs -rm DSGDtest-matrix/*.txt
#hadoop fs -put ../test-tensor.txt DSGDtest-tensor
#hadoop fs -put ../test-matrix.txt DSGDtest-matrix


#params=" -D dsgd.N=183031107 -D dsgd.M0=19210545 -D dsgd.P0=180072636 -D dsgd.rank=20 -D mapred.reduce.tasks=20 -D dsgd.stepSize=0.000001 20 3 1 justTensor /user/epapalex/nell-big/tensor "
#output_dir="/user/epapalex/nell-svo-out"
#last_iter=399


#echo "Iteration 0"
#time hadoop jar DSGD.jar DSGD $params ${output_dir}/run0

#last=0
#for i in {1..10}
#do
        #echo "Iteration ${i}"
        #time hadoop jar DSGD.jar DSGD $params ${output_dir}/run$i ${output_dir}/run${last}/iter${last_iter}
        #last=$i
#done


#params=" -D dsgd.N=100 -D dsgd.M0=100 -D dsgd.P0=100 -D dsgd.M1=100 -D dsgd.rank=1 -D mapred.reduce.tasks=1 -D dsgd.stepSize=0.00001 1 3 2 pairedTensor /user/epapalex/DSGDtest-tensor /user/epapalex/DSGDtest-matrix "

#N=183,031,107 -D dsgd.M0=19,210,545 -D dsgd.P0=180,072,636

#key="nellSVOFreq20L1-6"
#lambda=20
#rank=100
#mean=0.26
#sparse=1
#rank=100
#d=10
#initialStep=0.0001
#min=0.0000000015


key="nellSVOFreq20L1-9"
lambda=10
mean=0.33
sparse=1
nnmf=1
rank=100
d=10
initialStep=0.0005
min=0.00001

echo -e "Key: $key\nLambda: $lambda\nMean: $mean\nSparse: $sparse\nNNMF: $nnmf\nRank: $rank\nd: $d\ninitialStep: $initialStep\nminStep: $min\n" > ~/log-$key.txt

hadoop fs -rmr /user/abeutel/DSGDout/$key/*

params=" -D dsgd.regularizerLamda=$lambda -D dsgd.initMean=$mean -D dsgd.nnmf=$nnmf -D dsgd.sparse=$sparse -D mapred.child.java.opts=-Xmx4096m -D dsgd.N=494201 -D dsgd.M0=2383673 -D dsgd.P0=2344121 -D dsgd.rank=$rank -D mapred.reduce.tasks=$d $d 3 1 $key /user/abeutel/nell/freq20-vso-log"
output_dir="/user/abeutel/DSGDout/$key"
last_iter=99
#min=0.0000000015

echo "Iteration 0"
step=$initialStep
#time hadoop jar DSGD.jar DSGD $params ${output_dir}/run0
time hadoop jar DSGD.jar DSGD -D dsgd.stepSize=$step $params ${output_dir}/run0

last=0
for i in {1..10}
do
	step=$(echo "scale=10; $initialStep / (($i + 1) * 0.5)" | bc -l)
	step=$(echo $min $step | awk '{if ($1 < $2) print $2; else print $1}')

	echo "Iteration ${i}"
	#time hadoop jar DSGD.jar DSGD $params ${output_dir}/run$i ${output_dir}/run${last}/iter${last_iter}
	time hadoop jar DSGD.jar DSGD -D dsgd.stepSize=$step $params ${output_dir}/run$i ${output_dir}/run${last}/iter${last_iter}

	echo "Loss Iteration ${i}"
	time hadoop jar Frobenius.jar Frobenius $params ${output_dir}-loss/run$i ${output_dir}/run${last}/iter${last_iter}

	last=$i
done

