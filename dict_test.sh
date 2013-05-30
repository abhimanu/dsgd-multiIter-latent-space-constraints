# key=1, data_set=2, lda_simplex=3, min_step=4, step=5, no_wait=6, N(doc)=7, M0(vocab)=8, rank=$9, d=$10, shuffleList=$11, initStepMultiplierMultiIter=$12 nnmf=13 

#./lda_test.sh netflix1-0.01-0.9 /user/abeutel/netflix 0 0.00001 0.01 1 17770
#./lda_test.sh nips2_20R40D_0.05.9 /user/abeutel/lda_data/data_nips 1 0.00001 0.05 1 1500 12419 20 40 0 0.9

#dataset_path="/user/abhimank/lda_test/sparse"
#dataset_path="/user/abhimank/lda_test/dense"
key="dict_test_"$1
dataset_path=$2
N=$3	# 100
M0=$4   # 1000
P0=1
initialStep="$5" 
min=$6
d=${7}
initStepMultiplierMultiIter=${8}
lambda=10
mean=0.3
sparse=0
kl=0
nnmf=0 #${13}
rank=500 #$9
debug=1
lda_simplex=0 #$3
no_wait=1 #$6
shuffleList=0
dict2=1
#last_iter=$(echo "scale=10; $d*$d-1" | bc -l)
last_iter=$(echo "scale=10; $d-1" | bc -l)

echo -e "Key: $key\nLambda: $lambda\nMean: $mean\nSparse: $sparse\nNNMF: $nnmf\nKL: $kl\nRank: $rank\nd: $d\ninitialStep: $initialStep\nminStep: $min\ndebug: $debug\nlda_simplex: $lda_simplex\nshuffleList: $shuffleList\ninitStepMultiplierMultiIter: $initStepMultiplierMultiIter\nno_wait: $no_wait\nLast iteration: $last_iter\nDict2: $dict2\n\n" > ~/log-$key.txt

hadoop fs -rmr /user/abeutel/$key/*
#hadoop fs -rmr /user/abhimank/$key/*
#hadoop fs -rmr /user/abhimank/.Trash
#rm ~/loss-$key.txt
#rm ~/time-$key.txt

params=" -D dsgd.regularizerLambda=$lambda -D dsgd.initMean=$mean -D dsgd.nnmf=$nnmf -D dsgd.sparse=$sparse -D dsgd.KL=$kl -D dsgd.N=$N -D dsgd.M0=$M0 -D dsgd.P0=$P0 -D dsgd.rank=$rank -D dsgd.debug=$debug -D dsgd.lda_simplex=$lda_simplex -D dsgd.dictionary2=$dict2 -D dsgd.shuffleList=$shuffleList -D dsgd.initStepMultiplierMultiIter=$initStepMultiplierMultiIter -D dsgd.no_wait=$no_wait -D mapred.reduce.tasks=$d $d 3 1 $key $dataset_path"

echo -e $params >>~/log-$key.txt

output_dir="/user/abeutel/$key"
#output_dir="/user/abhimank/$key"


echo "Iteration 0"
step=$initialStep
time hadoop jar DSGD.jar DSGD -D dsgd.stepSize=$step $params ${output_dir}/run0

last=0
for i in {1..15}
do

#	step=$(echo "scale=10; $initialStep / (($i + 1) * 0.5)" | bc -l)
	step=$(echo "scale=10; $initialStep / ($i + 1)" | bc -l)
	step=$(echo $min $step | awk '{if ($1 < $2) print $2; else print $1}')

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
	#time hadoop jar Loss.jar Loss $params ${output_dir}-loss/run$i ${output_dir}/run${last}/iter${last_iter}
	#last=$i
#done

