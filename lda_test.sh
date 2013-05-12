# key=1, data_set=2, lda_simplex=3, min_step=4, step=5, no_wait=6

#dataset_path="/user/abeutel/lda_test/sparse"
#dataset_path="/user/abeutel/lda_test/dense"
dataset_path=$2
N=100
M0=1000
P0=1
key="lda_test"$1
lambda=10
mean=1
sparse=0
kl=0
nnmf=1
rank=10
d=2
initialStep="$5" 
debug=1
lda_simplex=$3
min=$4
no_wait=$6
#last_iter=$(echo "scale=10; $d*$d-1" | bc -l)
last_iter=$(echo "scale=10; $d-1" | bc -l)

echo -e "Key: $key\nLambda: $lambda\nMean: $mean\nSparse: $sparse\nNNMF: $nnmf\nKL: $kl\nRank: $rank\nd: $d\ninitialStep: $initialStep\nminStep: $min\ndebug: $debug\nlda_simplex: $lda_simplex\nno_wait: $no_wait\nLast iteration: $last_iter" > ~/log-$key.txt

hadoop fs -rmr /user/abeutel/$key/*
rm ~/loss-$key.txt

params=" -D dsgd.regularizerLambda=$lambda -D dsgd.initMean=$mean -D dsgd.nnmf=$nnmf -D dsgd.sparse=$sparse -D dsgd.KL=$kl -D mapred.child.java.opts=-Xmx4096m -D dsgd.N=$N -D dsgd.M0=$M0 -D dsgd.P0=$P0 -D dsgd.rank=$rank -D dsgd.debug=$debug -D dsgd.lda_simplex=$lda_simplex -D dsgd.no_wait=$no_wait -D mapred.reduce.tasks=$d $d 3 1 $key $dataset_path"
output_dir="/user/abeutel/$key"


echo "Iteration 0"
step=$initialStep
time hadoop jar DSGD.jar DSGD -D dsgd.stepSize=$step $params ${output_dir}/run0

last=0
for i in {1..15}
do

	step=$(echo "scale=10; $initialStep / (($i + 1) * 0.5)" | bc -l)
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

