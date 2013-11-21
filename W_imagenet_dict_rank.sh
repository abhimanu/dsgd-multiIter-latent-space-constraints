# key=1, data_set=2, lda_simplex=3, min_step=4, step=5, no_wait=6, N(doc)=7, M0(vocab)=8, rank=$9, d=$10, shuffleList=$11, initStepMultiplierMultiIter=$12 nnmf=13 stepFactor=$14 

N=1200000
M=100000
data_set=/user/abhimank/lda_data/data_nytimes4
lda_simplex=1
min_step=$(echo "scale=10; 0.00001" | bc -l )
step=$(echo "scale=10; 0.01" | bc -l )
no_wait=0
rank=25
#reducers=8
reducers=16
shuffleList=0
initStepMultiplierMultiIter=0.05
nnmf=1
stepFactor=1

#for rank in 5 10 25 50 100 250 500 1000 
for rank in 500 1000 
do
	key="W_Nytimes4_red16_rank"$rank
	./run_lda_scalability.sh $key ${data_set} ${lda_simplex} ${min_step} $step ${no_wait} $N $M $rank $reducers $shuffleList $initStepMultiplierMultiIter $nnmf $stepFactor
done
