
rank=100
output="/home/abeutel/DSGD/Tensor-L1-7-V/"
output2="/home/abeutel/DSGD/Tensor-L1-7-V-Sorted/"

rm -rf $output2
mkdir $output2

for (( i=0; i < $rank; i++ ))
do

	echo "Sort col $i"
	sort -k2gr -t $'\t' $output/col-$i.txt  > $output2/col-$i.txt

done
