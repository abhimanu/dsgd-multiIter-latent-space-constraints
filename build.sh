
# Include javac calls to build the jar here

rm -rf dsgd_classes
mkdir dsgd_classes
#javac -classpath /hadoop/hadoop-current/hadoop-0.20.1-core.jar -d dsgd_classes/ *.java
javac -classpath ${HADOOP_HOME}/hadoop-0.20.1-core.jar -d dsgd_classes/ DSGD.java DSGDMapper.java DSGDReducer.java Tensor.java SparseTensor.java DenseTensor.java TensorMultipleOutputFormat.java Matrix.java DSGDMapperPaired.java DSGDPartitioner.java KeyComparator.java GroupComparator.java IntArray.java FloatArray.java
#javac -classpath /hadoop/hadoop-current/hadoop-0.20.1-core.jar -d dsgd_classes/ DSGD.java DSGDMapper.java DSGDReducer.java Tensor.java SparseTensor.java DenseTensor.java TensorMultipleOutputFormat.java Matrix.java
#javac -classpath ${HADOOP_HOME}/hadoop-${HADOOP_VERSION}-core.jar -d dsgd_classes/ DSGD.java DSGDMapper.java DSGDReducer.java Tensor.java
jar -cvf DSGD.jar -C dsgd_classes/ .
rm -rf dsgd_classes


rm -rf frobenius_classes
mkdir frobenius_classes
javac -classpath ${HADOOP_HOME}/hadoop-0.20.1-core.jar -d frobenius_classes/ Frobenius.java DSGDMapper.java Tensor.java SparseTensor.java DenseTensor.java TensorMultipleOutputFormat.java Matrix.java DSGDMapperPaired.java DSGDPartitioner.java KeyComparator.java GroupComparator.java IntArray.java FloatArray.java
jar -cvf Frobenius.jar -C frobenius_classes/ .
rm -rf frobenius_classes

rm FloatReader.class
javac -g FloatReader.java

#rm -rf norm_classes
#mkdir norm_classes
#javac -classpath /hadoop/hadoop-current/hadoop-0.20.1-core.jar -d norm_classes/ FrobeniusNorm.java Tensor.java SparseTensor.java DenseTensor.java 
#jar -cvf Norm.jar -C norm_classes/ .
#rm -rf norm_classes


#rm -rf norm_classes
#mkdir norm_classes
#javac -classpath /hadoop/hadoop-current/hadoop-0.20.1-core.jar -d norm_classes/ FrobeniusNorm2.java Tensor.java SparseTensor.java DenseTensor.java 
#jar -cvf Norm2.jar -C norm_classes/ .
#rm -rf norm_classes
