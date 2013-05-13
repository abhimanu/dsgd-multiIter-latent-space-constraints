javac -classpath /usr/local/sw/hadoop/hadoop-0.20.1-core.jar -d classes/ PSGDRandomSplitter.java PSGDSplitterMapper.java PSGDSplitterReducer.java FloatArray.java ReaderWriterClass.java Tensor.java DenseTensor.java Matrix.java 
jar -cvf PSGDRandomSplitter.jar -C classes/ ./

# /usr/local/sw/hadoop/hadoop-0.20.1-core.jar
# /opt/hadoop/hadoop-core-0.20.203.0.jar
