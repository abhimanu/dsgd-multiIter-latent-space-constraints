/**
 * DSGD Implementation for 10-725 Class Project
 * Based on that paper (insert link)
 *
 */
import java.io.*;
import java.util.*;
import java.text.*;
import java.net.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.filecache.*;

public class DSGD extends Configured implements Tool  {

	public int run (String[] args) throws Exception {

		long startTime = System.currentTimeMillis() / 1000L;

		for(int i = 0; i < args.length; i++){
			System.out.println(i + " : " + args[i]);
		}

        if (args.length < 2) {
			System.err.printf("Usage: %s [Hadoop Options] <d> <maxDimensions> <dataSets> <key> <input> <input2?> <output> <prevrun?> \n"
					+ "Required Hadoop Options:\n"
					+ "dsgd.N=# Number of columns (or rows, whatever the first dimension is) for the primary matrix or tensor.  (This dimension will be shared with coupled data.)\n"
					+ "dsgd.M0=# Range of second dimension in 1st data set\n"
					+ "dsgd.rank=# Rank of the decomposition\n"
					+ "dsgd.stepSize=# Step size for SGD.  This is typically 1/N where N is the number of non-zero elements\n"
					+ "mapred.reduce.tasks=# This should be set to the value of d so that the number of reducers matches the parallelism of the problem precisely\n\n"
					+ "Optional Hadoop Options:\n"
					+ "dsgd.P0=# Range of third dimension in 1st data set\n"
					+ "dsgd.M1=# Range of second dimension in 2nd data set\n"
					+ "dsgd.P1=# Range of third dimension in 2nd data set\n"
					+ "dsgd.debug=1 - If set to 1 will use plain text files and will be more verbose\n\n",
					getClass().getSimpleName()); ToolRunner.printGenericCommandUsage(System.err); 
			return -1;
		}

		int d = Integer.parseInt(args[0]);

		boolean is2D = (Integer.parseInt(args[1]) < 3);
		int iter = d;
		if(!is2D) {
			iter = d*d;
		}

		boolean isPaired = (Integer.parseInt(args[2]) == 2);
		String jobName = args[3];

		iter = 1;
		for(int i = 0; i < iter; i++) {
			System.out.println("Sub-iteration " + i);

			JobConf conf = getJobInstance(i,isPaired);
			FileSystem fs = FileSystem.get(conf);

			conf.setInt("dsgd.d", d);
			conf.setInt("dsgd.subepoch", i);

			int outputIndex = 4;
			if(isPaired) {
				MultipleInputs.addInputPath(conf, new Path(args[4]), KeyValueTextInputFormat.class, DSGDMapper.class);
				MultipleInputs.addInputPath(conf, new Path(args[5]), KeyValueTextInputFormat.class, DSGDMapperPaired.class);
				outputIndex = 6;
			} else {
				FileInputFormat.addInputPath(conf, new Path(args[4])); 
				outputIndex = 5;
			}
			//FileOutputFormat.setOutputPath(conf, new Path(args[outputIndex] + "/iter"+i+"/"));
			//FileOutputFormat.setOutputPath(conf, new Path(args[outputIndex] + "/final/"));
			conf.setStrings("dsgd.outputPath", args[outputIndex]);
			if(args.length > outputIndex + 1) {
				conf.setStrings("dsgd.prevPath", args[outputIndex+1]);
			} else {
				conf.setStrings("dsgd.prevPath", "");
			}

			conf.setStrings("dsgd.jobName", jobName);

			System.out.println("dsgd.lda_simplex: " + conf.getInt("dsgd.lda_simplex",-1));

			RunningJob job = JobClient.runJob(conf);

		}	

		long endTime = System.currentTimeMillis() / 1000L;
		BufferedWriter timeResults = new BufferedWriter(new FileWriter("/home/abeutel/time" +"-" + args[3]+ ".txt",true)); ;
		timeResults.write(startTime + "\t" + endTime + "\t" + (endTime-startTime) + "\n");
		timeResults.close();

		return 0;
	}
	public void addFilesToCache(String path, FileSystem fs, JobConf conf) throws Exception {

		if(fs.exists(new Path(path))) {
			FileStatus[] Vfiles = fs.listStatus(new Path(path));
			for(FileStatus f : Vfiles) {
				DistributedCache.addCacheFile(f.getPath().toUri(), conf);
			}
		}

	}

	public JobConf getJobInstance(int sub, boolean isPaired) {
		JobConf conf = new JobConf(getConf(), DSGD.class); 
		conf.setJobName("DSGD-"+sub);

		if(!isPaired) conf.setMapperClass(DSGDMapper.class); 
		conf.setReducerClass(DSGDReducer.class);

		conf.setInputFormat(KeyValueTextInputFormat.class);
		//conf.setOutputFormat(TensorMultipleOutputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);

		conf.setMapOutputKeyClass(IntArray.class); 
		conf.setMapOutputValueClass(FloatArray.class);

		conf.setPartitionerClass(DSGDPartitioner.class);
		conf.setOutputKeyComparatorClass(KeyComparator.class);
		conf.setOutputValueGroupingComparator(GroupComparator.class);


		conf.setOutputKeyClass(Text.class); 
		conf.setOutputValueClass(Text.class);

		return conf;
	}


	/**
	 * Required parameters:
	 * dsgd.subepoch (possibly set in run() with a loop
	 * dsgd.N size of input matrix 
	 * dsgd.M size of input matrix
	 * dsgd.d number of blocks to do in parallel (number of reducers) 
	 * dsgd.rank rank of matrix (dimension of U and V)
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new DSGD(), args);
		System.exit(exitCode); 
	}

}
