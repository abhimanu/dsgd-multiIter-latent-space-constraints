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
import org.apache.hadoop.util.*;

import org.apache.hadoop.filecache.*;

public class FrobeniusNorm2 extends Configured implements Tool  {

	public static class FrobeniusMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		int s = 0;
		int N = 0;
		int M = 0;
		int d = 0;
		int dN = 0;
		int dM = 0;
		int rank;

		Tensor U;
		Tensor V;
		public void configure(JobConf job) {
			s = job.getInt("dsgd.subepoch", 0);
			N = job.getInt("dsgd.N", 1);
			M = job.getInt("dsgd.M", 1);
			d = job.getInt("dsgd.d", 1);
			rank = job.getInt("dsgd.rank", 1);

			dN = (int)Math.ceil(1.0 * N/d); 
			dM = (int)Math.ceil(1.0 * M/d); 

			System.out.println("N,M,d,s,rank: " + N + ", " + M + ", " + d + ", " + s + ", " + rank);
			System.out.println("dN,dM: " + dN + ", " + dM);
			System.out.println("done configuring");
		}

		public void map (
			final Text key, 
			final Text value, 
			final OutputCollector<Text, Text> output, 
			final Reporter reporter
		) throws IOException { 
			reporter.progress();

			//System.out.println("GO!");

			String[] vals = value.toString().split("\\s+");
			//System.out.println("key: " + key.toString());
			//System.out.println("value: " + value.toString());

			// Load from key/values
			int i = Integer.parseInt(key.toString()); 
			int j = Integer.parseInt(vals[0]); 
			double val = Double.parseDouble(vals[1]);

			int bi = (int)Math.floor(1.0 * i / dN);
			int bj = (int)Math.floor(1.0 * j / dM);

			//System.out.println(i + "," + j + " => " + dN + "," + dM + " => " + bi + ", " + bj);

			Text newkey = new Text(bi + "," + bj);
			Text newvalue = new Text(i + "\t" + j + "\t" + val);
			output.collect(newkey, newvalue);
			reporter.incrCounter("DSGD", "Number Passed", 1);
			reporter.incrCounter("DSGD", bi + "," + bj, 1);
		}

	}
	public static class FrobeniusReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		Tensor U;
		Tensor V;
		int s_subepoch = 0;
		int rank;
		int N;
		int M;
		int d;
		int dN;
		int dM;

		public void configure(JobConf job) {
			rank = job.getInt("dsgd.rank", 1);
			N = job.getInt("dsgd.N", 1);
			M = job.getInt("dsgd.M", 1);
			d = job.getInt("dsgd.d", 1);

			dN = (int)Math.ceil(1.0 * N/d); 
			dM = (int)Math.ceil(1.0 * M/d); 

			U = new DenseTensor(N,rank);
			V = new DenseTensor(M,rank);

			int cnt = 0;
			try {
				Path[] files = DistributedCache.getLocalCacheFiles(job);
				for(Path path : files) {
					System.out.println("Path: " + path.toString());
					Scanner s = new Scanner(new File(path.toString()));
					while(s.hasNext()) {
						String key = s.next();
						int i = s.nextInt();
						int j = s.nextInt();
						double val = s.nextDouble();
						if(key.toString().charAt(0) == 'U')
							U.set(i,j,val);
						else
							V.set(i,j,val);

						if(cnt % 100000 == 0)
							System.out.println(key + ", " + i + ", " + j + ", " + val);
						cnt++;
					}
				}
			} catch (Exception e) {
				System.err.println("Error reading distributed cache.");
			}
		}

		public void reduce (
			final Text key, 
			final Iterator<Text> values, 
			final OutputCollector<Text, Text> output, 
			final Reporter reporter
		) throws IOException { 

			while(values.hasNext()) {	// run SGD for U
				String[] v = values.next().toString().split("\\s+");;
				int i = Integer.parseInt(v[0]);
				int j = Integer.parseInt(v[1]);
				double val = Double.parseDouble(v[2]);

				double eval = 0;
				for(int k = 0; k < rank; k++) {
					eval += U.get(i,k) * V.get(j,k);
				}
				reporter.progress();

				double loss = Math.pow(val - eval, 2);
				int loss_estimate = (int)Math.round(loss * 100);
				reporter.incrCounter("DSGD","Loss", loss_estimate);
				reporter.incrCounter("DSGD","Points", 1);

			}

		}

	}



	public int run (String[] args) throws Exception {
		if (args.length < 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName()); ToolRunner.printGenericCommandUsage(System.err); 
			return -1;
		}

		JobConf conf = getJobInstance();
		FileSystem fs = FileSystem.get(conf);

		FileInputFormat.addInputPath(conf, new Path(args[0])); 
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		FileStatus[] Ufiles = fs.listStatus(new Path(args[2] + "/U/"));
		for(FileStatus f : Ufiles) {
			DistributedCache.addCacheFile(f.getPath().toUri(), conf);
		}

		FileStatus[] Vfiles = fs.listStatus(new Path(args[2] + "/V/"));
		for(FileStatus f : Vfiles) {
			DistributedCache.addCacheFile(f.getPath().toUri(), conf);
		}

		JobClient.runJob(conf);

		//long loss = job.getCounters().findCounter(RED).getValue();
		//long num_points = job.getCounters().findCounter(GREEN).getValue();


		return 0;
	}

	public JobConf getJobInstance() {
		JobConf conf = new JobConf(getConf(), FrobeniusNorm2.class); 
		conf.setJobName("FNorm");

		conf.setMapperClass(FrobeniusMapper.class); 
		//conf.setCombinerClass(FrobeniusReducer.class); 
		conf.setReducerClass(FrobeniusReducer.class);

		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class); 
		conf.setMapOutputValueClass(Text.class);

		//conf.setOutputKeyClass(Text.class); 
		//conf.setOutputValueClass(Text.class);

		return conf;
	}


	/**
	*/
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new FrobeniusNorm2(), args);
		System.exit(exitCode); 
	}

}
