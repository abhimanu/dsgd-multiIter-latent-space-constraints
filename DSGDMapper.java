/**
 * DSGD Implementation for 10-725 Class Project
 *
 */
import java.io.*;
import java.util.*;
import java.text.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.filecache.*;

public class DSGDMapper extends MapReduceBase implements Mapper<Text, Text, IntArray, FloatArray> {
	int s = 0;
    int N = 0;
    int M = 0;
    int P = 0;
    int d = 0;
	int dN = 0;
	int dM = 0;
	int dP = 0;
	int rank;

	boolean is2D;

	Random rand;

	int dataSet = 0;

	public void configure(JobConf job) {
		rand = new Random();

		dataSet = 0;

		s = job.getInt("dsgd.subepoch", 0);
		N = job.getInt("dsgd.N", 1);
		M = job.getInt("dsgd.M0", 1);
		P = job.getInt("dsgd.P0", 1);
		is2D = (P==1);

		d = job.getInt("dsgd.d", 1);
		rank = job.getInt("dsgd.rank", 1);

		dN = (int)Math.ceil(1.0 * N/d); 
		dM = (int)Math.ceil(1.0 * M/d); 
		dP = (int)Math.ceil(1.0 * P/d); 

		System.out.println("N,M,P,d,s: " + N + ", " + M + ", " + P + ", " + d + ", " + s);
		System.out.println("dN,dM,dP: " + dN + ", " + dM + ", " + dP);

	}

	public void map(Text key, Text value, OutputCollector<IntArray,FloatArray> output, Reporter reporter) throws IOException {

		//System.out.println("Key: " + key.toString());
		//System.out.println("Value: " + value.toString());
		String[] vals1 = key.toString().split("\\s+");
		String[] vals2 = value.toString().split("\\s+");
		String[] vals = new String[vals1.length + vals2.length];
		int cnt = 0;
		for(int i = 0; i < vals1.length; i++) {
			vals[cnt] = vals1[i];
			cnt++;
		}
		for(int i = 0; i < vals2.length; i++) {
			vals[cnt] = vals2[i];
			cnt++;
		}
		
		// Load from key/values
		int i = 0; 
		int j = 0; 
		int k = 0; 
		float val = 0;
		try {
			i = Integer.parseInt(vals[0]); 
			j = Integer.parseInt(vals[1]); 

			val = Float.parseFloat(vals[2]);
			if(!is2D) {
				k = Integer.parseInt(vals[2]);
				val = Float.parseFloat(vals[3]); 
			}
		} catch (Exception e) {
			System.out.println("Error on input: ");
			System.out.println("Key: " + key.toString());
			System.out.println("Value: " + value.toString());
			return;
		}

		int bi = (int)Math.floor(1.0 * i / dN);
		int bj = (int)Math.floor(1.0 * j / dM);
		int bk = (int)Math.floor(1.0 * k / dP);

		//System.out.println(bi + ", " + bj);

		int cj = (bi + s) % d;

		int ck = (bi + (int)Math.floor(s / d)) %d;

		//if(dataSet == 1 && is2D)
		
		int order = 9999;
		if (is2D) {
			order = (bj - bi + d) % d;
		} else { 
			order = d * ((bk - bi + d) % d) + ((bj - bi + d) % d);
			//order = d * ((bj - bi + d) % d) + ((bk - bi + d) % d);
		}

		int multiple = d;
		if(!is2D) {
			multiple = d*d;
		}

		multiple = 0;
		int epochs = 1;
		for(int e = 0; e < epochs; e++) {
			IntArray newkey = new IntArray(new int[]{bi,bj,bk,order + e*multiple});
			FloatArray newvalue = new FloatArray(new float[]{i,j,val,dataSet,order + e*multiple});
			if(!is2D) {
				newvalue = new FloatArray(new float[]{i,j,k,val,dataSet,order + e*multiple}); 
			}

			output.collect(newkey, newvalue);
			reporter.incrCounter("DSGD", "Number Passed", 1);
			reporter.incrCounter("DSGD", "Number Passed-"+dataSet, 1);
		}
		//reporter.incrCounter("DSGD", bi + "," + bj + "," + bk + "," + order, 1);

		//if(bj == cj && (is2D || bk == ck) && !(dataSet==1 && is2D && s >= d) ) {
			//output.collect(newkey, newvalue);
			//reporter.incrCounter("DSGD", "Number Passed", 1);
			//reporter.incrCounter("DSGD", "Number Passed-"+dataSet, 1);
			//reporter.incrCounter("DSGD", bi + "," + bj + "," + bk, 1);
			////} else if (s % d == 0) {
		//} else {
			//// Not in this iteration
			//// Ignore point
			//output.collect(new Text("OTHER"+rand.nextInt(15)), newvalue);
			//reporter.incrCounter("DSGD", "OTHER", 1);
		//}

		reporter.incrCounter("DSGD", "Number Total", 1);

	}

}

