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

public class DSGDPartitioner implements Partitioner<IntArray,FloatArray> {

	int d = 1;
	public void configure(JobConf job) {
		d = job.getInt("dsgd.d", 1);
	}

	public int getPartition(IntArray key, FloatArray value, int numPartitions) {

		//String[] block = key.toString().split(",");
		int bi = key.ar[0]; //Integer.parseInt(block[0]);
		int bj = key.ar[1]; //Integer.parseInt(block[1]);
		int bk = key.ar[2]; //Integer.parseInt(block[2]);

		//System.out.println("Number of partitions: " + numPartitions);

		return (bi + bj * d + bk * d * d) % numPartitions;

	}
}
