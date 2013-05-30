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

public class DSGDReducer extends MapReduceBase implements Reducer<IntArray, FloatArray, NullWritable, NullWritable> {
	DenseTensor U;
	DenseTensor[] V;
	DenseTensor Vseen;
	DenseTensor[] W;

	int rank;

	int N;
	int[] M;
	int[] P;

	int d;
	int dN;
	int[] dM;
	int[] dP;

	boolean[] is2D;

	double step_size = 0.000001;
	double stepMultiplierMultiIter = 1.0;
	double initStepMultiplierMultiIter = 0.5;
	boolean shuffleList = false;
	double[] weight;
	int dataSets = 1;
	JobConf thisjob;
	String jobName;

	String outputPath; 
	String prevPath;

	boolean debug = false;
	String taskId;

	boolean sparse = false;
	float lambda = 10;
	boolean nonNegative = false;
	boolean KL = false;
	boolean lda_simplex = true;
	boolean no_wait = true;
	boolean dictionary = false;
	boolean dictionary2 = false;

	float initMean = 0;

	public void configure(JobConf job) {
		//System.out.println("TEST");

		thisjob = job;

		outputPath = job.getStrings("dsgd.outputPath")[0];
		prevPath = job.getStrings("dsgd.prevPath", new String[]{""})[0];

		jobName = job.getStrings("dsgd.jobName", new String[]{""})[0];

		if(job.getInt("dsgd.debug",0) == 1) {
			debug = true;
		}

		sparse = (job.getInt("dsgd.sparse",0) == 1);
		nonNegative = (job.getInt("dsgd.nnmf",0) == 1);
		lda_simplex = (job.getInt("dsgd.lda_simplex",0) == 1);
		no_wait = (job.getInt("dsgd.no_wait",0) == 1);
		KL = (job.getInt("dsgd.KL",0) == 1);
		shuffleList = (job.getInt("dsgd.shuffleList",0) == 1);
		dictionary = (job.getInt("dsgd.dictionary",0) == 1);
		dictionary2 = (job.getInt("dsgd.dictionary2",0) == 1);

		lambda = job.getFloat("dsgd.regularizerLamda",10);
		initMean = job.getFloat("dsgd.initMean",0);

		if(job.getInt("dsgd.M1", 1) == 1) {
			dataSets = 1;
		} else {
			dataSets = 2;
		}
		System.out.println("Data sets: " + dataSets);

		V = new DenseTensor[dataSets];
		W = new DenseTensor[dataSets];
		M = new int[dataSets]; 
		P = new int[dataSets]; 
		dM = new int[dataSets]; 
		dP = new int[dataSets]; 
		is2D = new boolean[dataSets]; 
		weight = new double[dataSets]; 

		d = job.getInt("dsgd.d", 1);
		rank = job.getInt("dsgd.rank", 1);
		N = job.getInt("dsgd.N", 1);
		dN = (int)Math.ceil(1.0 * N/d); 
		U = new DenseTensor(dN,rank);

		System.out.println("d,rank,N,dn:" + d + ", " + rank + ", " + N + ", " + dN);

		for(int i = 0; i < dataSets; i++) {
			M[i] = job.getInt("dsgd.M"+i, 1);
			P[i] = job.getInt("dsgd.P"+i, 1);

			weight[i] = job.getFloat("dsgd.weight"+i,1.0f);

			is2D[i] = (P[i]==1);

			dM[i] = (int)Math.ceil(1.0 * M[i]/d); 
			dP[i] = (int)Math.ceil(1.0 * P[i]/d); 

			System.out.println("dataSet, M,P,dM,dP: " + i + ", " + M[i] + ", " + P[i] + ", " + dM[i] + ", "  + dP[i]);

			V[i] = new DenseTensor(dM[i],rank);
			W[i] = new DenseTensor(dP[i],rank);
		}
		Vseen = new DenseTensor(dM[0],1);
		Vseen.zero();

		step_size = job.getFloat("dsgd.stepSize",0.000001f);
		initStepMultiplierMultiIter = job.getFloat("dsgd.initStepMultiplierMultiIter",0.5f);
		System.out.println("Step size: " + step_size);

		taskId = getAttemptId(job);
			System.out.println("sparse, lambda, nnmf, lda_simplex, shuffleList, initStepMultiplierMultiIter, no_wait, step_size: "+ sparse + ", " + lambda + ", " + nonNegative + ", " + lda_simplex + ", " + shuffleList + ", "  + initStepMultiplierMultiIter + ", "+ no_wait + ", " + step_size);
	}

	public static String getAttemptId(Configuration conf) { // throws IllegalArgumentException {
		if (conf == null) {
			return "";
			//throw new NullPointerException("conf is null");
		}

		String taskId = conf.get("mapred.task.id");
		if (taskId == null) {
			return "";
			//throw new IllegalArgumentException("Configutaion does not contain the property mapred.task.id");
		}

		String[] parts = taskId.split("_");
		//if (parts.length != 6 || !parts[0].equals("attempt") || (!"m".equals(parts[3]) && !"r".equals(parts[3]))) {
			//throw new IllegalArgumentException("TaskAttemptId string : " + taskId + " is not properly formed");
		//}
		return parts[parts.length - 1];

		//return parts[4] + "-" + parts[5];
	}


	public boolean checkForFile(String path)  throws IOException {
		FileSystem fs = FileSystem.get(thisjob);
        boolean ans = checkForFile(path,fs);
		fs.close();
		return ans;
	}
	public boolean checkForFile(String path, FileSystem fs)  throws IOException {
		return fs.exists(new Path(path));
	}
	public boolean checkForFile(char c, int index, int iter, FileSystem fs) throws IOException {
		String fp = outputPath + "/iter" + iter + "/" + c + "/" + c + index;
		return checkForFile(fp,fs);
	}

	public void writeLog(char c, int index, int iter, FileSystem fs) throws IOException {
		try {
			String fp = outputPath + "/log";
			Path path = new Path(fp);
			if (!fs.exists(path)) {
				fs.mkdirs(path);
			}

			fp += "/" + c  + index + "." + iter;
			path = new Path(fp);
			if (!fs.exists(path)) {
				fs.createNewFile(path);
			}
		} catch (IOException e) { }
	}

	public void writeFactor(DenseTensor T, char c, int index, int iter, int minI, final Reporter reporter) throws IOException {

		//System.out.println("WRITE FACTOR: " + c + index + ", " + iter);
		FileSystem fs = FileSystem.get(thisjob);
		float[] rankSum = new float[T.M];
		for(int i = 0; i < T.M; i++) {					// .M is the rank dimension
			rankSum[i]=0;
		}

		String fp = outputPath + "/iter" + iter + "/" + c;
		Path path = new Path(fp);
		if (!fs.exists(path)) {
			fs.mkdirs(path);
		}

		fp += "/" + c + index + "." + taskId;
		FSDataOutputStream out = fs.create(new Path(fp));

		System.out.println("Write to " + fp);
		for(int i = 0; i < T.N; i++) {
			for(int j = 0; j < T.M; j++) {
				float valMat = T.get(i,j);
				if(c=='V' && lda_simplex)
					valMat=valMat*Vseen.get(i,0);
				if(!debug) {
					//if(sparse) {
						//if(T.get(i,j) != 0) {
							//out.writeInt(j);
							//out.writeInt(j);
							//out.writeFloat(T.get(i,j));
						//}
					//} else {
						out.writeFloat(valMat);
						//}
				} else {
					//if(Double.isNaN(T.get(i,j))) {
						//System.err.println("Error writing " + i + ", " + j + " - " + minI + ": " + T.get(i,j));
					//}
					String val = c + "" + index + "\t" + (i+minI) + "\t" + j + "\t" + valMat + "\n";
					out.writeBytes(val);
				}
				rankSum[j]+=valMat;
			}
		}
        if( (lda_simplex || dictionary2) && c!='U')
			writeSumRank(c, rankSum, index, iter, fs);
		writeLog(c,index,iter,fs);
		fs.close();

	}

	public void updateFactorDebug(FSDataInputStream in, DenseTensor M, char c, int minI) throws IOException {
		Scanner s = new Scanner(in);
		try{
		while(s.hasNext()) {
			String key = s.next();
			int i = s.nextInt() - minI;
			int j = s.nextInt();
			float val = s.nextFloat();
			if(key.toString().charAt(0) == c) {
				M.set(i,j,val);
			} else {
				System.out.println("ERROR reading input.  Mismatch on factors.");
			}
		}
		}catch(NoSuchElementException e){
			System.out.println("received NoSuchElementException, due to file half written, throwing up as IOException");
			throw new IOException(e.getMessage());
		}
	}



	public void writeSumRank(char c, float[] rankSum, int index, int iter, FileSystem fs) throws IOException{
			
		String normalizerPath = outputPath + "/iter" + iter + "/" + c + "/" + c ;
		String path = normalizerPath+"sum"+index+"."+taskId;
		FSDataOutputStream out = fs.create(new Path(path));

		System.out.println("Write to " + path);

		for(int i=0;i<rank;i++){						// This would be trouble if rank doesnt math .M
			String val = c + "" + index + "\t" + i + "\t" + rankSum[i] + "\n";
			out.writeBytes(val);
		}
	
	}


	public boolean normalizeFactor(DenseTensor M, String normalizerPath, FileSystem fs) throws IOException{
		System.out.println("SIMPLEX CONSTRAINT normalizing factor.");
		float rankSum[] = new float[M.M];
		for(int i = 0; i < M.M; i++) {					// .M is the rank dimension
			rankSum[i]=0;
		}
        boolean flag = false;
		for(int i=0; i<d; i++){
			flag = false;
			String path = normalizerPath+"sum"+i;
			FileStatus[] allFiles = fs.globStatus(new Path(path + ".*"));		// Is it okay to take fs in a for loop
			if(allFiles != null && allFiles.length > 0) {
				for ( FileStatus f : allFiles ) { 

					System.out.println("Update from " + f.getPath().toUri().getPath());
					try {
						//Path pt=new Path(path);
						FSDataInputStream in = fs.open(f.getPath());
						Scanner s = new Scanner(in);
						while(s.hasNext()) {					// .M is the rank dimension
                            String key = s.next();
							int j = s.nextInt();
							float val = s.nextFloat();
							rankSum[j]+=val;
						}
						System.out.println("success on reading sum factors");
						flag=true;
						break;	// break the inner for loop
						//return true && normalizationFlag;				// TODO: This might lead to hang scenarios?
					} catch (EOFException e) {
						//fs.close();
						System.out.println("ERROR EOFException, continue");
						//return false;
						continue;
					} catch (IOException e) {
							System.out.println("ERROR reading factors");
							continue;
					} 
				}
			}
            if(!flag)
				return flag;	//If it has failed in the task return false

		}

        if(dictionary2) {
			for(int j = 0; j < M.M; j++) {					// .M is the rank dimension
				for(int i = 0; i < M.N; i++) {					// .M is the rank dimension
					if(rankSum[j] * rankSum[j] > 1.0f && rankSum[j] != 0) {
						float newVal = M.get(i,j)*1.0f/rankSum[j];
						M.set(i,j,newVal);
					}
				}
			}

		} else {
			for(int j = 0; j < M.M; j++) {					// .M is the rank dimension
				for(int i = 0; i < M.N; i++) {					// .M is the rank dimension
					float newVal = M.get(i,j)*1.0f/rankSum[j];
					M.set(i,j,newVal);
				}
			}
		}
		return true;
	}
	
	public boolean updateFactor(DenseTensor M, char c, int index, int iter, int minI, final Reporter reporter) throws IOException {

		System.out.println("Update " + c + index + ", iter " + iter + " (minI = " + minI + ")");

		//if(debug) {
			//return updateFactorDebug(M,c,index,iter,minI,reporter);
		//}

		if(M.iter == iter) {
			return true;
		}

		FileSystem fs = FileSystem.get(thisjob);

		if(iter >= 0) {
			if((!lda_simplex && !dictionary2)||c=='U'){			// U 's simplex constrainit is different from V
				String logfile = outputPath + "/log/" + c + index + "." + iter;
				System.out.println("Check log: " + c + index + ", " + iter + ": " + logfile);
				if(!checkForFile(logfile,fs)) {
					fs.close();
					return false;
				}
				System.out.println("Log file found");
			}else{
				System.out.println("SIMPLEX CONSTRAINT case update factor.");
				for(int i=0; i<d; i++){
					String logfile = outputPath + "/log/" + c + i + "." + iter;
					System.out.println("Check log: " + c + i + ", " + iter + ": " + logfile);
					if(!checkForFile(logfile,fs)) {
						fs.close();
						return false;
					}
					System.out.println("Log file found: Simplex case");

				}
			}

		}

		M.reset(initMean);
		
		String normalizerPath = outputPath + "/iter" + iter + "/" + c + "/" + c ;

		String path = outputPath + "/iter" + iter + "/" + c + "/" + c + index;
		if(iter < 0 && prevPath != "") {
			path = prevPath + "/" + c + "/" + c + index;
			normalizerPath = prevPath + "/" + c + "/" + c ;
		}

		FileStatus[] allFiles = fs.globStatus(new Path(path + ".*"));

		if(allFiles != null && allFiles.length > 0) {
			for ( FileStatus f : allFiles ) { 

				System.out.println("Update from " + f.getPath().toUri().getPath());
				try {
					//Path pt=new Path(path);
					FSDataInputStream in = fs.open(f.getPath());
					if(debug) {
						updateFactorDebug(in,M,c,minI);
					} else {
						for(int i = 0; i < M.N; i++) {
							for(int j = 0; j < M.M; j++) {
								M.set(i,j,in.readFloat());
							}
						}
					}
					boolean normalizationFlag = true;
                    if( (lda_simplex || dictionary2) && c!='U')
						normalizationFlag=normalizeFactor(M, normalizerPath, fs);
					if(!normalizationFlag)
						System.out.println("Something is wrong in the sums");
					//M.iter = iter;
					fs.close();
					if(normalizationFlag){
						M.iter = iter;
						System.out.println("success on reading factors");
					}
					return normalizationFlag;//(true && normalizationFlag);				// TODO: This might lead to hang scenarios?
				} catch (EOFException e) {
					//fs.close();
					System.out.println("ERROR EOFException, continue");
					//return false;
					continue;
				} catch (IOException e) {
					if (iter < 0) {
						System.out.println("ERROR reading factors");
						continue;
					} else {
						System.out.println("ERROR reading factors, continue");
						//fs.close();
						//throw e;
						continue;
					}
				} 
			}
		}
		fs.close();
		if(iter < 0) {
			System.out.println("ERROR reading factors exiting true");
			return true;
		}
		return false;
	}


	public void performUpdate(FloatArray v, Reporter reporter){
	
			int i = (int)(v.ar[0]);
			int j = (int)(v.ar[1]);
			int dataSet = (int)v.ar[v.ar.length-2];
			int subepoch = (int)v.ar[v.ar.length-1];


			int k = 0;
			float val = v.ar[2];
			if(!is2D[dataSet]) {
				k = (int)(v.ar[2]);
				val = v.ar[3];
			}

			if(Float.isNaN(val) || Float.isInfinite(val) || Math.abs(val) > 10) { 
				System.out.print("val NaN: ");
				System.out.println(v.toString());
			}


			int bi = (int)Math.floor(1.0 * i / dN);
			int bj = (int)Math.floor(1.0 * j / dM[dataSet]);
			int bk = (int)Math.floor(1.0 * k / dP[dataSet]);
	
			i = i - bi * dN;
			j = j - bj * dM[dataSet];
			k = k - bk * dP[dataSet];

			float coeff = getGradient(i,j,k,val,dataSet);
			coeff = (float)(coeff * weight[dataSet]);

			if(Float.isNaN(coeff) || Float.isInfinite(coeff)) { 
				System.out.print("coeff NaN: ");
				System.out.println(i + ", " + j + ", " + k + ", " + val + ", " + dataSet + ": " + coeff);
			}

			float[] U_i = new float[rank];
			float[] V_j = new float[rank];
			float[] W_k = new float[rank];

			for(int r=0;r<rank;r++){
				U_i[r] = U.get(i,r);
				V_j[r] = V[dataSet].get(j,r);

				if(!is2D[dataSet]) {
					W_k[r] = W[dataSet].get(k,r);
				} else {
					W_k[r] = 1.0f;
				}
			}

			reporter.progress();

			for(int r=0;r<rank;r++){
				setGradient(U,i,r, coeff, V_j,W_k,'U');
				setGradient(V[dataSet],j,r, coeff, U_i,W_k,'V');

				if(!is2D[dataSet]) {
					setGradient(W[dataSet],k,r, coeff, U_i,V_j,'W');
				}
			}

			if(lda_simplex || dictionary){
				normalize_doc_topic(U,i);
				if(dataSet==0)
					Vseen.set(j,0,1);			// This means that this entry exists
			}
	}
	
	class LogPollingThread implements Runnable {
		String name;
		int tj;
        int tjNew;
		int tk;
		int tkNew;
		int subepoch;
		int curSubepoch;
		int Ublock;
		Reporter reporter;
		DSGDReducer reducer;

		Thread t;
		// at present only do for no simplex.
		//pLogPoller = new LogPollingThread(jobName+"-pLogPoller_"+taskId, tj, tjNew, tk, tkNew, subepoch, curSubepoch, Ublock, reporter, this);
		LogPollingThread(String name, int tj, int tjNew, int tk, int tkNew, int subepoch, int curSubepoch, int Ublock, Reporter reporter, DSGDReducer reducer){
			this.name = name;
			this.tj = tj;
			this.tjNew = tjNew;
			this.tk =tk;
			this.tkNew = tkNew;
			this.subepoch = subepoch;
			this.curSubepoch = curSubepoch;
			this.Ublock = Ublock;
            this.reporter = reporter;
			this.reducer = reducer;

			t = new Thread(this, name);
			System.out.println("Thread "+ name + "created");
			t.start();		
		}
	
		public void run(){

			boolean doneUpdating = false;
			int waiting = 0;

			// TODO: not doing simplex yet; i.e. synching over all reducers.
			while (!doneUpdating) {

				boolean passed = true;
				for(int set = 0; set < reducer.dataSets; set++) {

					//if(!is2D[set] || tkNew == 0) {
					if(!reducer.is2D[set] || subepoch < d) {
						if(tj != tjNew) {
							// read V[set]
							char vc = (set == 0) ? 'V' : 'A';
							passed = checkPLog(vc,tjNew,curSubepoch) && passed;
						}
					}

					if(!reducer.is2D[set]) {
						// read W[set]
						if(tk != tkNew) {
							char wc = (set == 0) ? 'W' : 'B';
							passed = checkPLog(wc,tkNew,curSubepoch) && passed;
						}
					}
					}

				doneUpdating = (curSubepoch < 0) || passed;			// curSubepoch < 0 is redundant here
				if(!doneUpdating) {
					System.out.println("Thread Waiting: "+waiting);
					reporter.incrCounter("DSGD", "Time Waiting", 1);
					reporter.incrCounter("Time waiting", "U" + Ublock, 1);
					//reporter.progress();
					try{
						Thread.sleep(3000);							// decrease the sleep time?
					} catch (Exception e) { }
					reporter.progress();
					waiting++;
				}
				}
		}

		public boolean checkPLog(char c, int index, int iter){
			try{
			FileSystem fs = FileSystem.get(reducer.thisjob);
			
			if((!reducer.lda_simplex && !reducer.dictionary2)){			// U 's simplex constrainit is different from V
				String logfile = reducer.outputPath + "/Plog/" + c + index + "." + iter;
				System.out.println("Thread: Check log: " + c + index + ", " + iter + ": " + logfile);
				if(!reducer.checkForFile(logfile,fs)) {         
					fs.close();
					return false;
				}
				System.out.println("Thread: Log file found");
				fs.close();
				return true;
			}else{
				for(int i=0; i<d; i++){
					String logfile = reducer.outputPath + "/Plog/" + c + index + "." + iter;
					System.out.println("Thread: Check log: " + c + index + ", " + iter + ": " + logfile);
					if(!reducer.checkForFile(logfile,fs)) {
						fs.close();
						return false;
					}
					System.out.println("Thread Log file found: Simplex case");

				}
				fs.close();
				return true;
			} 
			} catch(IOException e){
				System.out.println("Thread: got IOException in checkPLog");
				return false;
			}
			
		}
	
	}


	public void writePLog(char c, int index, int iter, Reporter reporter) throws IOException {
		try {
			FileSystem fs = FileSystem.get(thisjob);
			String fp = outputPath + "/Plog";
			Path path = new Path(fp);
			if (!fs.exists(path)) {
				fs.mkdirs(path);
			}

			fp += "/" + c  + index + "." + iter;
			path = new Path(fp);
			if (!fs.exists(path)) {
				fs.createNewFile(path);
			}
		} catch (IOException e) { }
	}


	public int updateThroughTheSubepochs(int curSubepoch, int subepoch, int bi, int Ublock,  LinkedList<FloatArray> queue, Reporter reporter) throws IOException{

//				System.out.println("New subepoch: " + bi +", " + bj  +", " + bk + ": " + subepoch + " (" + numSoFar + ")");
				
				int tj = (bi + curSubepoch) % d;
				int tk = (bi + (int)Math.floor(curSubepoch / d)) %d;

				int tjNew = (bi + subepoch) % d;
				int tkNew = (bi + (int)Math.floor(subepoch / d)) %d;

				System.out.println("Tj,Tk,TjNew,TkNew: " + bi +", " + tj  +", " + tk + ", " + tjNew + ", " + tkNew);

                // Abhi: Do extra work here

				// write plogs.
				if((curSubepoch >= 0) && (no_wait) && (!queue.isEmpty())){	// -ve curSubEpoch will have to read though but that read is not done via polling
					// write our own Plogs first
					System.out.println("For NO_WAIT case initializing the polling thread");
					for(int set = 0; set < dataSets; set++) {

						//if(!is2D[set] || tkNew == 0) {
						if(!is2D[set] || subepoch < d) {
							if(tj != tjNew) {
								// read V[set]
								char vc = (set == 0) ? 'V' : 'A';
								writePLog(vc,tjNew,curSubepoch,reporter);
							}
						}

						if(!is2D[set]) {
							// read W[set]
							if(tk != tkNew) {
								char wc = (set == 0) ? 'W' : 'B';
								writePLog(wc,tkNew,curSubepoch,reporter);
							}
						}
					}
					// intialize and run thread
				    LogPollingThread pLogPoller = new LogPollingThread(jobName+"-pLogPoller_"+taskId, tj, tjNew, tk, tkNew, subepoch, curSubepoch, Ublock, reporter, this);

				// Doing extra updates till the pLogPoller is alive
					int countExtraUpdates = 0;
					stepMultiplierMultiIter=1.0;
					while(pLogPoller.t.isAlive()){
						if(shuffleList) Collections.shuffle(queue);
						stepMultiplierMultiIter = stepMultiplierMultiIter*initStepMultiplierMultiIter;
						for(FloatArray listVal: queue) {
							if(!pLogPoller.t.isAlive())
								break;
							FloatArray valArray = listVal;
							performUpdate(valArray, reporter);
							countExtraUpdates++;
						}
						System.out.println("stepMultiplierMultiIter: "+ stepMultiplierMultiIter);
					}
					System.out.println("Extra Updates in U"+ Ublock + ": "+countExtraUpdates);
					reporter.incrCounter("Extra Updates", "U"+Ublock, countExtraUpdates);
				}

				// Now back to normal: synching reducers know that both are ready to write otu their factors.
				if(subepoch == 0) {  // First iteration, possibly get stuff from past run and must load U
					updateFactor(U,'U',bi,curSubepoch,dN*bi,reporter);	//  We dont need thread waiting For this, i.e. U
				} else {
					// Output
					System.out.println("Output!");
					for(int set = 0; set < dataSets; set++) {

						//if(!is2D[set] || tkNew == 0) {
						if(!is2D[set] || subepoch < d) {
							// output V[set]
							if(tj != tjNew) {
								char vc = (set == 0) ? 'V' : 'A';
								writeFactor(V[set],vc,tj,curSubepoch,dM[set]*tj,reporter);
							}
						}

						if(!is2D[set]) {
							// output W[set]
							if(tk != tkNew) {
								char wc = (set == 0) ? 'W' : 'B';
								writeFactor(W[set],wc,tk,curSubepoch,dP[set]*tk,reporter);
							}
						}
					}

				}

				U.iter = curSubepoch;
				// Input
				boolean doneUpdating = false;
				int waiting = 0;
				while (!doneUpdating) {

					boolean passed = true;
					for(int set = 0; set < dataSets; set++) {

						//if(!is2D[set] || tkNew == 0) {
						if(!is2D[set] || subepoch < d) {
							if(tj != tjNew) {
								// read V[set]
								char vc = (set == 0) ? 'V' : 'A';
								passed = updateFactor(V[set],vc,tjNew,curSubepoch,dM[set]*tjNew,reporter) && passed;
							}
						}

						if(!is2D[set]) {
							// read W[set]
							if(tk != tkNew) {
								char wc = (set == 0) ? 'W' : 'B';
								passed = updateFactor(W[set],wc,tkNew,curSubepoch,dP[set]*tkNew,reporter) && passed;
							}
						}
					}

					doneUpdating = (curSubepoch < 0) || passed;
					if(!doneUpdating) {
						System.out.println("Waiting: "+waiting);
						reporter.incrCounter("DSGD", "Time Waiting", 1);
						reporter.incrCounter("Time waiting", "U" + Ublock, 1);
						reporter.progress();
						try{
							Thread.sleep(3000);
						} catch (Exception e) { }
						reporter.progress();
						waiting++;
					}
				}

				if(queue.isEmpty())
					reporter.incrCounter("DSGD", "Empty Blocks", 1);

				curSubepoch = subepoch;
				//ci = bi;
				//cj = bj;
				//ck = bk;
				//numSoFar = 0;
				queue.clear();
				return curSubepoch;
	
	}

	public void reduce (
		final IntArray key, 
		final Iterator<FloatArray> values, 
		final OutputCollector<NullWritable, NullWritable> output, 
		final Reporter reporter
	) throws IOException { 

		System.out.println("Key: " + key.toString());
		
		int numSoFar = 0;
		int curSubepoch = -99999;
		//int ci = -99999;
		//int cj = -99999;
		//int ck = -99999;
		int Ublock = -99999;
		boolean first = true;

		LinkedList<FloatArray> queue = new LinkedList<FloatArray>();

		while(values.hasNext()) {	// run SGD for U

			FloatArray v = values.next();

			int i = (int)(v.ar[0]);
			int j = (int)(v.ar[1]);
			int dataSet = (int)v.ar[v.ar.length-2];
			int dataSubepoch = (int)v.ar[v.ar.length-1];


			int k = 0;
			float val = v.ar[2];
			if(!is2D[dataSet]) {
				k = (int)(v.ar[2]);
				val = v.ar[3];
			}

			if(Float.isNaN(val) || Float.isInfinite(val) || Math.abs(val) > 10) { 
				System.out.print("val NaN: ");
				System.out.println(v.toString());
			}


			int bi = (int)Math.floor(1.0 * i / dN);
			int bj = (int)Math.floor(1.0 * j / dM[dataSet]);
			int bk = (int)Math.floor(1.0 * k / dP[dataSet]);

			if(first) {
				Ublock = bi;
				first = false;
			}
			int subepoch = 0;
//public int updateThroughTheSubepochs(int curSubepoch, int subepoch, int bi, int Ublock,  LinkedList<FloatArray> queue, Reporter reporter){
//
			while (dataSubepoch != curSubepoch) {
				if(dataSubepoch==0)
					subepoch=0;
				else
					subepoch = curSubepoch+1;
				curSubepoch = updateThroughTheSubepochs(curSubepoch, subepoch, bi, Ublock, queue, reporter);

			}

			numSoFar++;

			queue.add(v);

			performUpdate(v, reporter);

            /*
			// Alex: check this
			i = i - bi * dN;
			j = j - bj * dM[dataSet];
			k = k - bk * dP[dataSet];

			float coeff = getGradient(i,j,k,val,dataSet);
			coeff = (float)(coeff * weight[dataSet]);

			if(Float.isNaN(coeff) || Float.isInfinite(coeff)) { 
				System.out.print("coeff NaN: ");
				System.out.println(i + ", " + j + ", " + k + ", " + val + ", " + dataSet + ": " + coeff);
			}

			float[] U_i = new float[rank];
			float[] V_j = new float[rank];
			float[] W_k = new float[rank];

			for(int r=0;r<rank;r++){
				U_i[r] = U.get(i,r);
				V_j[r] = V[dataSet].get(j,r);

				if(!is2D[dataSet]) {
					W_k[r] = W[dataSet].get(k,r);
				} else {
					W_k[r] = 1.0f;
				}
			}

			reporter.progress();

			for(int r=0;r<rank;r++){
				setGradient(U,i,r, coeff, V_j,W_k);
				setGradient(V[dataSet],j,r, coeff, U_i,W_k);

				if(!is2D[dataSet]) {
					setGradient(W[dataSet],k,r, coeff, U_i,V_j);
				}
			}
			if(lda_simplex)
				normalize_doc_topic(U,i);
            */
			reporter.incrCounter("DSGD", "Nonempty Number Processed", 1);
			reporter.incrCounter("Subepochs", "U" + Ublock, 1);
		}

		//System.out.println("Last batch: " + numSoFar);
		System.out.println("Total datapoints: " + numSoFar);

		while(curSubepoch!=d-1){			// now update until the last block
			// NOTE passing Ublock here.
			curSubepoch = updateThroughTheSubepochs(curSubepoch, curSubepoch+1, Ublock, Ublock, queue, reporter);
		}

		writeFactor(U,'U',Ublock,curSubepoch,dN*Ublock,reporter);

		for(int set = 0; set < dataSets; set++) {

			int tj = (Ublock + curSubepoch) % d;
			int tk = (Ublock + (int)Math.floor(curSubepoch / d)) %d;

			// output V[i]
			char vc = (set == 0) ? 'V' : 'A';
			writeFactor(V[set],vc,tj,curSubepoch,dM[set]*tj,reporter);

			if(!is2D[set]) {
				// output W[i]
				char wc = (set == 0) ? 'W' : 'B';
				writeFactor(W[set],wc,tk,curSubepoch,dP[set]*tk,reporter);
			}
		}

	}
	

    private void normalize_doc_topic(DenseTensor T, int i){
		float sum = 0;
		for(int r=0; r<rank; r++){
			sum+=T.get(i,r);		
		}
        if(sum==0)
			return;
		for(int r=0; r<rank; r++){
			float val = T.get(i,r);
			T.set(i,r,val*1.0f/sum);
		}
	}

	//private void setGradient(Matrix M, int i, int r, double coeff, ArrayList<Double> M1, ArrayList<Double> M2) {
	private void setGradient(DenseTensor M, int i, int r, double coeff, float[] M1, float[] M2, char matrix) {
		if(KL) {
			setGradientKL(M, i, r, coeff, M1, M2);
			return;
		}

		//double newVal = M.get(i, r) - step_size * stepMultiplierMultiIter * coeff * M1.get(r) * M2.get(r);
		float newVal = (float)(M.get(i, r) - step_size * stepMultiplierMultiIter * coeff * M1[r] * M2[r]);
		
		if(Float.isNaN(newVal) || Float.isInfinite(newVal)) { 
			System.out.print("newVal NaN: ");
			System.out.println(i + ", " + r + ", " + coeff + ": " + newVal);
		}

		if(sparse || (dictionary && matrix == 'V') || (dictionary2 && matrix == 'U')) {
			//System.out.println("SPARSITY CONSTRAINT being validated");
			newVal = softThreshold(newVal,lambda * step_size * stepMultiplierMultiIter);
		}

		if( (nonNegative || (dictionary && matrix == 'U') || (dictionary2 && matrix == 'V') )&& newVal < 0) {
			//System.out.println("NON-NEGATIVITY CONSTRAINT being validated");
			newVal = 0f;
		}

		//System.out.println("Change: " + (step_size * stepMultiplierMultiIter * coeff * M1[r] * M2[r]));
		M.set(i, r, newVal);
	}


	private float softThreshold(float val, double lambda){
		if(val > lambda)
			return (float)(val - lambda);
		if(val < -1.0f * lambda)
			return  (float)(val + lambda);
		return 0f;
	}


	float pertubation = 0.00001f;
	private void setGradientKL(DenseTensor M, int i, int r, double coeff, float[] M1, float[] M2) {
		float newVal = (float)(M.get(i,r) - step_size*stepMultiplierMultiIter*coeff*M1[r]*M2[r]);
		
		if(Float.isNaN(newVal) || Float.isInfinite(newVal)) { 
			System.out.print("newVal NaN: ");
			System.out.println(i + ", " + r + ", " + coeff + ": " + newVal);
		}

		if(newVal < 0) {
			newVal = 0f;
		}

		M.set(i, r, newVal);
	}

	private float getGradientKL(int i, int j, int k,  float val, int dataSet){
		float sum = 0;
		for(int r=0;r<rank;r++) {
            float prod = U.get(i, r)*V[dataSet].get(j, r); 
            if(!is2D[dataSet]) { 
                prod *= W[dataSet].get(k,r);
            }
            sum+=prod;
        }
        if(Float.isNaN(sum) || Float.isInfinite(sum)){
            System.out.println("getGradient sum NaN: " + sum);
        }
        //System.out.println(i + "," + j + "," + k + ": " + val + " - " + sum);
		//return -1.0f*val/sum;
		if(sum == 0) {
			return -1.0f*val/pertubation;
		}
        return -1.0f*(val)/(sum);
	}

	private float getGradient(int i, int j, int k,  float val, int dataSet){
		if(KL) {
			return getGradientKL(i,j,k,val,dataSet);
		}

		float sum = 0;
		for(int r=0;r<rank;r++) {
			float prod = U.get(i, r)*V[dataSet].get(j, r); 
			if(!is2D[dataSet]) { 
				prod *= W[dataSet].get(k,r);
			}
			sum+=prod;
		}
		if(Float.isNaN(sum) || Float.isInfinite(sum)){
			System.out.println("getGradient sum NaN: " + sum);
		}
		//System.out.println(i + "," + j + "," + k + ": " + val + " - " + sum);
		return -2.0f*(val-sum);
	}



}
	
