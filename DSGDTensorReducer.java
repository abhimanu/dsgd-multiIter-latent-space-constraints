
public class DSGDTensorReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	Tensor U;
	Tensor V;
	Tensor W;
	int s_subepoch = 0;
	int N;
	int M;
	int P;
	int d;
	int dN;
	int dM;

	double step_size = 0.000001;	//TODO step size
	public void configure(JobConf job) {
	rank = job.getInt("dsgd.rank", 1);
	N = job.getInt("dsgd.N",1);
	M = job.getInt("dsgd.M",1);
	P = job.getInt("dsgd.P",1);
	d = job.getInt("dsgd.d",1);

	dN = (int)Math.ceil(1.0*N/d);
	dM = (int)Math.ceil(1.0*M/d);
	dP = (int)Math.ceil(1.0*P/d);

	s_subepoch = job.getInt("dsgd.subepoch",0);

	U = new DenseTensor(N,rank);
	V = new DenseTensor(M,rank);
	W = new DenseTensor(P,rank);
	
	try{
		Path[] files = DistributedCache.getLocalCacheFiles(job);
		for(Path path: files){
			Scanner s = new Scanner(new File(path.toString()));
			while(s.hasNext()){
				String key = s.next();
				int i = s.nextInt();
				int j = s.nextInt();
				double val = s.nextDouble();
				if(key.toString().charAt(0)=='U')
					U.set(i,j,val);
				else if(key.toString().charAt(0)=='V')
					V.set(i,j,val);
				else
					W.set(i,j,val);
			}
			
		}
	} catch (Exception e){
		System.err.println("Error reading Distributed Cache.");
	}
	}

	public void reduce(
		final Text key,
		final Iterator<Text> values,
		final OutputCollector<Text,Text> output,
		final Reporter
	) throws IOException{

		
	while(values.hasNext()){
		String[] v = values.next().toString().split("\\s+");
		int i = Integer.parseInt(v[0]);
		int j = Integer.parseInt(v[1]);
		int k = Integer.parseInt(v[2]);
		double val = Double.parseDouble(v[3]);
		double coeff = getGradient(i,j,k,val);
		ArrayList<Double> U_i = new ArrayList<Double>(rank);
		ArrayList<Double> V_j = new ArrayList<Double>(rank);
		ArrayList<Double> W_k = new ArrayList<Double>(rank);
	
		for(int r=0; r<rank; r++){
			U_i.add(r, U.get(i,r));
			V_j.add(r, V.get(j,r));
			W_k.add(r, W.get(k,r));
		}		
		reporter.progress();
		for(int r=0; r<rank, r++){
			setGradient(U,i,coeff,V_j,W_k,r);
			setGradient(V,j,coeff,U_i,W_k,r);
			setGradient(W,k,coeff,V_j,U_i,r);
			
		}
		reporter.incrCounter("DSGD", key.toString(), 1);
	
	}
	
	String[] block = key.toString().split(",");
	int bi = Integer.parseInt(block[0]);
	int bj = Integer.parseInt(block[1]);
	int bk = Integer.parseInt(block[2]);

	System.out.println("OUPUT UVW");
	System.out.println("block: " + bi + ", " + bj+ ", "+ bk);
	outputUVW(key,output,reporter,bi,bj,bk);

	}

	private void setGradient(Tensor u2, int i, double coeff, ArrayList<Double> v_j, ArrayList<Double> w_k, int r) {
		double newVal = u2.get(i,r) - step_size*coeff*v_j.get(r)*w_k.get(r);
		u2.set(i,r,newVal);
	}
	
	private double getGradient(int i, int j, int k, double val){
		double uvwSum = 0;
		for(int r=0; r<rank, r++){
			uvwSum+=U.get(i,r)*V.get(j,r)*W.get(k,r);
		}
		return -2*(val-uvwSum);
	}

}	

