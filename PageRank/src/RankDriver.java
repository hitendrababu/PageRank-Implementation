import org.apache.hadoop.util.ToolRunner;

public class RankDriver {

	private static final int iterationCount = 10;	// Number of page rank iterations

	
	public static void main(String[] args) throws Exception{
		
		
			String[] ip = new String[2];
		
			int res = ToolRunner.run(new LinkGraph(), args);  //Call to LinkGraph.java to generate find out no. of nodes and create out-links for graph  
			if (res == 0) {
				
				for (int i = 1; i <= iterationCount; i++) {
					ip[0] = args[0] + "_rank" + (i - 1);
					if(i==iterationCount){
						ip[1] = args[0] + "_finalrank"; // The directory for final page rank file
					}
					else{
						ip[1] = args[0] + "_rank" + i; // For each page rank iteration new directory is created
					}
					res = ToolRunner.run(new PageRank(), ip);// Calls the page ran java class to get the page rank over a iteration of 10 times.
				}
				if(res == 0){
					
					ip[0] = args[0];
					ip[1] = args[1];
					res = ToolRunner.run(new CleanUp(), ip); // Cleans up all the files other than the final page rank file.
				}
				

			}
			System.exit(res);


	}

}
