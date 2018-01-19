import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;




public class PageRank extends Configured implements Tool {
   
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " calc_rank ");
		job.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job, args[0]); // Path of previous ranks
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // Path for storing new ranks
		job.setMapperClass(PageRankMap.class);
		job.setReducerClass(PageRankReduce.class);
		job.setMapOutputKeyClass(Text.class); // Class for Mapper Output Key
		job.setMapOutputValueClass(Text.class); // Class for Mapper
		job.setOutputFormatClass(TextOutputFormat.class);
		job.getConfiguration().set("mapred.textoutputformat.separator", "#&#&SEP#&#&"); // Customized Key-Value seperator
		job.setOutputKeyClass(Text.class); // Class for Reducer Output Key
		job.setOutputValueClass(Text.class); // Class for Reducer Output Value
		
		
		job.getConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"); //this line is to stop generating _SUCCESS file
		
		return job.waitForCompletion(true) ? 0 : 1;

	}
	
	
	public static class PageRankMap extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException { //Mapper 
			double rank =0.0; // Variable in which previous rank is read
			
			String olList = ""; // Variable in which list of URLs is read
			
			
			String[] line = lineText.toString().split("#&#&SEP#&#&");
			String currentNode = line[0];				// Source URL
			line = line[1].split("#&#&RSEP#&#&");		
			rank = Double.parseDouble(line[0]);			// Previous Rank of source outlink
			
			
			
			if (line.length>1) {		// To check if Target outlink list is present or its empty

				olList = line[1];		
				String[] ols = olList.split("###&&&&&&###");  // Target outlinks array
				double newRank = rank / ols.length;		 	// Rank contribution for each target link

				for (String url : ols) {
					context.write(new Text(url), new Text("" + newRank));	// Output Target outlink and rank contribution given to it
				}
			}

			
				context.write(new Text(currentNode), new Text(olList));	// Output Source outlink and Target outlink list

		}
	}

	
	
	public static class PageRankReduce extends Reducer<Text, Text, Text, Text> {

	
		@Override
		public void reduce(Text word, Iterable<Text> links, Context context) throws IOException, InterruptedException {

			double newRank = 0.0;
			String pageLinks = "";
			double dampingFactor = 0.85;
			

			for (Text link : links) {
				
				String str = link.toString();	

				
				if (str.contains("###&&&&&&###")) { // Check if the input (Key, Value) is (Source URL, List of Target URLs)
					pageLinks	 = str;
				} else if(!str.equals("")){			// Else if Empty target link
					newRank = newRank + Double.parseDouble(str);;	// Adding all rank contribution

				}

			}

			double finalRank = (1-dampingFactor) + (dampingFactor*newRank); // PageRank equation
			
			// Output source URL with new page rank and list of target URLs
			context.write(word, new Text("" + finalRank + "#&#&RSEP#&#&" + pageLinks)); 

			
			
		}
	}

}
