import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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




public class LinkGraph extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		/*
		 * This job is to find total number of links 
		 */
		Job job1 = Job.getInstance(getConf(), " count ");
		job1.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job1, args[0]); // Path to Wiki file
		FileOutputFormat.setOutputPath(job1, new Path(args[0] + "_temp")); // Temp Output Path
		job1.setMapperClass(CountMap.class); // Mapper Class
		job1.setReducerClass(CountReduce.class); // Reducer Class
		job1.setMapOutputKeyClass(Text.class); // Class for Mapper Output Key
		job1.setMapOutputValueClass(Text.class); // Class for Mapper Output Value

		
		job1.setOutputFormatClass(TextOutputFormat.class);
		job1.getConfiguration().set("mapred.textoutputformat.separator", "#&#");

		job1.setOutputKeyClass(Text.class); // Class for Reducer Output Key
		job1.setOutputValueClass(Text.class); // Class for Reducer Output Value
		job1.setNumReduceTasks(1); // Enforcing single reducer to avoid counter synchronization issue
		
		//Following line is to stop generating _SUCCESS file
		job1.getConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
		
		int status = job1.waitForCompletion(true) ? 0 : 1;
		if (status == 0) {
			/*
			 * This job is to form the out-links graph
			 */

			long nodeCount = job1.getCounters().findCounter("Val", "cntVal").getValue(); // Number of links found is taken as total page count
																							
			System.out.println("Number of Nodes: " + nodeCount);
			Job job = Job.getInstance(getConf(), " link_graph ");
			job.getConfiguration().set("nodeCount", "" + nodeCount); // Setting links count to job configuration
			job.setJarByClass(this.getClass());
			FileInputFormat.addInputPaths(job, args[0]); // Path to Wiki File
			FileOutputFormat.setOutputPath(job, new Path(args[0] + "_rank0")); // Path for Linkgraph with inital ranks
			job.setMapperClass(LinkMap.class);
			job.setReducerClass(LinkReduce.class);
			job.setMapOutputKeyClass(Text.class); // Class for Mapper Output Key
			job.setMapOutputValueClass(Text.class); // Class for Mapper Output Value
			job.setOutputFormatClass(TextOutputFormat.class);
			job.getConfiguration().set("mapred.textoutputformat.separator", "#&#&SEP#&#&");// Customized Key-Value seperator
			job.setOutputKeyClass(Text.class); // Class for Reducer Output Key
			job.setOutputValueClass(Text.class); // Class for Reducer Output Value
			
			//Following line is to stop generating _SUCCESS file
			job.getConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
			
			return job.waitForCompletion(true) ? 0 : 1;
		} else
			return 1;
	}

	

	public static class LinkMap extends Mapper<LongWritable, Text, Text, Text> {          // mapper to create link graph. Takes lines from input file. output is link as key and initial rank and
		                                                                                  // outlinks as value

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {


			Pattern title = Pattern.compile("<title>(.*?)</title>"); // Pattern to search for source URL
			Pattern outlinks = Pattern.compile("\\[\\[.*?\\]\\]"); // Pattern to search out-links

			String line = lineText.toString(); // Line retrieved from input file
			line = line.trim(); //leading and tailing whit spaces are trimmed.

			Matcher titleMatcher = title.matcher(line); // Source URL pattern set to a matcher
			Matcher outlinkMatcher = outlinks.matcher(line);// Target outlinks pattern set to matcher

			String titlePage = null;
			if (titleMatcher.find()) {
				titlePage = titleMatcher.group().trim();
				titlePage = titlePage.substring(7, titlePage.length() - 8); // Find the title page name between the <title> tags
			}
			else{
				return; // If does not find title page, return
			}
				

			String outLinkList = ""; // String declared to store the list of outlinks
			int i = 0;
			
			while (outlinkMatcher.find()) {
				String link = outlinkMatcher.group().trim();
				link = link.replace("[[", "").replace("]]", ""); // Replace the unwanted [[ and ]] inside an out-link
				if(!link.isEmpty()){
				if (i == 0)
					outLinkList = outLinkList + link;
				else
					outLinkList = outLinkList + "###&&&&&&###" + link;	// Building the target URL list using a special seperator
				
				i++;
				}
			}
			
			context.write(new Text(titlePage), new Text(outLinkList));
		}
	}

	
	public static class LinkReduce extends Reducer<Text, Text, Text, Text> {
		long nodeCnt; // Total Number wiki pages
		double initialRank; // Rank all nodes are initialized with
		
		
		
		protected void setup(Context context) throws IOException, InterruptedException {
			nodeCnt = context.getConfiguration().getLong("nodeCount", 1); // setup function reads no of nodes set to configuration 
			initialRank = 1.0 / nodeCnt;
		}
		
		
		@Override
		public void reduce(Text word, Iterable<Text> outlinks, Context context)
				throws IOException, InterruptedException {
			StringBuilder stringBuild = new StringBuilder();
			
			for (Text link : outlinks) { 
				stringBuild.append(link.toString());
					stringBuild.append("###&&&&&&###"); // all the duplicate links are combines and seperated by special string
			}
			
			context.write(word, new Text(initialRank + "#&#&RSEP#&#&" + stringBuild.toString()));
		}
	}

	
	
	public static class CountMap extends Mapper<LongWritable, Text, Text, Text> {                    // Mapper gets total links count . output is link as key and value is one.

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			
			Pattern title = Pattern.compile("<title>(.*?)</title>"); //Pattern to search for source URL
			Pattern outlink = Pattern.compile("\\[\\[.*?\\]\\]"); //Pattern to search for Target URLs

			String line = lineText.toString();
			line = line.trim();

			Matcher m1 = title.matcher(line); // Source URL pattern set to its matcher
			Matcher m2 = outlink.matcher(line); // Target URL pattern set to its matcher

			while (m1.find()) {
				String pg = m1.group().trim();
				pg = pg.substring(7, pg.length() - 8); // Find the source URL between <input> tags
				context.write(new Text(pg), new Text("1")); // Source URL written as output
			}
			while (m2.find()) {
				String pg1 = m2.group().trim();
				pg1 = pg1.replace("[[", "").replace("]]", "");; // Replace unwanted [[ and ]] in matched target tags
				if(!pg1.isEmpty())
					context.write(new Text(pg1), new Text("1")); // Target URL written as output
			}

		}
	}

	
	public static class CountReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> outlinks, Context context)
				throws IOException, InterruptedException {

			context.getCounter("Val", "cntVal").increment(1); // To count the total number of links, I have used a counter. Every time a new link is found, the counter is incremented by 1.
			                                                  // In this reducer job, nothing is being written to an output file to avoid file writing overhead.
			

		}
	}

}