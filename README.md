# PageRank-Implementation
THe page rank implementation contains 4 classes :
1. LinkGraph.java  
2. PageRank.java  
3. CleanUp.java
4. RankDRiver.java

The goal of this assignment is to implement the Page Rank algorithm on a mini wiki datset that is a xml document which is preprocessed 
to contain titles and outlinks.
The LinkGraph.java takes in the input text file and extracts the titles and outlinks from the document using Matchers and Patterns. 
The linkMap mapper class extracts the title and outlinks from each line and creates an outlinkList that contains all the outlinks 
seperated by a seperator. THe mapper writes out the title as the key and outlink list as the value pair. The reducer calculates the 
initial rank which is nothing but 1/(total no of nodes) and appends it to the outlinkList with a seperator. THe countmap mapper does 
the same thing as linkMap but appends text 1 to each title and outlink and writes it toreducer. The linkReduce counts all the outlinks
and generates a counter.

THe PageRank.java takes in the output from the LinkGraph.java. The pageRankMap seperates the initial rank from the string and writes the
new rank which is old rank divided by no of outlinks to that node. The PageRankReduce reducer calculates the final rank by using the 
formula for PageRank. The damping factor is taken as 0.85. THe reducer writes out the title as key and rank as value pairs, and also 
attaches out links from that page for calculating other PageRanks. 

The CleanUp.java cleans up all the intermediate files and also success files. It also sorts the pages based on their page rank using a 
custom sorter.The reducer sorts the pages based on their rank.

The RankDriver class contains the main class and all the other java classes are called from here. It iterates 10 times over the page
rank file. 

Execution:
1. The jar file(PageRank.jar) is included in the submission. It can directly be used for execution.
2. Execute the jar-file using the command-   hadoop jar PageRank.jar PageRank.RankDiver <Input Directory> <Output Directory>
3. Here the input directory is simple-micro-wiki. Path to the file needs to be given. Output directory is the directory where output
needs to be stored.
or---------------------------------------------------------------------------------------------------------------------------------------------------------
1.Open the project in eclipse or any other IDE.
2.Right click on the project and go to Properties->Run/Debug settings->New.
3.In the main tab give the Project as PageRank and the main class as RankDriver.java. In the arguments tab give the arguments as the 
path for the input and output directories in double quotes. And do not forget to apply the settings.
4. Run the RankDriver.java class as it contains the main function.   
