brew install mvnvm (just to install maven on mac)

make a eclipse maven project on your local (File -> New -> Project -> Maven Project). During the setup just click next until you run into a place that prompt you to set the group id = com.javamakeuse.hadoop.poc (it turns out you can name it whatever you want), artifact id = Homeworkx (name is whatever you want, e.g. Homework1)

copy the pom.xml from wolf and replace the local pom.xml (you'll see it on your left in eclipse)

go to src/main/java and start a new class (e.g. Exercise1) to do your coding

after we're done coding, navigate to where the maven project is stored (e.g. mine is stored under /Users/ethen/Documents/workspace/Homework1) and type mvn package to create the jar file

After that copy the mr-app-1.0-SNAPSHOT.jar inside the target folder to wolf.

Then ssh to wolf and run the job on wolf using hadoop jar <name of jar file> <name of class with main()> <input files> <output directory> e.g. for the wordcount example I had a folder called wordcount on hadoop and I want the output folder to be called output, thus I ran hadoop jar mr-app-1.0-SNAPSHOT.jar com.javamakeuse.hadoop.poc.Homework1.Exercise1 wordcount output. For the class name remember to copy the full path from eclipse (look at the highlighted section in the screenshot below)

After that we can do hdfs dfs -cat outFolder/* to look at result, or use hdfs dfs -getmerge <output directory>/ output.txt, where the output.txt will be the merged result, again name this whatever you want



