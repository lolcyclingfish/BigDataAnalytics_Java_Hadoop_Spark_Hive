# BigDataAnalytics_Java_Hadoop_Spark_Hive

How to set up a maven project in on Eclipse: credit to (E. Liu)

brew install mvnvm (just to install maven on mac)
make a eclipse maven project on your local (File -> New -> Project -> Maven Project). During the setup just click next until you run into a place that prompt you to set the group id = com.javamakeuse.hadoop.poc (it turns out you can name it whatever you want), artifact id = Homeworkx (name is whatever you want, e.g. Homework1)
copy the pom.xml from wolf and replace the local pom.xml (you'll see it on your left in eclipse)
go to src/main/java and start a new class (e.g. Exercise1) to do your coding
after we're done coding, navigate to where the maven project is stored (e.g. mine is stored under /Users/ethen/Documents/workspace/Homework1) and type mvn package to create the jar file
After that copy the mr-app-1.0-SNAPSHOT.jar inside the target folder to wolf.
Then ssh to wolf and run the job on wolf using hadoop jar <name of jar file> <name of class with main()> <input files> <output directory> e.g. for the wordcount example I had a folder called wordcount on hadoop and I want the output folder to be called output, thus I ran hadoop jar mr-app-1.0-SNAPSHOT.jar com.javamakeuse.hadoop.poc.Homework1.Exercise1 wordcount output. For the class name remember to copy the full path from eclipse (look at the highlighted section in the screenshot below)
After that we can do hdfs dfs -cat outFolder/* to look at result, or use hdfs dfs -getmerge <output directory>/ output.txt, where the output.txt will be the merged result, again name this whatever you want

How to set up cluster and run job on AWS: (credit to A. Liu)

Go to the S3 Management Console https://console.aws.amazon.com/s3/home?region=us-east-2
Create a new bucket, I named mine aliuhomework2 but you can name it whatever you want. Make sure the region is US East (Ohio). Don't change any of the other settings.
Click on the the bucket name once you've created it and upload your jar file.
Go to the EMR Console https://us-east-2.console.aws.amazon.com/elasticmapreduce/home?region=us-east-2
Make sure you are on Ohio!!!!
Create a new cluster. You can change the S3 folder to the one you just created but I don't think it affects anything if you don't (?).
Under hardware configuration change the type of cluster & number of instances depending on what you want.
Select your EC2 keypair. If you don't have one there's instructions to create one on the page. Create your cluster.
Click Add Step. Under jar location click the folder icon & select the jar you uploaded.
Under arguments this is where you put in the args you normally pass to the hadoop jar or yarn jar method.
For HW2 the arguments I used for 1gram/2gram data was com.javamakeuse.hadoop.poc.Homework2.Exercise2 s3://msiahw2/google/googlebooks-eng-all-1gram-20120701-n s3://msiahw2/google/googlebooks-eng-all-2gram-20120701-6 s3://aliuhomework2/output2. Edit it to fit the names of your files/folders.
For HW2 the arguments I used for the music data was com.javamakeuse.hadoop.poc.Homework2.Exercise4 s3://msiahw2/music/dataMusic10000.csv s3://aliuhomework2/output4. Edit it to fit the names of your files/folders.
Add the step
If you want to monitor the progress, scroll down & expand Steps, click View logs & click syslog. It will show your map % and reduce %.
Once it's completed the results will be in your S3 folder.
