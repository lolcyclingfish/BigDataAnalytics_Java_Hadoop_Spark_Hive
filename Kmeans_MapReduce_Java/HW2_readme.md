
AWS Instructions (using the add step method, not ssh)

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
