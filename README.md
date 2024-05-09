# Sarcasm_Detection
Instructions (run in the localUp directory!):

1. put the input files inside the "target" dir.
2. make a jar file out of the local Up project.
3. run java -jar jarFile.jar args

OR

run it locally and put the args in the main function in app and call the LocalUp.

Gameplay:

Local-up:
1. initialization - creates if not exist the bucket, the queues and the manager.
2. uploade the input files to s3 -> ordered by file indexes and client id.
3. send a msg to the queue: LocalsToManager.
4. it creates the manager if it falls and wait for it response.
5. when get the response he creates the summery file.

Manager:
1. check if there is a backUp file in S3
2. creates the queues.
3. have 2 threads->1 listen to locals, 2 listen to workers.

1 listen to locals:
waits for a msg from a local up 
and distributes the file to mini files that each has n/2 reviews.
upload files to S3 creates workers accordingly and send them msgs.
it creates an average number of workers that depends on the total 
files we recieved and the avg N.

2 listen to workers:
waits for a msg from workers and creates the summery file accordingly and send
it to the wanted local-up.

Worker:
1. waits for a msg from the Manager.
2. process the msg and upload the done-files.
3. if it got a terminate msg it dies.

TYPES:
AMI_ID = "ami-0a3c3a20c09d6f377"
InstanceType.M4_LARGE
