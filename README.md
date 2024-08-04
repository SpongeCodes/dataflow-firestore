To RUN WITH COMMAND LINE ARGUMENTS

-- USE mvn compile exec:java \
-P PROFILE_NAME \
-Dexec.mainClass=$MAIN_CLASS_NAME \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=$PROJECT_ID \
--region=$REGION \
--stagingLocation=$BUCKET/stage \
--tempLocation=$BUCKET/temp \
--inputFile=$BUCKET/input/input.csv \
--outputFile=$BUCKET/output/output.csv \
--runner=$RUNNER"
