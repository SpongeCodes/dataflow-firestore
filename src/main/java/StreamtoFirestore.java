import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.WriteResult;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;

import com.google.cloud.firestore.Firestore;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

import java.io.IOException;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;


import com.fasterxml.jackson.databind.ObjectMapper;

public class StreamtoFirestore {
    public static class VehicleData {
        private String vehicle_id;
        private double latitude;
        private double longitude;
        private double temp;
        private long timestamp;

        // getters and setters


        public String getVehicle_id() {
            return vehicle_id;
        }

        public void setVehicle_id(String vehicle_id) {
            this.vehicle_id = vehicle_id;
        }

        public double getLatitude() {
            return latitude;
        }

        public void setLatitude(double latitude) {
            this.latitude = latitude;
        }

        public double getLongitude() {
            return longitude;
        }

        public void setLongitude(double longitude) {
            this.longitude = longitude;
        }

        public double getTemp() {
            return temp;
        }

        public void setTemp(double temp) {
            this.temp = temp;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public Map<String, Object> toMap() {
            Map<String, Object> firestoreSchemaMap = new HashMap<>();
            firestoreSchemaMap.put("vehicle_id", vehicle_id);
            firestoreSchemaMap.put("latitude", latitude);
            firestoreSchemaMap.put("longitude", longitude);
            firestoreSchemaMap.put("temp", temp);
            ZoneId zone = ZoneId.of("Asia/Kolkata");
            firestoreSchemaMap.put("timestamp", Instant.ofEpochMilli(timestamp).atZone(zone).toString());
            return firestoreSchemaMap;
        }
    }

    // Convert JSON string to VehicleData
    private static VehicleData convertToVehicleData(String json) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(json, VehicleData.class);
        } catch (IOException e) {
            // log errors -> replace later
            System.out.println("Error!");
            return null;
        }
    }

    public static class WriteToFirestoreDoFn extends DoFn<String, Void> {
        private transient Firestore firestore;

        @Setup
        public void setup() {
            firestore = FirestoreOptions.getDefaultInstance().getService();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String json = c.element();
            System.out.println(json);
            VehicleData data = convertToVehicleData(json);

            if(data!=null) {

                // Populate the Firestore map with the data
                Map<String, Object> firestoreData = data.toMap();

                // Generate a unique document ID using UUID
                String uniqueDocumentId = UUID.randomUUID().toString();

                // Define Firestore document path with unique ID
                String documentPath = "vehicle-tracking/" + uniqueDocumentId;
                // Firestore write operation
                ApiFuture<WriteResult> future = firestore.document(documentPath).set(firestoreData);

                // Handle asynchronous result
                ApiFutures.addCallback(future, new ApiFutureCallback<WriteResult>() {
                    @Override
                    public void onFailure(Throwable t) {
                        // Log the error
                        System.err.println("Error writing to Firestore: " + t.getMessage());
                    }

                    @Override
                    public void onSuccess(WriteResult result) {
                        // Optionally log success
                        System.out.println("Successfully wrote to Firestore: " + result.getUpdateTime());
                    }
                }, MoreExecutors.directExecutor());
            }
        }
    }

    public static void main(String[] args) throws IOException {

        // can extend this to create a more generic pipeline with command line arguments
        // arguments for local testing/cloud testing , maven profiles
        // create an options class
        /*PipelineOptions myPipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

*/
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setRunner(DataflowRunner.class);
        options.setProject("clean-doodad-428805-i9");
        options.setRegion("asia-east1");
        options.setStreaming(true);
        options.setGcpCredential(GoogleCredentials.getApplicationDefault());
        //options.setTempLocation("gs://dataflow-upamanyu/temp/");
        Pipeline p = Pipeline.create(options);
        // read from pub/sub and process from dataflow and send to firestore

        // Step 1. subscribe from pub/sub pull all unacknowledged messages
        p.apply("Read from Pub/Sub IOT Topic", PubsubIO.readStrings().fromTopic("projects/clean-doodad-428805-i9/topics/iot"))

                // Step 2. divide into windows of size 1 minute
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))

        // save to firestore asynchronously
        .apply(" Transform and Write to Firestore DB",ParDo.of(new WriteToFirestoreDoFn()));

        // run dataflow job

        p.run().waitUntilFinish();
    }

}
