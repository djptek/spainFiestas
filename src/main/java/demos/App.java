package demos;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.File;
import java.util.Scanner;

/*
 * Setup for 2019 Fiestas mapping demo
 */
public class App {

    private static Logger log = LogManager.getLogger(App.class);
    private static String HOSTNAME = "localhost";
    private static String SCHEME = "http";
    private static String FIESTAS_BULK_NDJSON = "src/main/resources/fiestas.js";

    private static int PORT = 9200;

    private static void dumpBulkResponse(BulkResponse bulkResponse) {
        for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
            log.printf(Level.INFO, "Bulk Operation [%s] id [%s] to [%s] status [%s]",
                    bulkItemResponse.getOpType(),
                    bulkItemResponse.getId(),
                    bulkItemResponse.getIndex(),
                    bulkItemResponse.status());
        }
    }

    public static void main(String[] args) throws java.io.IOException {

        boolean acknowledged;

        //Open connection to Elasticsearch
        log.printf(Level.INFO, "fiestamapper demo data loader started");
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(HOSTNAME, PORT, SCHEME)));

        //Add a pipeline to coerce document ID to the value of field fiesta
        acknowledged = ScriptPipeline.put(ScriptPipeline.FIESTA_TO_ID, client);
        log.printf(Level.INFO, "Pipeline response acknowledged [%s]", acknowledged);

        //tidy/create indices
        for (Index index : Index.values()) {
            if (Index.exists(index, client)) Index.delete(index, client);
            acknowledged = Index.create(index, client);
            log.printf(Level.INFO, "Index creation [%s] acknowledged [%s]",
                    index, acknowledged);
        }

        // read data into a new Bulk Request
        Scanner scanner = new Scanner(new File(FIESTAS_BULK_NDJSON)).useDelimiter("\n");
        BulkRequest bulkRequest = new ScanToBulk(
                scanner,
                new BulkRequest(),
                Index.FIESTAS_VS_COMUNIDAD_AUTONOMA)
                .getBulkRequest();
        scanner.close();

        // Send the bulk request
        bulkRequest.setRefreshPolicy(org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL);

        BulkResponse bulkResponse = client.bulk(bulkRequest,
                RequestOptions.DEFAULT);

        dumpBulkResponse(bulkResponse);

        if (bulkResponse.hasFailures()) {
            log.printf(Level.ERROR, "Bulk index failed, abandoning");
        } else {
            log.printf(Level.INFO, "Bulk index OK, denormalizing");
            // refactor to new bulk request
            BulkRequest remappedBulkRequest = new AggsToBulk(
                    Index.FIESTAS_VS_COMUNIDAD_AUTONOMA,
                    Index.COMUNIDAD_AUTONOMA_VS_FIESTAS,
                    client,
                    new BulkRequest()
            ).getBulkRequest();

            // check bulk count to ensure at least 1 action was added
            if (remappedBulkRequest.numberOfActions() > 0) {
                bulkResponse = client.bulk(remappedBulkRequest,
                        RequestOptions.DEFAULT);
                dumpBulkResponse(bulkResponse);
            } else {
                log.printf(Level.WARN, "Failed to Populate Bulk Request");
            }
        }

        client.close();

    }


}


