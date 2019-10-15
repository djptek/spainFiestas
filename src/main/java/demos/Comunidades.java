package demos;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

class Comunidades {

    private static Logger log = LogManager.getLogger(App.class);

    static void load(RestHighLevelClient client, File file) throws IOException {
        // read Community Fiestas data into a new Bulk Request
        Scanner scanner = new Scanner(file).useDelimiter("\n");
        BulkRequest bulkRequest = new ScanToBulk(
                scanner,
                new BulkRequest(),
                Index.FIESTAS_VS_COMUNIDAD_AUTONOMA,
                true)
                .getBulkRequest();
        scanner.close();

        // Send the bulk request
        bulkRequest.setRefreshPolicy(org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL);

        BulkResponse bulkResponse = client.bulk(bulkRequest,
                RequestOptions.DEFAULT);

        for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
            log.printf(Level.INFO, "Bulk Operation [%s] id [%s] to [%s] status [%s]",
                    bulkItemResponse.getOpType(),
                    bulkItemResponse.getId(),
                    bulkItemResponse.getIndex(),
                    bulkItemResponse.status());
        }

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
                for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
                    log.printf(Level.INFO, "Bulk Operation [%s] id [%s] to [%s] status [%s]",
                            bulkItemResponse.getOpType(),
                            bulkItemResponse.getId(),
                            bulkItemResponse.getIndex(),
                            bulkItemResponse.status());
                }

            } else {
                log.printf(Level.WARN, "Failed to Populate Bulk Request");
            }
        }

    }
}
