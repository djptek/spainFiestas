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

class CoordinatesLookup {

    private static Logger log = LogManager.getLogger(App.class);
    private static String COORDINATES_BULK_NDJSON = "src/main/resources/municipios.coordinates.ndjson";
    private boolean IndexLoaded = false;

    void loadIndex (RestHighLevelClient client) throws IOException {
        Scanner scanner = new Scanner(new File(COORDINATES_BULK_NDJSON)).useDelimiter("\n");

        BulkRequest bulkRequest = new ScanToBulk(
                scanner,
                new BulkRequest(),
                Index.MUNICIPIO_VS_COORDS,
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
            log.printf(Level.ERROR, "Bulk index failed for Index "+Index.MUNICIPIO_VS_COORDS);
        } else {
            IndexLoaded = true;
        }
    }

    boolean indexLoaded() {
        return IndexLoaded;
    }
}
