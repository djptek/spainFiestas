package demos;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

class KibanaObjectsLoader {
    private static Logger log = LogManager.getLogger(App.class);
    private static BulkResponseDumper bulkResponseDumper = new BulkResponseDumper(log);

    static void load(RestHighLevelClient client, File file) throws IOException {
    // now add Kibana Objects read data into a new Bulk Request
        Scanner scanner = new Scanner(file).useDelimiter("\n");
        BulkRequest bulkRequest = new ScanToBulk(
                scanner,
                new BulkRequest())
                .getBulkRequest();
        scanner.close();

        // Send the bulk request
        bulkRequest.setRefreshPolicy(org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL);

        BulkResponse bulkResponse = client.bulk(bulkRequest,
                RequestOptions.DEFAULT);

        bulkResponseDumper.dump(bulkResponse);

        if (bulkResponse.hasFailures()) {
            log.printf(Level.ERROR, "Bulk index failed for Kibana Objects");
        } else {
            log.printf(Level.INFO, "Bulk index OK for Kibana Objects");
        }            // refactor to new bulk request
    }
}
