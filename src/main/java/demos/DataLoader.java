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

class DataLoader {

    static Logger logger;
    static File file;
    static BulkResponseDumper bulkResponseDumper;
    static RestHighLevelClient client;

    DataLoader (RestHighLevelClient client, File file, Logger logger) {
        this.logger = logger;
        this.file = file;
        this.client = client;
        this.bulkResponseDumper = new BulkResponseDumper(logger);
    }

    private static BulkResponse sendBulk(RestHighLevelClient client, BulkRequest bulkRequest) throws IOException {
        // Send the bulk request
        bulkRequest.setRefreshPolicy(org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL);

        BulkResponse bulkResponse = client.bulk(bulkRequest,
                RequestOptions.DEFAULT);

        bulkResponseDumper.dump(bulkResponse);

        if (bulkResponse.hasFailures()) {
            logger.printf(Level.ERROR, "Bulk index failed for "+file.getName());
        } else {
            logger.printf(Level.INFO, "Bulk index OK for "+file.getName());
        }
        return bulkResponse;
    }


    static BulkResponse load(Index index) throws IOException {
        BulkRequest bulkRequest = new FileToBulkHelper(
                file,
                new BulkRequest(),
                index).getBulkRequest();

        return sendBulk(client, bulkRequest);
    }

    static BulkResponse load() throws IOException {
        BulkRequest bulkRequest = new FileToBulkHelper(
                file,
                new BulkRequest())
                .getBulkRequest();

        return sendBulk(client, bulkRequest);
    }

    static BulkResponse load(BulkRequest bulkRequest) throws IOException {
        return sendBulk(client, bulkRequest);
    }

}

