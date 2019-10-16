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
    private static Logger logger = LogManager.getLogger(App.class);
    private static BulkResponseDumper bulkResponseDumper = new BulkResponseDumper(logger);
    private static String filename = "src/main/resources/kibana_1_bulk.ndjson";
    private static File file = new File(filename);
    private static DataLoader dataLoader;

    KibanaObjectsLoader(RestHighLevelClient client) {
        this.dataLoader = new DataLoader(client, file, logger);
    }

    static void load() throws IOException {
        dataLoader.load();
    }

}
