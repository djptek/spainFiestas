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

class CommunityLoader {

    private static Logger logger = LogManager.getLogger(App.class);
    private static BulkResponseDumper bulkResponseDumper = new BulkResponseDumper(logger);
    private static String[] filenames = {
            "src/main/resources/2019.fiestas.comunidad.ndjson",
            "src/main/resources/2020.fiestas.comunidad.ndjson"};
    private static DataLoader dataLoader;
    private static RestHighLevelClient client;

    CommunityLoader (RestHighLevelClient client) {
        this.client = client;
        for (String filename : filenames) {
            File file = new File(filename);
            this.dataLoader = new DataLoader(client, file, logger);
        }
    }

    void load() throws IOException {
        // read Community Fiestas data into a new Bulk Request
        BulkResponse bulkResponse = dataLoader.load(Index.FIESTAS_VS_COMUNIDAD_AUTONOMA);

        if (bulkResponse.hasFailures()) {
            logger.printf(Level.ERROR, "Bulk index failed, abandoning");
        } else {
            logger.printf(Level.INFO, "Bulk index OK, denormalizing");
            // refactor to new bulk request
            BulkRequest remappedBulkRequest = new CommunityFiestasDenormalizer(
                    Index.FIESTAS_VS_COMUNIDAD_AUTONOMA,
                    Index.COMUNIDAD_AUTONOMA_VS_FIESTAS,
                    client,
                    new BulkRequest()
            ).getBulkRequest();

            // check bulk count to ensure at least 1 action was added
            if (remappedBulkRequest.numberOfActions() > 0) {
                bulkResponse = client.bulk(remappedBulkRequest,
                        RequestOptions.DEFAULT);
                bulkResponseDumper.dump(bulkResponse);
            } else {
                logger.printf(Level.WARN, "Failed to Populate Bulk Request");
            }
        }

    }
}
