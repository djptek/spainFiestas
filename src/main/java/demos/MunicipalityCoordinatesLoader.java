package demos;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.File;
import java.io.IOException;

class MunicipalityCoordinatesLoader {
    private static Logger logger = LogManager.getLogger(App.class);
    private static String filename = "src/main/resources/municipios.coordinates.ndjson";
    private static File file = new File(filename);
    private static DataLoader dataLoader;

    private static RestHighLevelClient client;

    MunicipalityCoordinatesLoader(RestHighLevelClient client) {
        this.client = client;
        this.dataLoader = new DataLoader(client, file, logger);
    }

    void load() throws IOException {
        dataLoader.load(Index.MUNICIPIO_VS_COORDS);
    }
}
