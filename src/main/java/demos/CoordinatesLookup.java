package demos;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

class CoordinatesLookup {

    private static Logger log = LogManager.getLogger(App.class);
    private static String COORDINATES_BULK_NDJSON = "src/main/resources/municipios.coordinates.ndjson";
    private volatile boolean indexLoaded = false;
    private RestHighLevelClient client;

    CoordinatesLookup(RestHighLevelClient client) {
        this.client = client;
    }

    void loadIndex () throws IOException {
        if (indexLoaded) {
            log.printf(Level.ERROR, "Tried to reload " + COORDINATES_BULK_NDJSON + " ignoring request");
        } else {
            MunicipalityCoordinatesLoader municipalityCoordinatesLoader = new MunicipalityCoordinatesLoader(client);
            municipalityCoordinatesLoader.load();
            indexLoaded = true;
        }
    }

    boolean indexLoaded() {
        return indexLoaded;
    }
}
