package demos;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

class MunicipalityFiestasLoader {

    private static Logger logger = LogManager.getLogger(App.class);
    private static BulkResponseDumper bulkResponseDumper = new BulkResponseDumper(logger);
    private static String MUNICIPIOS_BULK_NDJSON = "src/main/resources/provincias.valencia.fiestasmunicipales.ndjson";

    private static File file = new File(MUNICIPIOS_BULK_NDJSON);

    private static DataLoader dataLoader;
    private static RestHighLevelClient client;
    private static BulkRequest bulkRequest = new BulkRequest();

    private static String PROVINCIA_KEY = "provincia";
    private static String MUNICIPIO_KEY = "municipio";
    private static String DATE_KEY = "date";
    private static String FIESTA_KEY = "fiesta";
    private static String NAME_KEY = "name";
    private static String LAT_KEY = "lat";
    private static String LON_KEY = "lon";
    private static String LOCATION_KEY = "location_geo_point";

    private static DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ofPattern("dd-MM-yyyy"))
            .toFormatter();

    MunicipalityFiestasLoader(RestHighLevelClient client) {
        this.client = client;
        this.dataLoader = new DataLoader(client, file, logger);
    }

    void load() throws IOException {
        Scanner scanner = new Scanner(file).useDelimiter("\n");

        while (scanner.hasNext()) {
            String nextLine = scanner.nextLine();
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(
                            NamedXContentRegistry.EMPTY,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            nextLine);
            Map<String, Object> requestMap = parser.map();

            logger.printf(Level.INFO, "Municipality " + requestMap.get(MUNICIPIO_KEY).toString());

            List<QueryBuilder> clauses = new ArrayList<>();
            clauses.add(QueryBuilders.matchQuery(PROVINCIA_KEY, requestMap.get(PROVINCIA_KEY).toString()));
            clauses.add(QueryBuilders.matchQuery(MUNICIPIO_KEY, requestMap.get(MUNICIPIO_KEY).toString()));
            BoolQueryBuilder query = new BoolQueryBuilder();
            query.filter().addAll(clauses);

            SearchRequest searchRequest = new SearchRequest()
                    .indices(Index.MUNICIPIO_VS_COORDS.lowerCaseString)
                    .source(new SearchSourceBuilder().query(query));

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            SearchHits searchHits = searchResponse.getHits();

            TotalHits totalHits = searchHits.getTotalHits();
            long numHits = totalHits.value;

            if (numHits < 1) {
                logger.printf(Level.WARN,
                        "No Match for Municipality " + requestMap.get(MUNICIPIO_KEY).toString());
            } else {
                logger.printf(Level.INFO,
                        "Found Unique Match for Municipality " + requestMap.get(MUNICIPIO_KEY).toString());
                SearchHit[] hit = searchHits.getHits();
                Map<String, Object> sourceAsMap = hit[0].getSourceAsMap();
                logger.printf(Level.DEBUG,
                        "lon, lat => [%s, %s] ",
                        sourceAsMap.get(LON_KEY).toString(),
                        sourceAsMap.get(LAT_KEY).toString());
                try {
                    //.id(requestMap.get(MUNICIPIO_KEY).toString() + ":" + requestMap.get(DATE_KEY).toString())
                    IndexRequest indexRequest = new IndexRequest(
                            Index.MUNICIPIO_VS_FIESTAS.lowerCaseString)
                            .source(XContentFactory.jsonBuilder()
                                    .startObject()
                                    .field(PROVINCIA_KEY, requestMap.get(PROVINCIA_KEY).toString())
                                    .startObject(FIESTA_KEY)
                                    .field(DATE_KEY,
                                            LocalDate.parse(
                                                    requestMap.get(DATE_KEY).toString(),
                                                    dateTimeFormatter).toString())
                                    .field(NAME_KEY, requestMap.get(FIESTA_KEY).toString())
                                    .endObject()
                                    .startObject(MUNICIPIO_KEY)
                                    .field(NAME_KEY, requestMap.get(MUNICIPIO_KEY).toString())
                                    .field(LOCATION_KEY, new double[]{
                                            Double.parseDouble(sourceAsMap.get(LON_KEY).toString()),
                                            Double.parseDouble(sourceAsMap.get(LAT_KEY).toString())})
                                    .endObject()
                                    .endObject());
                    logger.printf(Level.DEBUG, "Index Request is " + indexRequest.toString());
                    bulkRequest.add(indexRequest);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        scanner.close();
        BulkResponse bulkResponse = dataLoader.load(bulkRequest);
        bulkResponseDumper.dump(bulkResponse);
    }
}
