package demos;

import org.apache.commons.cli.*;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Scanner;

/*
 * Setup for 2019 Fiestas mapping demo
 */
public class App {

    private static Logger log = LogManager.getLogger(App.class);
    private static String FIESTAS_BULK_NDJSON = "src/main/resources/fiestas.comunidad.js";

    private static String HOSTNAME = "localhost";
    private static String SCHEME = "http";
    private static String USERNAME = "elastic";
    private static String PASSWORD = "password";
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

    static private Options getOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "Show this help");
        options.addOption("p", "password", true, "password default <" + PASSWORD + ">");
        options.addOption("u", "username", true, "username default <" + USERNAME + ">");
        options.addOption("H", "host", true, "Host default <" + HOSTNAME + ">");
        options.addOption("P", "port", true, "Port default <" + PORT + ">");
        options.addOption("S", "scheme", true, "Scheme [http|https] default <" + SCHEME + ">");
        return options;
    }

    static private void setOptions(CommandLine line) {
        if (line.hasOption("p")) PASSWORD = line.getOptionValue("p");
        if (line.hasOption("u")) USERNAME = line.getOptionValue("u");
        if (line.hasOption("H")) HOSTNAME = line.getOptionValue("H");
        if (line.hasOption("P")) PORT = Integer.valueOf(line.getOptionValue("P"));
        if (line.hasOption("S")) SCHEME = line.getOptionValue("S");
    }

    static private void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(System.getProperty("java.class.path"), options);
    }

    public static void main(String[] args) throws java.io.IOException, ParseException {

        boolean acknowledged;

        if (args.length > 0) {
            Options options = getOptions();
            CommandLine line = new DefaultParser().parse(options, args);
            if (line.hasOption("h")) {
                printHelp(options);
                return;
            } else {
                setOptions(line);
            }
        }

        // connect to Elasticsearch
        final CredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();

        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(USERNAME, PASSWORD));

        //Open connection to Elasticsearch
        log.printf(Level.INFO, "fiestamapper demo data loader started");
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(HOSTNAME, PORT, SCHEME))
                        .setHttpClientConfigCallback(
                                httpClientBuilder -> httpClientBuilder
                                        .setDefaultCredentialsProvider(credentialsProvider)));

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


