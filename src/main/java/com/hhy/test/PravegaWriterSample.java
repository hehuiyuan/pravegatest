package com.hhy.test;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class PravegaWriterSample {
    public final String scope;
    public final String streamName;
    public final URI controllerURI;
    public EventStreamWriter<String> writer;

    public PravegaWriterSample(String scope, String streamName, URI controllerURI) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
    }

    public void open() {
        EventStreamClientFactory clientFactory =
                EventStreamClientFactory.withScope(
                        scope, ClientConfig.builder().controllerURI(controllerURI).build());
        writer =
                clientFactory.createEventWriter(
                        streamName,
                        new UTF8StringSerializer(),
                        EventWriterConfig.builder().build());
    }

    public void run(String routingKey, String message) {
        StreamManager streamManager = StreamManager.create(controllerURI);
        final boolean scopeIsNew = streamManager.createScope(scope);

        StreamConfiguration streamConfig =
                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();

        boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);

        System.out.format(
                "Writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
                message, routingKey, scope, streamName);

        // final CompletableFuture<Void> writeFuture = writer.writeEvent(routingKey, message);
        final CompletableFuture<Void> writeFuture = writer.writeEvent(message);
        while (writeFuture.isDone()) {
            try {
                System.err.println("write result " + writeFuture.get());
                TimeUnit.SECONDS.sleep(1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        final Options options = getOptions();

        CommandLine cmd = null;

        try {
            cmd = parseCommandLineArgs(options, args);
        } catch (ParseException e) {
            System.err.format("%s.%n", e.getMessage());
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("HelloWorldWriter", options);
            System.exit(1);
        }

        final String scope =
                cmd.getOptionValue("scope") == null
                        ? Constants.DEFAULT_SCOPE
                        : cmd.getOptionValue("scope");
        final String streamName =
                cmd.getOptionValue("name") == null
                        ? Constants.DEFAULT_STREAM_NAME
                        : cmd.getOptionValue("name");
        final String uriString =
                cmd.getOptionValue("uri") == null
                        ? Constants.DEFAULT_CONTROLLER_URI
                        : cmd.getOptionValue("uri");
        final URI controllerURI = URI.create(uriString);

        PravegaWriterSample pws = new PravegaWriterSample(scope, streamName, controllerURI);

        final String routingKey =
                cmd.getOptionValue("routingKey") == null
                        ? Constants.DEFAULT_ROUTING_KEY
                        : cmd.getOptionValue("routingKey");
        final String message =
                cmd.getOptionValue("message") == null
                        ? Constants.DEFAULT_MESSAGE
                        : cmd.getOptionValue("message");

        pws.open();
        while (true) {
            pws.run(routingKey, message);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("s", "scope", true, "The scope name of the stream to read from.");
        options.addOption("n", "name", true, "The name of the stream to read from.");
        options.addOption(
                "u", "uri", true, "The URI to the controller in the form tcp://host:port");
        options.addOption("r", "routingKey", true, "The routing key of the message to write.");
        options.addOption("m", "message", true, "The message to write.");

        return options;
    }

    private static CommandLine parseCommandLineArgs(Options options, String[] args)
            throws ParseException {
        final DefaultParser parser = new DefaultParser();
        return parser.parse(options, args);
    }
}
