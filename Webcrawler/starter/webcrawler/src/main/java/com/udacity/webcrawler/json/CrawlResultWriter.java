package com.udacity.webcrawler.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/*
 * This class is responsible for writing the crawl result to a file.
 * The result is written in JSON format.
 */
public final class CrawlResultWriter {

    // Holds the final crawl result data
    private final CrawlResult result;

    /*
     * Creates a writer for the given crawl result.
     */
    public CrawlResultWriter(CrawlResult result) {
        this.result = Objects.requireNonNull(result);
    }

    /*
     * Writes the crawl result to the given file path.
     * The file is created if it does not exist.
     * Data is appended to the file.
     */
    public void write(Path path) throws IOException {
        Objects.requireNonNull(path);
// TODO: Fill in this method.
        // Open file in create + append mode
        try (Writer fileWriter = Files.newBufferedWriter(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND)) {

            // Write JSON data to the file
            write(fileWriter);
        }
    }

    /*
     * Converts the crawl result to JSON and writes it using the writer.
     */
    public void write(Writer writer) throws IOException {
        Objects.requireNonNull(writer);
// TODO: Fill in this method.
        // ObjectMapper converts Java objects to JSON
        ObjectMapper mapper = new ObjectMapper();

        // Prevent closing the writer automatically
        mapper.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);

        // Write the crawl result as JSON
        mapper.writeValue(writer, result);
    }
}
