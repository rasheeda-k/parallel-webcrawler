package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.regex.Pattern;

/*
 * This crawler uses multiple threads to crawl web pages in parallel.
 * It uses ForkJoinPool to manage threads.
 */
final class ParallelWebCrawler implements WebCrawler {

    // Used to track current time
    private final Clock clock;

    // Maximum time allowed for crawling
    private final Duration timeout;

    // Number of most frequent words to return
    private final int popularWordCount;

    // Thread pool for parallel execution
    private final ForkJoinPool pool;

    // URL patterns that should be ignored
    private final List<Pattern> ignoredUrls;

    // Maximum depth allowed for crawling
    private final int maxDepth;

    // Used to create page parsers
    private final PageParserFactory parserFactory;

    /*
     * Constructor where dependencies are injected.
     */
    @Inject
    ParallelWebCrawler(
            Clock clock,
            @Timeout Duration timeout,
            @PopularWordCount int popularWordCount,
            @TargetParallelism int threadCount,
            @IgnoredUrls List<Pattern> ignoredUrls,
            @MaxDepth int maxDepth,
            PageParserFactory parserFactory) {

        this.clock = clock;
        this.timeout = timeout;
        this.popularWordCount = popularWordCount;

        // Limit threads to available processors
        this.pool = new ForkJoinPool(
                Math.min(threadCount, getMaxParallelism())
        );

        this.ignoredUrls = ignoredUrls;
        this.maxDepth = maxDepth;
        this.parserFactory = parserFactory;
    }

    /*
     * Starts crawling from the given URLs.
     */
    @Override
    public CrawlResult crawl(List<String> startingUrls) {

        // Calculate the deadline time
        Instant deadline = clock.instant().plus(timeout);

        // Stores word counts safely across threads
        ConcurrentMap<String, Integer> counts = new ConcurrentHashMap<>();

        // Stores visited URLs without duplicates
        ConcurrentSkipListSet<String> visitedUrls = new ConcurrentSkipListSet<>();

        // Start crawling each starting URL
        for (String url : startingUrls) {
            pool.invoke(
                    new InternalCrawler(
                            url,
                            deadline,
                            maxDepth,
                            counts,
                            visitedUrls
                    )
            );
        }

        // If no words were found, return empty result
        if (counts.isEmpty()) {
            return new CrawlResult.Builder()
                    .setWordCounts(counts)
                    .setUrlsVisited(visitedUrls.size())
                    .build();
        }

        // Sort words and return final result
        return new CrawlResult.Builder()
                .setWordCounts(
                        WordCounts.sort(counts, popularWordCount)
                )
                .setUrlsVisited(visitedUrls.size())
                .build();
    }

    /*
     * Internal task that crawls a single URL.
     * It can create more tasks for linked pages.
     */
    public class InternalCrawler extends RecursiveTask<Boolean> {

        private final String url;
        private final Instant deadline;
        private final int maxDepth;
        private final ConcurrentMap<String, Integer> counts;
        private final ConcurrentSkipListSet<String> visitedUrls;

        public InternalCrawler(
                String url,
                Instant deadline,
                int maxDepth,
                ConcurrentMap<String, Integer> counts,
                ConcurrentSkipListSet<String> visitedUrls) {

            this.url = url;
            this.deadline = deadline;
            this.maxDepth = maxDepth;
            this.counts = counts;
            this.visitedUrls = visitedUrls;
        }

        /*
         * Main logic for crawling a page.
         */
        @Override
        protected Boolean compute() {

            // Stop if max depth reached or time is over
            if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
                return false;
            }

            // Skip ignored URLs
            for (Pattern pattern : ignoredUrls) {
                if (pattern.matcher(url).matches()) {
                    return false;
                }
            }

            // Skip already visited URLs
            if (!visitedUrls.add(url)) {
                return false;
            }

            // Parse the page
            PageParser.Result result = parserFactory
                    .get(url)
                    .parse();

            // Update word counts
            result.getWordCounts().forEach(
                    (word, count) ->
                            counts.merge(word, count, Integer::sum)
            );

            // Create subtasks for each link
            List<InternalCrawler> subtasks = new ArrayList<>();
            for (String link : result.getLinks()) {
                subtasks.add(
                        new InternalCrawler(
                                link,
                                deadline,
                                maxDepth - 1,
                                counts,
                                visitedUrls
                        )
                );
            }

            // Run subtasks in parallel
            invokeAll(subtasks);
            return true;
        }
    }

    /*
     * Returns maximum available CPU cores.
     */
    @Override
    public int getMaxParallelism() {
        return Runtime.getRuntime().availableProcessors();
    }
}
