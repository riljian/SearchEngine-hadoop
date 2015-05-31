/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.parser;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 *
 * @author ril
 */
public class Parser {
    
    private static FileSystem fs;
    private static final HashSet<String> downloadedDoc = new HashSet();
    private static final String[] wantedWebsites = {
        "http://www56.eyny.com/"
    };
    
    public static void main(String[] args) throws InterruptedException, IOException {
        final int downloadThreadCount = 15;
        final int docThreadCount = 5;
        final int flushThreadCount = 10;
        final int urlCount = 1000000;
        final int docCount = 50;
        final int resultCount = 50;
        ExecutorService service = Executors.newFixedThreadPool(
            downloadThreadCount +
            docThreadCount +
            flushThreadCount
        );
        BlockingQueue<URL> urlQueue = new ArrayBlockingQueue(urlCount);
        BlockingQueue<Document> docQueue = new ArrayBlockingQueue(docCount);
        BlockingQueue<HtmlResult> resultQueue = new ArrayBlockingQueue(resultCount);
        
        for (String s : wantedWebsites) {
            urlQueue.put(new URL(s));
            downloadedDoc.add(s);
        }
        fs = FileSystem.get(new Configuration());
        
        for (int i = 0; i != downloadThreadCount; ++i) {
            service.execute(new HtmlDownloader(urlQueue, docQueue, resultQueue));
        }
        for (int i = 0; i != docThreadCount; ++i) {
            service.execute(new HtmlParser(docQueue, urlQueue));
        }
        for (int i = 0; i != flushThreadCount; ++i) {
            service.execute(new HtmlResultFlusher(resultQueue));
        }
        //service.shutdown();
        //TimeUnit.SECONDS.sleep(10);
        //service.shutdownNow();
        service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        System.exit(0);
    }
    private static class HtmlDownloader implements Runnable {
        private final BlockingQueue<URL> urlQueue;
        private final BlockingQueue<Document> docQueue;
        private final BlockingQueue<HtmlResult> resultQueue;
        private final int TIMEOUT_MILLIS = 3000;
        public HtmlDownloader(
                BlockingQueue<URL> urlQueue, 
                BlockingQueue<Document> docQueue,
                BlockingQueue<HtmlResult> resultQueue
        ) {
            this.urlQueue = urlQueue;
            this.docQueue = docQueue;
            this.resultQueue = resultQueue;
        }
        @Override
        public void run() {
            try {
                while (!Thread.interrupted()) {
                    //TODO
                    try {
                        Document doc = Jsoup.parse(urlQueue.take(), TIMEOUT_MILLIS);
                        resultQueue.put(new HtmlResult(doc.location(), doc.title(), doc.body().text()));
                        docQueue.put(doc);
                    } catch (IOException ex) {
                    }
                }
            } catch (InterruptedException ex) {
            }
        }
    }
    private static class HtmlParser implements Runnable {
        private final BlockingQueue<Document> docQueue;
        private final BlockingQueue<URL> urlQueue;
        public HtmlParser(BlockingQueue<Document> docQueue, BlockingQueue<URL> urlQueue) {
            this.docQueue = docQueue;
            this.urlQueue = urlQueue;
        }
        @Override
        public void run() {
            try {
                while (!Thread.interrupted()) {
                    //TODO
                    
                    // parse link
                    Elements links = docQueue.take().select("a[href]");
                    
                    for (Element link : links) {
                        
                        try {
                            String s = link.attr("abs:href");
                            if (!downloadedDoc.contains(s) && isWanted(s)) {
                                urlQueue.put(new URL(s));
                                downloadedDoc.add(s);
                            }
                        } catch (MalformedURLException ex) {
                        }
                    }
                    
                }
            } catch (InterruptedException ex) {
            }
        }
    }
    private static class HtmlResultFlusher implements Runnable {
        private final BlockingQueue<HtmlResult> resultQueue;
        public HtmlResultFlusher(BlockingQueue<HtmlResult> resultQueue) {
            this.resultQueue = resultQueue;
        }
        @Override
        public void run() {
            try {
                while (!Thread.interrupted()) {
                    HtmlResult htmlResult = resultQueue.take();
                    //TODO
                    String fileName = htmlResult.getLink().substring("http://www56.eyny.com/".length());
                    if (fileName.length() == 0) {
                        fileName ="index.html";
                    }
                    Path inputPath = new Path("/www56.eyny.com", fileName);
                    try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(inputPath, true), "UTF8"))) {
                        bw.write(htmlResult.getLink() + "\n");
                        bw.write(htmlResult.getTitle() + "\n");
                        bw.write(htmlResult.getContent());
                        bw.close();
                    } catch (IOException ex) {
                    }
                }
            } catch (InterruptedException ex) {
            }
        }
    }
    private static class HtmlResult {
        private final String link;
        private final String title;
        private final String content;
        public HtmlResult(
            String link,
            String title,
            String content
        ) {
            this.link = link;
            this.title = title;
            this.content = content;
        }
        public String getTitle() {
            return title;
        }
        public String getLink() {
            return link;
        }
        public String getContent() {
            return content;
        }
    }
    private static boolean isWanted(String url) {
        if (url.endsWith(".html")) {
            for (String s : wantedWebsites) {
                if (url.contains(s)) {
                    return true;
                }
            }
        }
        return false;
    }
}