package org.OpenGeoPortal.Ingest;

import java.io.File;
import java.io.IOException;

import org.OpenGeoPortal.Utilities.FileUtils;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class CrawlMetadataJob extends AbstractMetadataJob implements UploadMetadataJob, Runnable 
{

	/**
	 * kick off a crawl of url looking for metadata files
	 * initiate a web crawl using crawler4j
	 */
	@Override
	public void getMetadata() throws IOException 
	{
		ingestStatus = ingestStatusManager.getIngestStatus(jobId);
		// we need a temp/working directory for crawler4j
		String crawlStorageFolder = FileUtils.createTempDir().toString();

		//String crawlStorageFolder = "/Users/stevemcdonald/Devel/WebCrawlZip/WebCrawlZip/tmp";  //hack!
        int numberOfCrawlers = 1;

        CrawlConfig config = new CrawlConfig();
        config.setCrawlStorageFolder(crawlStorageFolder);
        config.setPolitenessDelay(500);
        config.setMaxDepthOfCrawling(40);
        config.setMaxPagesToFetch(-1);
        // should the user agent include Tufts/the institution's name?
        // how is it available?  hack
        config.setUserAgentString("OpenGeoPortal/beta (Tufts, http://www.OpenGeoPortal.com)");

        // Instantiate the controller for this crawl.                                                                        
        PageFetcher pageFetcher = new PageFetcher(config);
        RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
        RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
        CrawlController controller;
		try 
		{
			controller = new CrawlController(config, pageFetcher, robotstxtServer);
		} 
		catch (Exception e) 
		{
			ingestStatus.addError(url, "error creating CrawlController");
			e.printStackTrace();
			return;
		}

         // seed crawl with user entered domain
        controller.addSeed(url);
        
        // CrawlPageHandler needs access to the CrawlMetadataJob object
        controller.setCustomData(this);  
        
         // Start the crawl. This is a blocking operation
        controller.start(CrawlPageHandler.class, numberOfCrawlers);
        
	}

}
