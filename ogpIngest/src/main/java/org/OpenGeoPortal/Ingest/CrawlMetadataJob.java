package org.OpenGeoPortal.Ingest;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.OpenGeoPortal.Layer.Metadata;
import org.OpenGeoPortal.Layer.PlaceKeywords;
import org.OpenGeoPortal.Utilities.FileUtils;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
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
	
	/**
	 * apply information from the page to update the passed metadata
	 * much of the processing below is very specific to the UN site at http://cod.humanitarianresponse.info
	 *   and is not generally applicable
	 * the limitation with the approach here is we don't know which element 
	 *   in the page is associated with the passed metadata object
	 *   so we can only scrape the page for values that apply to 
	 *   all the elements on it
	 * for the un site, this means I'm not crawling the date or the data type
	 * rather then passing page page, this function should be passed a second
	 *   metadata object that was created when the link was found
	 */
	public void adjustMetadata(Metadata metadata, Object auxInfo)
	{
		String adjustMetadata = "";
		try
		{adjustMetadata = ingestProperties.getProperty("crawlMetadataJob.adjustMetadata");}
		catch (IOException e){logger.error("could not get ingestProperty crawlMetadataJob.adjustMetadata");return;}
		if ("false".equalsIgnoreCase(adjustMetadata)) return;
		
		logger.info("in CrawlMetaDAtaJob.adjustMetadata, adjustMetadata = " + adjustMetadata 
					+ ", auxInfo = " + auxInfo);
		Page currentPage = (Page)auxInfo;
    	HtmlParseData htmlParseData = (HtmlParseData) currentPage.getParseData();
    	// we assume the pageTitle is something like Afghanistan  | COD-FOD Registry
    	String pagePlaceName = htmlParseData.getTitle();
    	if (pagePlaceName != null)
    	{
    		if (pagePlaceName.contains("|"))
    		{
    			String[] parts = pagePlaceName.split("\\|");
    			if (parts.length > 0)
    				pagePlaceName = parts[0].trim();
    		}
    	}
    	String layerTitle = metadata.getTitle();
    	logger.info("  adjusting with placename " + pagePlaceName);
    	if (layerTitle.contains(pagePlaceName) == false)
    	{
    		// here if the layer title lacks the placename from the page title
    		metadata.setTitle(pagePlaceName + " " + layerTitle);
    	}
    	// add page place name to place keywords
    	// is this right?  
    	List<PlaceKeywords> placeKeywordsList = metadata.getPlaceKeywords();
    	PlaceKeywords pagePlaceNameKeyword = new PlaceKeywords();
    	pagePlaceNameKeyword.addKeyword(pagePlaceName);
    	placeKeywordsList.add(pagePlaceNameKeyword);

    	
	}


}
