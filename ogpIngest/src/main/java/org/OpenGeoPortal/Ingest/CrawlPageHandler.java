package org.OpenGeoPortal.Ingest;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.OpenGeoPortal.Utilities.FileUtils;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;



/**
 * this class is used by crawler4j framework to handle found pages
 * 
 * @author stevemcdonald
 *
 */
public class CrawlPageHandler extends WebCrawler 
{
	/**                                                                                                                  
	 * should the page with the passed url be crawled
	 * only crawl the seed page and those below it 
	 */
	@Override
	public boolean shouldVisit(WebURL url) 
	{
		CrawlMetadataJob crawlMetadataJob = (CrawlMetadataJob)this.getMyController().getCustomData();
		String href = url.getURL().toLowerCase();
		if (href.startsWith(crawlMetadataJob.url))
		{
			logger.info("shouldVisit url " + href + ", based on " + crawlMetadataJob.url);
			return true;
		}
		else
		{
			logger.info("shouldVisit not url " + href + ", based on " + crawlMetadataJob.url);
			return false;
		}
	}
	
	 /**                                                                                                                      
     * called when a page on a site we're crawling
     * get all the links on the page, for links to zip and xml files
     *   create a local copy of the file and process
     */
    @Override
    public void visit(Page page)
    {
    	CrawlMetadataJob crawlMetadataJob = (CrawlMetadataJob)this.getMyController().getCustomData();
        String pageUrl$ = page.getWebURL().getURL();
        URL pageUrl = null;
        try  {pageUrl = new URL(pageUrl$);} 
        catch (MalformedURLException e) {e.printStackTrace();} 	// since we crawled to the page, it really should be valid

        logger.info("processing web page at " + pageUrl$);

        if (page.getParseData() instanceof HtmlParseData)
        {
        	HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
        	List<WebURL> links = htmlParseData.getOutgoingUrls();
        	for (WebURL link : links)
        	{
        		String link$ = link.getURL().toString();
        		URL linkUrl;
				try {linkUrl = new URL(link$);} 
				catch (MalformedURLException e1) {logger.error("inCrawlPageHandler.visit, couldn't parse "  + link$);return;}

        		link$ = link$.toLowerCase();
        		if (link$.endsWith(".zip") || (link$.endsWith(".xml")))
        		{
        			// process any zip or xml file, even when link points to another site
        			String urlPath = link.getPath();
        			String localFileName$ = urlPath;
        			if (urlPath.contains("/"))
        			{
        				String[] parts = urlPath.split("/");
        				localFileName$ = parts[parts.length - 1];
        			}
                    try 
                    {
                		File localTempDir = FileUtils.createTempDir();
            			File localTempFile = new File(localTempDir, localFileName$);
        				logger.info("copying link " + link$ + " on " + pageUrl$ + " to " + localTempFile);
        				
						org.apache.commons.io.FileUtils.copyURLToFile(linkUrl, localTempFile);
        				logger.info("processing file " + localTempFile.toString());
						crawlMetadataJob.processFile(localTempFile, 1);
					} 
                    catch (IOException e) 
					{
                    	System.out.println("in CrawlPageHandler.visit with crawlMetadataJob = " + crawlMetadataJob);
                    	System.out.println("  ingestStatus = " + crawlMetadataJob.ingestStatus);
            			crawlMetadataJob.ingestStatus.addError("crawlFileCopy", "could not create local copy");
						e.printStackTrace();
					}                    
        		}
        	}
        }
    }

}
