package org.OpenGeoPortal.Ingest;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Vector;

import org.OpenGeoPortal.Layer.GeometryType;
import org.OpenGeoPortal.Layer.Metadata;
import org.OpenGeoPortal.Layer.PlaceKeywords;
import org.OpenGeoPortal.Utilities.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

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
     * called with a page on a site we're crawling
     * get all the links on the page, for links to zip and xml files
     *   create a local copy of the file and process it
     * Unfortuantely, the html parsing provided by jcralwer4j is limited
     * So we re-process the discovered page with jsoup so there is a dom to traverse 
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
        	Document doc;
        	try
        	{
        		doc= Jsoup.connect(pageUrl$).get();
        	}
        	catch (IOException e)
        	{
        		logger.error("in CrawlPageHandler.visit, could not parse html: " + e.getMessage());
        		return;
        	}
	        Elements links = doc.select("a[href]");
        	//HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
        	//List<WebURL> links = htmlParseData.getOutgoingUrls();
        	for (Element link : links)
        	{
        		String link$ = link.attr("abs:href");
        		//String link$ = link.getURL().toString();
        		URL linkUrl;
				try {linkUrl = new URL(link$);} 
				catch (MalformedURLException e1) {logger.error("inCrawlPageHandler.visit, couldn't parse "  + link$);return;}
		        
        		link$ = link$.toLowerCase();
        		if (link$.endsWith(".zip") || (link$.endsWith(".xml")))
        		{
        			// process any zip or xml file, even when link points to another site        			
        			String urlPath = linkUrl.getPath();
        			//urlPath = link.getPath();
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
        				// rather then passing the page, we probably need to parse data on the page
        				//   near the current link, build a Metadata object, and pass that 
    	        		Metadata pageMetadata = getPageMetadata(doc, link);
						crawlMetadataJob.processFile(localTempFile, 1, pageMetadata);
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
    
    /**
     * use jsoup for more sophisticated crawling then supported by crawler4j
     * this requires getting the page again, perhaps we could use text from cralwer4j?
     * 
     * @param page
     */
    public void visit2(Page page)
    {
    	logger.info(":");
    	logger.info("!top of visit2");
    	CrawlMetadataJob crawlMetadataJob = (CrawlMetadataJob)this.getMyController().getCustomData();
        String pageUrl$ = page.getWebURL().getURL();
        try 
        {
			Document doc = Jsoup.connect(pageUrl$).get();
	        Elements links = doc.select("a[href]");
	        for (Element link : links) 
	        {
	        	String linkHref = link.attr("abs:href");
	        	logger.info(" link = " + linkHref);
	        	if (linkHref.endsWith(".zip") || linkHref.endsWith(".xml"))
	        	{
	        		// here with a file that potentially contains metadata
	        		// we need to scrape page for metadata
	        		// for the UN site, we need to find the thead table heading
	        		// find column names for date and data type
	        		// then scrape data from row
	        		// perhaps get parents to tr, then use siblingElements?
	        		Metadata pageMetadata = getPageMetadata(doc, link);
	        	}	        	
	        }
	        logger.info(": ");
	        logger.info("visit2: crawled data, end");

		} 
        catch (IOException e1) 
        {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    }
    
    /**
     * code for metadata specific to the unsite http://cod.humanitarianresponse.info/country-region
     * 
     * @param link
     * @return
     */
    private Metadata getPageMetadata(Document page, Element link)
    {
		Metadata auxMetadata = new Metadata();
    	Elements columns = getColumns(link);
		logger.info(" rows size = " + columns.size());
		if (columns.size() >= 6)
		{

			// assuming we're on the un site
			String abstract$ = columns.get(2).text();
			auxMetadata.setDescription(abstract$);
			String date$ = columns.get(3).text();
			if ("Unknown".equalsIgnoreCase(date$) == false)
				auxMetadata.setContentDate(date$);
			String geometryType$ = columns.get(5).text();
			geometryType$ = adjustGeometryType(geometryType$);
			auxMetadata.setGeometryType(geometryType$);
			logger.info(": ");
			logger.info("visit2: crawled data = " + date$ + ", geometryType$ = " + geometryType$);
			logger.info(" description = " + abstract$);
		}
		// we assume the pageTitle is something like Afghanistan  | COD-FOD Registry
		String pagePlaceName = page.title();
    	if (pagePlaceName != null)
    	{
    		if (pagePlaceName.contains("|"))
    		{
    			String[] parts = pagePlaceName.split("\\|");
    			if (parts.length > 0)
    			{
    				pagePlaceName = parts[0].trim();
    				auxMetadata.setTitle(pagePlaceName);
    				List<PlaceKeywords> placeKeywordsList = new Vector<PlaceKeywords>();
    		    	PlaceKeywords pagePlaceNameKeyword = new PlaceKeywords();
    		    	pagePlaceNameKeyword.addKeyword(pagePlaceName);
    		    	placeKeywordsList.add(pagePlaceNameKeyword);
    		    	logger.info(" pagePlaceName = " + pagePlaceName);
    			}
    		}
    	}
		return auxMetadata;
    }
    
    
    /**
     * this is a pretty limited function
     * @param geometryType
     * @return
     */
    private static String adjustGeometryType(String geometryType)
    {
    	if (geometryType == null)
    		return "";
    	String[] parts = geometryType.split(" ");
    	String returnValue = parts[0];
    	if (returnValue.endsWith("s"))
    		returnValue = returnValue.substring(0, returnValue.length() - 1);
    	if (returnValue.equals("Area"))
    		returnValue = "Polygon";
    	return returnValue;
    }
    /**
     * walk up parents looking for the tr the passed element is in
     * then return all the rows
     * @param element
     * @return
     */
    private Elements getColumns(Element element)
    {
    	// first get the tr this element is in
    	Element temp = element;
    	for (int i = 0 ; i < 10 ; i++)
    	{
    		logger.info(" tag = " + temp.tagName() + ", text = " + temp.text());
    		if (temp.tagName() == "td")
    		{
    			logger.info("  found td");
    			Elements rows = temp.siblingElements();
    			return rows;
    		}
    		if (temp.parent() == null)
    			return null;
    		temp = temp.parent();
    	}
    	return null;  // here if we didn't find the tr
    }

}
