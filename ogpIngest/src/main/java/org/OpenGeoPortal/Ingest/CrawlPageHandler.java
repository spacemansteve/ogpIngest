package org.OpenGeoPortal.Ingest;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.OpenGeoPortal.Layer.BoundingBox;
import org.OpenGeoPortal.Layer.GeometryType;
import org.OpenGeoPortal.Layer.Metadata;
import org.OpenGeoPortal.Layer.PlaceKeywords;
import org.OpenGeoPortal.Layer.ThemeKeywords;
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
		if (href.endsWith(".img") || href.endsWith(".lbl"))
		{
			logger.debug("shouldVisit not url " + href + ", because it is not a web page");
			return false;
		}
		if (href.startsWith(crawlMetadataJob.url))
		{
			logger.debug("shouldVisit url " + href + ", based on " + crawlMetadataJob.url);
			return true;
		}
		else
		{
			logger.debug("shouldVisit not url " + href + ", based on " + crawlMetadataJob.url);
			return false;
		}
	}
	
	public static int ingestCount = 0;
	public static int pageCount = 0;
	
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
        	logger.info("  html page found");
        	
        	Document doc;
        	try
        	{
        		doc = Jsoup.connect(pageUrl$).get();
        		logger.info("  parsed page with Jsoup");
        	}
        	catch (IOException e)
        	{
        		logger.error("in CrawlPageHandler.visit, could not parse html: " + e.getMessage());
        		return;
        	}
            processLinks(doc, pageUrl$, crawlMetadataJob);
            //processPageAsResource(doc, pageUrl$, crawlMetadataJob);
        }
        else
        {
        	logger.info("  skipping non-html page");
        }
    }
    
    /**
     * check the page for latitude and longitude class element
     * this works for wikipedia
     * @param doc
     * @param pageUrl
     * @param crawlMetadataJob
     */
    private void processPageAsResource(Document doc, String pageUrl, CrawlMetadataJob crawlMetadataJob)
    {
    	pageCount++;
    	Elements latitudes = doc.getElementsByClass("latitude");
    	Elements longitudes = doc.getElementsByClass("longitude");
    	
    	if ((latitudes.size() == 0) || (longitudes.size() == 0))
    	{
    		return;
    	}
    	double area = getArea(doc);
    	if (area == 0)
    		return;
    	double sideLengthMiles = Math.sqrt(area);
    	double earthMilesPerDegree = 69.172;
    	double sideLengthDegree = sideLengthMiles / earthMilesPerDegree;
    	double halfSideLengthDegree = sideLengthDegree / 2.0;
    	String latitude$ = latitudes.get(0).text();
    	String longitude$ = longitudes.get(0).text();

    	double centerLatitude = parseDegrees(latitude$);
    	double centerLongitude = parseDegrees(longitude$);

    	// given the location and other page information, create a metadata object to ingest
    	Metadata metadata = new Metadata(pageUrl);
    	BoundingBox boundingBox = new BoundingBox(centerLongitude - halfSideLengthDegree,
				  							centerLatitude - halfSideLengthDegree,
				  							centerLongitude + halfSideLengthDegree,
				  							centerLatitude + halfSideLengthDegree);
    	System.out.println(" bounding box = " + (centerLongitude - halfSideLengthDegree)
					+ ", " +  (centerLatitude - halfSideLengthDegree)
					+ ", " + (centerLongitude + halfSideLengthDegree)
					+ ", " + (centerLatitude + halfSideLengthDegree));
    	metadata.setBounds(boundingBox);
    	String title = doc.title();
    	// strip off "- Wikipedia, the free encyclopedia" 
    	if (title.contains(" - "))
    		title = title.substring(0, title.indexOf("-") - 1);
    	metadata.setTitle(title);
    	metadata.setOwsName(pageUrl);
		List<ThemeKeywords> themeKeywordList = new ArrayList<ThemeKeywords>();
		metadata.setThemeKeywords(themeKeywordList);
		List<PlaceKeywords> placeKeywordList = new ArrayList<PlaceKeywords>();
		metadata.setPlaceKeywords(placeKeywordList);
		metadata.setInstitution("Wikipedia");
		metadata.setOriginator("Wikipedia");
		metadata.setGeometryType(GeometryType.Undefined);
    	System.out.println("  trying to ingest " + metadata.toString());
    	ingestCount++;
    	System.out.println("\ningest count = " + ingestCount + ", pageCount = " + pageCount + "\n");
    	crawlMetadataJob.solrIngestMetadata(metadata);
    }
    
    /**
     * loop over all the rows in the geography table and return the first value with units of sq mi
     * we get some strange unicode characters from jsoup so we use Normalizer to eliminate them
     * @param doc
     * @return
     */
    private double getArea(Document doc)
    {
    	//Elements infoBoxes = doc.getElementsByClass("infobox geography vcard");
    	Elements infoBoxes = doc.getElementsByClass("geography");
    	if (infoBoxes.size() == 0)
    	{
    		System.out.println("Did not find infobox geography vcard");
    		return 0;
    	}
    	
    	Element infoBox = infoBoxes.get(0);
    	Elements tableRows = infoBox.child(0).children();
    	for (Element currentRow : tableRows)
    	{
    		String text = currentRow.text();
    		
    		if (text.contains("sq") && text.contains("mi") && text.contains("km2"))
    		{
    			//String area$ = currentRow.child(1).text();
    			String areaRow$ = currentRow.text();
    			System.out.println("area$ = " + areaRow$);
    			
    			String cleanedUp$ = cleanUpString(areaRow$);
    			System.out.println("cleaned = " + cleanedUp$);
    			int unitsIndex = cleanedUp$.indexOf("sq mi");
    			String beforeUnits$ = cleanedUp$.substring(0, unitsIndex -1);
    			beforeUnits$ = beforeUnits$.trim();
    			String area$ = "";
    			if (beforeUnits$.contains(" "))
    			{
    				// here if string contains square kilometers before square miles
    				int lastSpace = beforeUnits$.lastIndexOf(" ");
    				area$ = beforeUnits$.substring(lastSpace);
    			}
    			else
    			{
    				int firstSpace = beforeUnits$.indexOf(" ");
    				area$ = beforeUnits$.substring(0, firstSpace);
    			}
    			System.out.println("area$ = " + area$);
    			area$ = area$.replace(",", "");
    			area$ = area$.replace("(", "");
    			area$ = area$.replace(")", "");
    			if (area$.contains("["))
    				area$ = area$.substring(0, area$.indexOf("["));
    			double area = Double.parseDouble(area$);
    			System.out.println("area = " + area);
    			return area;
    		}

    		
    	}
    	return 0;
    	/*
    	System.out.println("  infoBox children, first = " + infoBox.child(0).childNodeSize());
    	
    	//infoBox is a table, we need to find the row that holds the area
    	Elements areas = infoBox.getElementsMatchingText("Area");
    	if (areas.size() == 0)
    	{
    		System.out.println("did not find Area text");
    		return 0;
    	}
    	Element areaElement = areas.get(0);
    	System.out.println("  areaElement = " + areaElement.text());
    	Element areaElementParent = areaElement.parent();
    	System.out.println("  areaElementParent = " + areaElementParent.text());
    	int index = areaElementParent.elementSiblingIndex();
    	System.out.println(" sibling index = " + index);
    	
    	Element areaRow = tableRows.get(index + 1);
    	System.out.println("  areaRow = " + areaRow.text());
    	Element areaCell = areaRow.child(1);
    	System.out.println("  areaCell = " + areaCell.text());
    	String area$ = areaCell.text();
    	area$ = area$.trim();
    	String[] parts = area$.split(" ");
    	String temp = parts[0];
    	System.out.println(" area string = " + temp);
    	double area = Double.parseDouble(temp);
    	return area;
    	*/
    	
    }
    
    /**
     * replace non-ascii characters with spaces
     * @param passedString
     * @return
     */
    private String cleanUpString(String passedString)
    {
		String convertedString = Normalizer.normalize(passedString, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", " ");
		return convertedString;
    }
    
    //42 (degrees) 23 (minutes) 15N
    private double parseDegrees(String degrees$)
    {
    	String cleaned = cleanUpString(degrees$);  //degrees$.replaceAll("[^0-9.]", " ");
    	String[] parts = cleaned.split(" ");
    	double degrees = Double.parseDouble(parts[0]);
    	if (parts.length > 1)
    	{
    		double minutes = Double.parseDouble(parts[1]) / 60.0; 
    		degrees += minutes;
    	}
    	if (parts.length > 2)
    	{
    		double seconds = 0;
    		try {seconds = Double.parseDouble(parts[2]) / 3600.0;}
    		catch (java.lang.NumberFormatException e) {System.out.println("could not parse " + parts[2] + ", " + e.toString());}
    		degrees += seconds;
    	}
    	if (degrees$.contains("S") || degrees$.contains("W"))
    		degrees = degrees * -1.0;
    	return degrees;
    }

    /**
     * process all the links for spatial data
     * links ot xml or zip files are checked for spatial metadata
     * @param doc
     * @param pageUrl$
     * @param crawlMetadataJob
     */
    private void processLinks(Document doc, String pageUrl$, CrawlMetadataJob crawlMetadataJob)
    {
        Elements links = doc.select("a[href]");
    	for (Element link : links)
    	{
    		String link$ = link.attr("abs:href");
    		//String link$ = link.getURL().toString();
    		URL linkUrl;
			try {linkUrl = new URL(link$);} 
			catch (MalformedURLException e1) {logger.error("inCrawlPageHandler.visit, couldn't parse "  + link$);return;}
	        
    		link$ = link$.toLowerCase();
    		logger.info("current link = " + link$);
    		if (link$.endsWith(".zip") || (link$.endsWith(".xml") || (link$.endsWith(".lbl") || link$.endsWith(".img"))))
    		{
    			if (link$.contains("gazetter"))
    				continue;
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
    				// hack, when should we look for page metadata?
	        		Metadata pageMetadata = null; //getPageMetadata(doc, link);
					crawlMetadataJob.processFile(localTempFile, 1, pageMetadata);
					localTempFile.delete();  // after ingest, delete file
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
