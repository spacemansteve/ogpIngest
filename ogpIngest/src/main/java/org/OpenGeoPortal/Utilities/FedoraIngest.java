package org.OpenGeoPortal.Utilities;

import org.apache.solr.client.solrj.*;
import org.apache.solr.client.solrj.impl.*;
import org.apache.solr.client.solrj.response.*;
import org.apache.solr.common.*;

import java.io.IOException;
import java.util.*;

/**
 * This code, which ingests Fedora repository data into OGP, is only for proof of concept purposes.
 * It gathers election data records from a hard-coded Tufts university fedora instance.
 * @author stevemcdonald
 *
 */
public class FedoraIngest 
{
	/** election records fedora url */
	static String fedoraSolrUrl = "http://elections-int.lib.tufts.edu/solr_tufts_election/tufts-election/";
	
	/** ogp solr instance to query for bounding box */
	static String ogpSolrBoundingBoxUrl = "http://geoportal-demo.atech.tufts.edu/solr/collection1";
	/** ogp solr instance to ingest election records into */
	static String ogpSolrIngestUrl = "http://ec2-23-20-177-35.compute-1.amazonaws.com/solr/ogp"; 
	//http://localhost:8983/solr/collection1"; // "http://geoportal-dev.atech.tufts.edu/solr/ogp";

    static int batchSize = 50;
    static int numberOfResults = 2626;

    public static void transfer()
    {
    	//numberOfResults = 50; // for testing                                                                                                                  
    	Hashtable<String, Hashtable<FedoraIngest.BoundingBox, Double>> uniqueNames
    	= new Hashtable<String, Hashtable<FedoraIngest.BoundingBox, Double>>();

    	System.out.println("fedora solr setup started");
    	HttpSolrServer fedoraSolrServer = new HttpSolrServer(fedoraSolrUrl);
    	System.out.println("  completed");
    	fedoraSolrServer.setParser(new XMLResponseParser());

    	int nullCount = 0;
    	int errorCount = 0;

    	for (int j = 0 ; j < numberOfResults/batchSize ; j++)
    	{
    		SolrQuery query = new SolrQuery();
    		query.setQuery("jurisdiction_t:State");
    		//query.setRequestHandler("standard");  // required                                                                                             
    		query.setRows(batchSize);
    		query.setStart(j*batchSize);
    		System.out.println("start = " + j*batchSize + ", nullCount = " + nullCount + ", uniqueNameCount = " + uniqueNames.keySet().size()
    				+ ", errorCount = " + errorCount);
    		try
    		{
    			System.out.println("fedora solr query");
    			QueryResponse queryResponse = fedoraSolrServer.query(query);
    			System.out.println("  returned");
    			SolrDocumentList documents = queryResponse.getResults();
    			for (int i = 0 ; i < documents.size() ; i++)
    			{
    				SolrDocument document = documents.get(i);
    				Object stateName = document.getFieldValue("state_0_name_t");
    				Object title = document.getFieldValue("title_t");
    				Object date = document.getFieldValue("date_t");
    				Object id = document.getFieldValue("id");
    				//String id$ = "fedora:" + id.toString();
    				if (stateName != null)
    				{
    					String stateName$ = stateName.toString();
    					stateName$ = stateName$.trim();
    					Hashtable<FedoraIngest.BoundingBox, Double> boundingBox = null;
    					if (uniqueNames.containsKey(stateName$) == false)
    					{
    						boundingBox = getBoundingBox(stateName$);
    						if (boundingBox != null)
    							uniqueNames.put(stateName$, boundingBox);
    					}
    					else
    						boundingBox = uniqueNames.get(stateName$);
    					addToOgp(id.toString(), title.toString(), date.toString(), boundingBox);
    				}

    				if (stateName == null)
    					nullCount++;
    			}
    		}
    		catch (Exception e)
    		{
    			System.out.println(e.toString() );
    			errorCount++;
    		}
    	}
    }

    
    public static enum BoundingBox {MinX, MaxX, MinY, MaxY};
    
    /** our simple computation of media bounding box doesn't work for all layers so we hard code a few */
    public static Hashtable<String, String> stateLayers = null;
    private static void initStates()
    {
    	stateLayers = new Hashtable<String, String>();
    	stateLayers.put("Virginia", "Tufts.VAcensusblocks10");
    	stateLayers.put("North Carolina", "Tufts.NCcensusblkgps10");
    	stateLayers.put("South Carolina", "Tufts.SCcensusblkgps10");
    	stateLayers.put("West Virginia", "Tufts.WVcensusblkgps10");
    	stateLayers.put("Rhode Island", "Tufts.RIcensusblkgps10");
    	stateLayers.put("Connecticut", "HARVARD.SDE.TG95CTLKBLN");
    	stateLayers.put("Maine", "HARVARD.SDE.TG95MELKDLN");
    	stateLayers.put("New Hampshire", "MassGIS.GISDATA.NHTOWNS_ARC");
    	stateLayers.put("New York", "HARVARD.SDE.TG95NYLPTPT");
    	stateLayers.put("Pennsylvania", "Tufts.PAcensusblocks10");
    	stateLayers.put("Maryland", "HARVARD.SDE.TG95MDBLKPY");
    	stateLayers.put("Louisiana", "Tufts.LAcensusblocks10");
    	stateLayers.put("Ohio", "Tufts.OHcensusblocks10");
    }
    /**                                                                                                                                                             
     * compute consensus bounding box from OGP Solr for passed place name                                                                                           
     * @param placeName                                                                                                                                             
     * @return                                                                                                                                                      
     */
    public static Hashtable<BoundingBox, Double> getBoundingBox(String placeName)
    {
    	if (stateLayers == null)
    		initStates();
    	System.out.println(stateLayers.keySet().size() + ", " + stateLayers.keySet().toArray()[0]);
    	if (stateLayers.containsKey(placeName))
    	{
    		return getBoundingBoxById(stateLayers.get(placeName));
    	}
    	else
    	{
    		return getBoundingBoxByConsensus(placeName);
    	}
    }
    
    /**
     * when concensus bounding box isn't right, we can hard code a layer id that will work
     * @param id
     * @return
     */
    public static Hashtable<BoundingBox, Double> getBoundingBoxById(String id)
    {
    	HttpSolrServer ogpSolrServer = new HttpSolrServer(ogpSolrBoundingBoxUrl);
    	SolrQuery query = new SolrQuery();
    	query.setQuery("LayerId:" + id);
    	query.setRows(1);  // limit the total number of results                                                                                               
    	QueryResponse queryResponse;
    	try
    	{
    		queryResponse = ogpSolrServer.query(query);
    		SolrDocumentList documents = queryResponse.getResults();
    		long numberFound = documents.getNumFound();
    		if (numberFound != 1)
    			return null;
    		SolrDocument current = documents.get(0);
			double minX = (Double)current.getFieldValue("MinX");
			double maxX = (Double)current.getFieldValue("MaxX");
			double minY = (Double)current.getFieldValue("MinY");
			double maxY = (Double)current.getFieldValue("MaxY");
			Hashtable<BoundingBox, Double> returnValue = new Hashtable<BoundingBox, Double>();
			returnValue.put(BoundingBox.MinX, minX);
			returnValue.put(BoundingBox.MaxX, maxX);
			returnValue.put(BoundingBox.MinY, minY);
			returnValue.put(BoundingBox.MaxY, maxY);
			System.out.println("obtaining bounding box by id");
			return returnValue;
    	}
    	catch (SolrServerException e)
    	{
    		e.printStackTrace();
    		return null;
    	}
    }
    
    /**
     * compute consensus bounding box
     * request many layers matching PlaceKeyword and get median
     * @param placeName
     * @return
     */
    public static Hashtable<BoundingBox, Double> getBoundingBoxByConsensus(String placeName)
    {
    	System.out.println("bounding box solr start");
    	HttpSolrServer ogpSolrServer = new HttpSolrServer(ogpSolrBoundingBoxUrl);
    	SolrQuery query = new SolrQuery();
    	query.setQuery("PlaceKeywords:" + placeName);
    	query.setRows(350);  // limit the total number of results                                                                                               
    	QueryResponse queryResponse;
    	try
    	{
    		queryResponse = ogpSolrServer.query(query);
    		System.out.println("  completed");
    		SolrDocumentList documents = queryResponse.getResults();
    		long numberFound = documents.getNumFound();
    		double[] minXs = new double[documents.size()];
    		double[] maxXs = new double[documents.size()];
    		double[] minYs = new double[documents.size()];
    		double[] maxYs = new double[documents.size()];
    		if (documents.size() == 0)
    			return null;  // here if the place name is unknown to ogp solr     
    			for (int i = 0 ; i < documents.size() ; i++)
    			{
    				SolrDocument current = documents.get(i);
    				double currentMinX = (Double)current.getFieldValue("MinX");
    				minXs[i] = currentMinX;

    				double currentMaxX = (Double)current.getFieldValue("MaxX");
    				maxXs[i] = currentMaxX;

    				double currentMinY = (Double)current.getFieldValue("MinY");
    				minYs[i] = currentMinY;

    				double currentMaxY = (Double)current.getFieldValue("MaxY");
    				maxYs[i] = currentMaxY;
    			}

    			Arrays.sort(minXs);
    			int index = documents.size() / 2;
    			Hashtable<BoundingBox, Double> returnValue = new Hashtable<BoundingBox, Double>();
    			returnValue.put(BoundingBox.MinX, minXs[index]);
    			returnValue.put(BoundingBox.MaxX, maxXs[index]);
    			returnValue.put(BoundingBox.MinY, minYs[index]);
    			returnValue.put(BoundingBox.MaxY, maxYs[index]);
    			return returnValue;
    	}
    	catch (SolrServerException e)
    	{
    		e.printStackTrace();
    		return null;
    	}
}

    
    public static void addToOgp(String layerId, String displayName, String date, Hashtable<FedoraIngest.BoundingBox, Double> boundingBox)
    {
    	if (boundingBox == null)
    	{
    		System.out.println("warning, no bounding box for " + displayName);
    		return;
    	}
    	System.out.println("adding " + displayName);
    	HttpSolrServer server = new HttpSolrServer(ogpSolrIngestUrl);

    	SolrInputDocument input = new SolrInputDocument();
    	input.addField("LayerId", "fedora:" + layerId);
    	input.addField("ExternalLayerId", "fedora:" + layerId);
    	input.addField("Institution", "TuftsDigitalLibrary");
    	input.addField("MinX", boundingBox.get(BoundingBox.MinX).toString());
    	input.addField("MaxX", boundingBox.get(BoundingBox.MaxX).toString());
    	input.addField("MinY", boundingBox.get(BoundingBox.MinY).toString());
    	input.addField("MaxY", boundingBox.get(BoundingBox.MaxY).toString());
    	double width = boundingBox.get(BoundingBox.MaxX) - boundingBox.get(BoundingBox.MinX);
    	double centerX = (boundingBox.get(BoundingBox.MaxX) + boundingBox.get(BoundingBox.MinX))/2.;
    	double height = boundingBox.get(BoundingBox.MaxY) - boundingBox.get(BoundingBox.MinY);
    	double centerY = (boundingBox.get(BoundingBox.MaxY) + boundingBox.get(BoundingBox.MinY))/2.;
    	input.addField("HalfWidth", Double.toString(width/2.));
    	input.addField("HalfHeight", Double.toString(height/2.));
    	input.addField("Area", Double.toString(width * height));
    	input.addField("CenterX", Double.toString(centerX));
    	input.addField("CenterY", Double.toString(centerY));
    	input.addField("GeoReferenced", "true");
    	if (date.contains("-"))  
    		date = date.substring(0, date.indexOf("-"));  // strip off month
    	System.out.println(displayName + ", date = " + date);
    	if (date.length() == 4)
    		input.addField("ContentDate", date + "-01-01T01:01:01Z");  //if it looks like a year, hack to iso
    	else
    		System.out.println(" !!! could not handle above date");
    	
    	input.addField("DataType", "Polygon");
    	input.addField("LayerDisplayName", displayName);
    	input.addField("Name", displayName);
    	input.addField("Access", "Public");
    	input.addField("Availability", "Online");
    	input.addField("CollectionId", "collectionId");
    	input.addField("Publisher", "Fedora");
    	input.addField("ThemeKeywords", "none");
    	input.addField("PlaceKeywords", "none");
    	input.addField("Abstract", "abstract");
    	input.addField("Location", "{}");
    	input.addField("WorkspaceName", "none");
    	input.addField("FgdcText", "none");

    	UpdateResponse solrResponse = null;
    	try
    	{
    		solrResponse = server.add(input);
    		server.commit();
    	}
    	catch (SolrServerException e)
    	{
    		e.printStackTrace();
    	}
    	catch (IOException e)
    	{
    		e.printStackTrace();
    	}

    	System.out.println(solrResponse.toString());
    }

    /**
     * run the above proof of concept code
     * @param args
     */
    public static void main(String[] args)
    {
            transfer();
            System.out.println("hello, world");
    }


}
