package org.OpenGeoPortal.Utilities;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang3.text.WordUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.geometry.BoundingBox;

/** 
 * this is some code to explore the amazing location data from Newberry Library 
 * attribute from the file are:
 * ID_NUM: 1 
 * NAME: ALEUTIAN ISLANDS
 * ID: aks_aleutianislands
 * STATE_TERR: Alaska
 * FIPS: 02010
 * VERSION: 1
 * START_DATE: Fri Apr 01 00:00:00 EST 1960 
 * END_DATE: Tue Mar 31 00:00:00 EST 1970
 * CHANGE: 1960 census. Aleutian Islands (Election District 14) served as a census area.
 * CITATION: (Census of Population 1960, 51; NHGIS)
 * START_N: 19600401
 * END_N: 19700331
 * AREA_SQMI: 14159.0
 * CNTY_TYPE: County
 * FULL_NAME: ALEUTIAN ISLANDS
 * CROS_REF: aks_aleutianislands_1_AK_Historical_Counties
 * ALEUTIAN ISLANDS (1960-04-01)
 * author stevemcdonald
 *
 */
public class NewberryIngest 
{
	
	//static String ogpSolrIngestUrl = "http://localhost:8983/solr/collection1"; // "http://geoportal-dev.atech.tufts.edu/solr/ogp";
	static String ogpSolrIngestUrl = "http://ec2-54-234-203-81.compute-1.amazonaws.com/solr/ogp";
	public static void processData(File shapeFile)
	{
		try
		{
			FileDataStore store = FileDataStoreFinder.getDataStore(shapeFile);
	        SimpleFeatureSource featureSource = store.getFeatureSource();
	        SimpleFeatureCollection simpleFeatureCollection = featureSource.getFeatures();
	        SimpleFeatureIterator iterator = simpleFeatureCollection.features();
	        int i = 0;
	        while (iterator.hasNext())
	        {
	        	SimpleFeature feature = iterator.next();
	        	addToOgp(feature);
	        	i++;
	        }

	        System.out.println(" i = " + i);
	        iterator.close();
		}
		catch (MalformedURLException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void addToOgp(SimpleFeature feature)
	{
		HttpSolrServer server = new HttpSolrServer(ogpSolrIngestUrl);

    	SolrInputDocument input = new SolrInputDocument();
    	input.addField("LayerId", "NewberryBoundaries:" + feature.getID());
    	input.addField("ExternalLayerId", "NewberryBoundaries:" + feature.getID());
    	input.addField("Institution", "newberry.org");
    	BoundingBox boundingBox = feature.getBounds();

    	input.addField("MinX", boundingBox.getMinX());
    	input.addField("MaxX", boundingBox.getMaxX());
    	input.addField("MinY", boundingBox.getMinY());
    	input.addField("MaxY", boundingBox.getMaxY());
    	double width = boundingBox.getWidth(); 
    	double centerX = (boundingBox.getMaxX() + boundingBox.getMinX())/2.;
    	double height = boundingBox.getHeight();
    	double centerY = (boundingBox.getMaxY() + boundingBox.getMinY())/2.;
    	input.addField("HalfWidth", Double.toString(width/2.));
    	input.addField("HalfHeight", Double.toString(height/2.));
    	input.addField("Area", Double.toString(width * height));
    	input.addField("CenterX", Double.toString(centerX));
    	input.addField("CenterY", Double.toString(centerY));
    	input.addField("GeoReferenced", "true");
    	
    	//DateFormat inputDateFormat = new SimpleDateFormat("EEE MMM dd kk:mm:ss z yyyy", Locale.ENGLISH);
    	Date startDate = (Date)feature.getAttribute("START_DATE");
    	String startDateIso = convertDate(startDate);
    	Date endDate = (Date)feature.getAttribute("END_DATE");
    	String endDateIso = convertDate(endDate);
    	
    	input.addField("ContentDate", endDateIso);
            	
    	input.addField("DataType", "BoundingBox");
    	DateFormat yearFormat = new SimpleDateFormat("yyyy");
    	
    	
    	String name = WordUtils.capitalize((String)feature.getAttribute("NAME"))
    					+ " " + yearFormat.format(startDate) + "-" + yearFormat.format(endDate); 
    	input.addField("LayerDisplayName", name);
    	input.addField("Name", name);
    	input.addField("Access", "Public");
    	input.addField("Availability", "Online");
    	input.addField("CollectionId", "collectionId");
    	input.addField("Publisher", "NewBerry");
    	input.addField("ThemeKeywords", "none");
    	input.addField("PlaceKeywords", name);
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

    	System.out.println(solrResponse);
	}
	
	public static String convertDate(Date passedDate)
	{
		try
		{
			//DateFormat inputDateFormat = new SimpleDateFormat("EEE MMM dd kk:mm:ss z yyyy", Locale.ENGLISH);
			//Date date = inputDateFormat.parse(passedDate);
			DateFormat isoDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:00'Z'");
			String dateIso$ = isoDateFormat.format(passedDate);
			return dateIso$;
		}
		catch (Exception e)
		{
			System.out.println("could not parse " + passedDate);
		}
		return "";
	}
	public static void main(String args[])
	{
		File 
			newberryShapeFile = new File(
  "/Users/stevemcdonald/tmp/borders/US_AtlasHCB_Counties_Gen05/US_HistCounties_Gen05_Shapefile/US_HistCounties_Gen05.shp");
		processData(newberryShapeFile);
	}

}
