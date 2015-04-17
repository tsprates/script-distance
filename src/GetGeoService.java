import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Locale;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * 
 * @author Thiago
 */
public class GetGeoService implements Runnable {

	private File inputfile;
	private File outputfile;
	private FileInputStream configFile;
	private Properties props = new Properties();

	private Long startTime, endTime;

	private String delimeter;
	private String text_delimeter;

	private String dest[];
	private String orig[];

	private BufferedReader fileIn;
	private PrintWriter fileOut;

	// xml
	private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	private DocumentBuilder db;
	private XPathFactory xpathFactory = XPathFactory.newInstance();

	/**
	 * 
	 * 
	 * @param pathFileConfig
	 */
	public GetGeoService(String pathFileConfig) throws FileNotFoundException {
		System.out.println("Projeto - AtlasBrasil");

		try {
			configFile = new FileInputStream(pathFileConfig);
			props.load(configFile);
		} catch (IOException e) {
			throw new RuntimeException(
					"Erro ao carregar de propriedades do arquivo de config.");
		}

		// if (!inputfile.exists()) {
		// throw new RuntimeException("[Erro] Arquivos inválidos.");
		// }

		loadProps(props);

		System.out.println("1. Searching:");

		loadXmlReader();
	}

	/**
	 * 
	 */
	private void loadXmlReader() {
		try {
			db = dbf.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			throw new RuntimeException("[Erro] Leitor de xml.");
		}
	}

	/**
	 * 
	 * @param props
	 */
	private void loadProps(Properties props) {
		inputfile = new File(props.getProperty("inputfile"));
		outputfile = new File(props.getProperty("outputfile"));

		delimeter = props.getProperty("delimiter");
		text_delimeter = props.getProperty("text_delimiter");

		dest = props.getProperty("dest").split(delimeter);
		orig = props.getProperty("orig").split(delimeter);
	}

	/**
	 * 
	 * @param colsHeader
	 * @param origDest
	 * @param currentLineCols
	 */
	private String getCols(String[] colsHeader, String[] origDest,
			String[] currentLineCols) {
		StringBuilder search = new StringBuilder("");
		for (int k = 0; k < colsHeader.length; k++) {
			for (int i = 0; i < origDest.length; i++) {
				if (colsHeader[k].trim().equals(origDest[i].trim())) {
					search.append(" " + currentLineCols[k]);
				}
			}

		}

		return search.toString();
	}

	@Override
	public void run() {

		try {
			fileIn = new BufferedReader(new FileReader(inputfile));
			fileOut = new PrintWriter(new BufferedWriter(new FileWriter(
					outputfile)));

			StringBuffer sb = null;
			String line;

			String[] colsHeader = null;

			String route = "", distance = "";

			String url;
			String resp;

			int lines = 0;

			String searchOrig = "", searchDest = "";
			boolean existCols = false;

			Double lat1 = null, lng1 = null, lat2 = null, lng2 = null;

			String saxDirecResp = "/DirectionsResponse/route/leg/";

			startTime = System.currentTimeMillis();

			while ((line = fileIn.readLine()) != null) {
				String currentLineCols[] = line.split(delimeter);
				lines++;

				sb = new StringBuffer("");

				if (lines == 1) {
					makeHeader(line);
				}

				if (existCols == false) {
					colsHeader = line.split(delimeter);
					existCols = true;
					continue;
				}

				searchOrig = getCols(colsHeader, orig, currentLineCols);

				searchDest = getCols(colsHeader, dest, currentLineCols);

				// route and duration
				url = String
						.format(Locale.ENGLISH,
								"https://maps.googleapis.com/maps/api/directions/xml?origin=%s&destination=%s&sensor=false",
								encode(searchOrig.trim().toLowerCase()),
								encode(searchDest.trim().toLowerCase()));
				resp = getXml(url);

				// duration = "";
				distance = "";
				route = "";

				if ("OK".equals(getXPath("/DirectionsResponse/status", resp))) {
					route = getXPath(saxDirecResp + "distance/text", resp);

					lat1 = Double.parseDouble(getXPath(saxDirecResp
							+ "start_location/lat", resp));

					lng1 = Double.parseDouble(getXPath(saxDirecResp
							+ "start_location/lng", resp));

					lat2 = Double.parseDouble(getXPath(saxDirecResp
							+ "end_location/lat", resp));

					lng2 = Double.parseDouble(getXPath(saxDirecResp
							+ "end_location/lng", resp));

					distance = String.format(Locale.ENGLISH, "%s%.2f km%s",
							text_delimeter,
							(calcDistance(lat1, lng1, lat2, lng2) / 1000),
							text_delimeter);

					// duration = getXPath(saxDirecResp + "duration/text",
					// resp);
				}

				sb.append(line + delimeter + distance + delimeter + route);

				fileOut.println(sb.toString());

				System.out.println(lines + ". " + url);

			}

		} catch (IOException e) {
			System.err.println("Arquivo não existe.");
			e.printStackTrace();
		} finally {

			try {
				fileIn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			fileOut.close();
		}

		endTime = System.currentTimeMillis();
		System.out.println("End. " + ((endTime - startTime) / 1000L) + " segs");
	}

	/**
	 * 
	 * @param fieldCsv
	 */
	private void makeHeader(String fieldCsv) {
		fileOut.print(fieldCsv + delimeter);
		fileOut.println("Distância" + delimeter + "Percurso");
	}

	/**
	 * 
	 * @param indexCols
	 * @param tag
	 * @param resp
	 * @param current
	 * @return
	 */
	private String getXPath(String tag, String resp) {
		return getTag(resp, tag);
	}

	/**
	 * 
	 * @param s
	 * @return
	 */
	private String encode(String s) {
		try {
			return URLEncoder.encode(s, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return "";
		}
	}

	/**
	 * 
	 * @param radlat1
	 * @param radlng1
	 * @param radlat2
	 * @param radlng2
	 * @return
	 */
	private double calcDistance(double lat1, double lng1, double lat2,
			double lng2) {

		double R = 6378137; // Earth’s mean radius in meter
		double dLat = rad(lat2 - lat1);
		double dLong = rad(lng2 - lng1);
		double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
				+ Math.cos(rad(lat1)) * Math.cos(rad(lat2))
				* Math.sin(dLong / 2) * Math.sin(dLong / 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		double d = R * c;
		return d; // returns the distance in meter

	}

	/**
	 * 
	 * @param degrees
	 * @return
	 */
	private double rad(double degrees) {
		// return degrees * Math.PI / 180;
		return Math.toRadians(degrees);
	}

	/**
	 * 
	 * @param url
	 * @return
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	private synchronized String getXml(String url) {
		InputStream is = null;
		BufferedReader br = null;
		HttpURLConnection urlCon = null;
		String line;

		try {

			urlCon = (HttpURLConnection) new URL(url).openConnection();

			urlCon.setAllowUserInteraction(false);
			urlCon.setDoInput(true);
			urlCon.setDoOutput(false);
			urlCon.setUseCaches(false);
			urlCon.setRequestMethod("GET");
			urlCon.connect();

			StringBuffer sb = new StringBuffer("");

			br = new BufferedReader(new InputStreamReader(
					urlCon.getInputStream(), "UTF-8"));
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}

			return sb.toString();
		} catch (IOException e) {
			return "";
		} finally {
			if (urlCon != null)
				urlCon.disconnect();

			try {
				if (is != null)
					is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

	}

	/**
	 * 
	 * @param xml
	 * @param tag
	 * @return
	 */
	private String getTag(String xml, String tag) {
		try {
			Document document = db
					.parse(new InputSource(new StringReader(xml)));
			XPath xpath = xpathFactory.newXPath();
			return xpath.evaluate(tag, document);
		} catch (SAXException | IOException | XPathExpressionException e) {
			e.printStackTrace();
			return "";
		}
	}

}
