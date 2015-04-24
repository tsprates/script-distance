import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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
	 * Constructor.
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

		System.out.println("Searching:");

		loadXmlReader();
	}

	/**
	 * Instantiate XML doc builder.
	 */
	private void loadXmlReader() {
		try {
			db = dbf.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			throw new RuntimeException("[Erro] Leitor de xml.");
		}
	}

	/**
	 * Load properties.
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
	 * Returns the columns from the CSV file.
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

		return search.toString().trim();
	}

	@Override
	public void run() {

		try {
			fileIn = new BufferedReader(new FileReader(inputfile));

			fileOut = new PrintWriter(new OutputStreamWriter(
					new FileOutputStream(outputfile), "UTF-8"));

			StringBuffer sb = null;
			String line;

			String[] colsHeader = null;

			String result;
			String route, distance, duration;
			Double lat1 = null, lng1 = null, lat2 = null, lng2 = null;

			String url;
			String resp;

			int lines = 0;

			String searchOrig = "", searchDest = "";
			boolean existCols = false;

			String saxDirecResp = "/DirectionsResponse/route/leg/";

			// init time
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
								encode(searchOrig.toLowerCase()),
								encode(searchDest.toLowerCase()));
				resp = getXml(url);

				// reset previous values
				duration = "";
				distance = "";
				route = "";

				result = getXPath("/DirectionsResponse/status", resp);
				if ("OK".equals(result)) {
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

					duration = getXPath(saxDirecResp + "duration/text", resp);
				}

				sb.append(line + delimeter + distance + delimeter
						+ text_delimeter + route + text_delimeter + delimeter
						+ text_delimeter + duration + text_delimeter);

				fileOut.println(sb.toString());

				System.out.println(lines + ". " + url);

				try {
					// avoid status OVER_QUERY_LIMIT
					Thread.sleep(500);
				} catch (InterruptedException e) {
				}
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

		// calc elapsed time
		endTime = System.currentTimeMillis();
		System.out.println("End. " + ((endTime - startTime) / 1000L) + " segs");
	}

	/**
	 * Build the CSV header.
	 * 
	 * @param fieldCsv
	 */
	private void makeHeader(String fieldCsv) {
		fileOut.print(fieldCsv + delimeter);
		fileOut.println("Distância" + delimeter + "Rota" + delimeter
				+ "Duração");
	}

	/**
	 * Helper function for getTag.
	 * 
	 * @see #getTag(String, String)
	 * @param tag
	 * @param resp
	 * @return
	 */
	private String getXPath(String tag, String resp) {
		return getTag(resp, tag);
	}

	/**
	 * Encode param URL.
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
	 * Calcula distance.
	 * 
	 * @see http
	 *      ://stackoverflow.com/questions/1502590/calculate-distance-between
	 *      -two-points-in-google-maps-v3
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
	 * Convert degrees to radians.
	 * 
	 * @param degrees
	 * @return
	 */
	private double rad(double degrees) {
		// return degrees * Math.PI / 180;
		return Math.toRadians(degrees);
	}

	/**
	 * Get string response in XML format.
	 * 
	 * @param url
	 * @return
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	private synchronized String getXml(String url) {
		HttpURLConnection conn = null;
		InputStream stream = null;

		try {

			conn = (HttpURLConnection) new URL(url).openConnection();

			conn.setRequestMethod("GET");
			conn.setRequestProperty("Content-Type", "application/xml");
			conn.setRequestProperty("Accept-Language",
					"pt-br,pt;q=0.8,en-us;q=0.5,en;q=0.3");
			conn.setAllowUserInteraction(false);
			conn.setDoInput(true);
			conn.setDoOutput(false);
			conn.setUseCaches(false);
			conn.connect();

			StringBuffer sb = new StringBuffer("");

			BufferedReader br = new BufferedReader(new InputStreamReader(
					conn.getInputStream(), "UTF-8"));
			int c;
			while ((c = br.read()) != -1) {
				sb.append((char) c);
			}

			return sb.toString();
		} catch (IOException e) {
			return "";
		} finally {
			if (conn != null) {
				conn.disconnect();
			}

			try {
				if (stream != null) {
					stream.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

	/**
	 * Parse XML string.
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
