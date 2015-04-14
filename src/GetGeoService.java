import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
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

	private String dest, orig;

	private BufferedReader fileIn;
	private PrintStream fileOut;

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

		System.out.println("Searching:");

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

		dest = props.getProperty("dest");
		orig = props.getProperty("orig");
	}

	@Override
	public void run() {

		try {
			fileIn = new BufferedReader(new FileReader(inputfile));
			fileOut = new PrintStream(outputfile);

			StringBuilder sb = new StringBuilder();
			String line;

			Map<String, Integer> mapColsCsv = new HashMap<String, Integer>();

			String latOrig, lngOrig;
			String latDest, lngDest;
			String route, duration;

			String respOrig, respDest, respRoute;
			String urlGeocodeOrig, urlGeocodeDest, urlRoute;

			int lines = 0;

			boolean existCols = false;
			int indexDest, indexOrig;

			double lat1, lng1, lat2, lng2;

			String saxGeocodeLat = "/GeocodeResponse/result/geometry/location/lat";
			String saxGeocodeLng = "/GeocodeResponse/result/geometry/location/lng";
			String saxGeocodeStatus = "/GeocodeResponse/status";

			startTime = System.currentTimeMillis();

			while ((line = fileIn.readLine()) != null) {
				String currentLineCols[] = line.split(delimeter);
				lines++;

				sb = new StringBuilder("");

				if (lines == 1) {
					makeHeader();
				}

				if (existCols == false) {
					for (int i = 0; i < currentLineCols.length; i++) {
						mapColsCsv.put(currentLineCols[i], i);
					}
					existCols = true;
					continue;
				}

				if (existCols == false) {
					throw new RuntimeException(
							"Não foi possivel identificar as colunas do arquivo.");
				}

				if (mapColsCsv.get(dest) == null
						|| mapColsCsv.get(orig) == null) {
					throw new RuntimeException("Colunas não batem.");
				}

				indexDest = mapColsCsv.get(dest);
				indexOrig = mapColsCsv.get(orig);

				urlGeocodeOrig = String
						.format("http://maps.googleapis.com/maps/api/geocode/xml?address=%s&sensor=false",
								encode(currentLineCols[indexOrig]));

				respOrig = getXml(urlGeocodeOrig);
				if ("OK".equals(getXPath(saxGeocodeStatus, respOrig))) {
					latOrig = getXPath(saxGeocodeLat, respOrig);
					lngOrig = getXPath(saxGeocodeLng, respOrig);

					lat1 = Double.parseDouble(latOrig);
					lng1 = Double.parseDouble(lngOrig);
				} else {
					throw new RuntimeException(
							"Não foi possivel recuperar as valores da origem.");
				}

				// add column lat and lng destination
				sb.append(text_delimeter + currentLineCols[indexOrig]
						+ text_delimeter + delimeter + latOrig + delimeter
						+ lngOrig + delimeter);

				urlGeocodeDest = String
						.format("http://maps.googleapis.com/maps/api/geocode/xml?address=%s&sensor=false",
								encode(currentLineCols[indexDest]));
				respDest = getXml(urlGeocodeDest);

				if ("OK".equals(getXPath(saxGeocodeStatus, respDest))) {
					latDest = getXPath(saxGeocodeLat, respDest);
					lngDest = getXPath(saxGeocodeLng, respDest);

					lat2 = Double.parseDouble(latDest);
					lng2 = Double.parseDouble(lngDest);
				} else {
					throw new RuntimeException(
							"Não foi possivel recuperar as valores do destino.");
				}

				// add column lat lng destination
				sb.append(text_delimeter + currentLineCols[indexDest]
						+ text_delimeter + delimeter + latOrig + delimeter
						+ lngOrig + delimeter);

				// distance
				sb.append(String.format(Locale.ENGLISH, "%s%.2f km%s",
						text_delimeter,
						(calcDistance(lat1, lng1, lat2, lng2) / 1000),
						text_delimeter)
						+ delimeter);

				// route and duration
				urlRoute = String
						.format("http://maps.googleapis.com/maps/api/directions/xml?origin=%s,%s&destination=%s,%s&sensor=false",
								encode(latOrig), encode(lngOrig),
								encode(latDest), encode(lngDest));
				respRoute = getXml(urlRoute);

				if ("OK".equals(getXPath("/DirectionsResponse/status",
						respRoute))) {
					route = getXPath(
							"/DirectionsResponse/route/leg/distance/text",
							respRoute);
					duration = getXPath(
							"/DirectionsResponse/route/leg/duration/text",
							respRoute);
				} else {
					throw new RuntimeException(
							"Não foi possivel recuperar da distância.");
				}

				sb.append(route + delimeter + duration);

				// outputfile
				fileOut.println(sb.toString());

				// progress
				System.out.println((lines - 1) + ". " + urlRoute);

				waitForLines(lines);
			}

			fileIn.close();
			fileOut.close();

		} catch (IOException e) {
			System.err.println("Arquivo não existe.");
			e.printStackTrace();
		}

		endTime = System.currentTimeMillis();
		System.out.println("End. " + ((endTime - startTime) / 1000L) + " segs");
	}

	/**
	 * 
	 */
	private void makeHeader() {
		fileOut.println("Dest" + delimeter + "lat" + delimeter + "lng"
				+ delimeter + "Orig" + delimeter + "lat" + delimeter + "lng"
				+ delimeter + "KM" + delimeter + "Distância" + delimeter
				+ "Duração");
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
	 * Sleep thread.
	 * 
	 * @param linhas
	 */
	private void waitForLines(int linhas) {
		if ((linhas % 50) == 0) {
			try {
				Thread.sleep(5000L);
			} catch (InterruptedException e) {
			}
		}

		if ((linhas % 500) == 0) {
			try {
				Thread.sleep(10000L);
			} catch (InterruptedException e) {
			}
		}
	}

	/**
	 * Ler o conteúdo XML devolvido pela URL.
	 * 
	 * @param url
	 *            Url do geoservice.
	 * @return
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	private String getXml(String url) {
		InputStream is;
		BufferedReader br;
		String lin;

		try {
			is = new URL(url).openStream();
			br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
			StringBuilder sb = new StringBuilder();

			while ((lin = br.readLine()) != null) {
				sb.append(lin);
			}

			br.close();
			return sb.toString();
		} catch (IOException e) {
			return "";
		}

	}

	/**
	 * Filtra tag procurada no XML devolvido pelo geoservice.
	 * 
	 * @param xml
	 *            String que representa uma XML de resposta de um webservice
	 * @param tag
	 *            Tag que procura-se encontrar no XML.
	 * @return Valor da tag encontrado.
	 */
	private String getTag(String xml, String tag) {
		try {
			Document document = db
					.parse(new InputSource(new StringReader(xml)));
			XPath xpath = xpathFactory.newXPath();
			return xpath.evaluate(tag, document);
		} catch (SAXException | IOException | XPathExpressionException e) {
			return "";
		}
	}

	/**
	 * Formata para retirar o último delimiter utilizado para o arquivo de
	 * entrada.
	 * 
	 * @param s
	 * @return String
	 */
	// private String removeLastDelim(String s) {
	// return s.substring(0, s.length()
	// - props.getProperty("delimiter").length());
	// }

}
