import java.io.FileNotFoundException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class GeoDist {
	public static void main(String[] args) {

		if (args.length > 0) {
			ExecutorService service = Executors.newSingleThreadExecutor();

			try {
				service.submit(new GetGeoService(args[0]));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			service.shutdown();

			try {
				service.awaitTermination(1, TimeUnit.HOURS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else {
			System.err
					.println(" [Error] - Necessário arquivo de configurações.");
			System.exit(0);
		}
	}
}
