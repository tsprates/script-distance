import java.io.FileNotFoundException;

public class Program {
	public static void main(String[] args) {

		if (args.length > 0) {
			try {
				new GetKm(args[0]);

				System.out.println("Projeto - AtlasBrasil");
				System.out.println("Iniciando:");
			} catch (FileNotFoundException e) {
				System.err.println(" [Error] - Necessario arquivo de Config.");
				System.exit(0);
			}
		} else {
			System.err.println(" [Error] - Necessario arquivo de Config.");
			System.exit(0);
		}
	}
}
