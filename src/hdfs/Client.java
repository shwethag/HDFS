package hdfs;

import java.util.Scanner;

import util.Connector;

public class Client {

	private Connector connector;

	public Client() {

		connector = Connector.getConnector();
	}

	public static void main(String[] args) {

		Client client = new Client();

		Scanner sc = null;
		sc = new Scanner(System.in);
		boolean exitflag = false;
		while (true) {
			System.out.println("1.get 2.put 3.list");

			int option = Integer.parseInt(sc.nextLine());
			String fileName;
			switch (option) {
			case 1:
				System.out.println("Enter File Name");
				fileName = sc.nextLine();

				client.connector.get(fileName);
				break;

			case 2:
				System.out.println("Enter File Name");
				fileName = sc.nextLine();
				System.out.println("INFO: File name to put " + fileName);
				client.connector.put(fileName);
				break;

			case 3:
				client.connector.list();
				break;

			default:
				exitflag = true;
				break;
			}
			if (exitflag)
				break;
		}
		if (sc != null)
			sc.close();

	}
}
