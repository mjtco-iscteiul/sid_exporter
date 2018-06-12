package pt.iscte.sid;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Timer;

import org.bson.Document;
import org.bson.types.BSONTimestamp;

import com.mongodb.MongoTimeoutException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

public class Exporter{

	private String[] propertiesAtributes = null;
	private static final String CONFIG_DIR = System.getProperty("user.dir")+File.separator+"config.properties";

	public Exporter() {
		readConfig();
		/*
		 * 0-MongodbPass
		 * 1-SybasePass
		 * 2-SybaseDatabase
		 * 3-MongoIp
		 * 4-SybasePort
		 * 5-MongodbUser
		 * 6-SybaseUser
		 * 7-MongoPort
		 * 8-MongoCollection
		 * 9-MongoDatabase
		 * 10-MigrationFreq
		 * 11-SybaseIp
		 * 
		 */
	}

	public int getPeriodicity() {
		if(validateConfig(propertiesAtributes)!=false) {
			return Integer.valueOf(propertiesAtributes[10]);
		}else {
			return 5;
		}
	}

	private boolean validateConfig(String[] propertiesAtributes) {
		try {
			if(propertiesAtributes.equals(null)) {
				System.out.println("Reconfigurar.");
				return false;
			}else if(propertiesAtributes.length != 12) {
				System.out.println("Reconfigurar.");
				return false;
			}else if(Integer.valueOf(propertiesAtributes[4])>65535 && Integer.valueOf(propertiesAtributes[4])<0) {
				System.out.println("Reconfigurar.");
				return false;
			}else if(Integer.valueOf(propertiesAtributes[7])>65535 && Integer.valueOf(propertiesAtributes[7])<0) {
				System.out.println("Reconfigurar.");
				return false;
			}else if(Integer.valueOf(propertiesAtributes[10])<0) {
				System.out.println("Reconfigurar.");
				return false;
			}else if((propertiesAtributes[11].length() - propertiesAtributes[11].replace(".", "").length())!=3) {
				System.out.println("Reconfigurar.");
				return false;
			}else if((propertiesAtributes[3].length() - propertiesAtributes[3].replace(".", "").length())!=3) {
				System.out.println("Reconfigurar.");
				return false;
			}else if(propertiesAtributes[8].length()==0) {
				System.out.println("Reconfigurar.");
				return false;
			}else if(propertiesAtributes[2].length()==0) {
				System.out.println("Reconfigurar.");
				return false;
			}else if(propertiesAtributes[9].length()==0) {
				System.out.println("Reconfigurar.");
				return false;
			}
		}catch(NullPointerException e) {
			System.out.println("Reconfigurar.");
			return false;
		}
		return true;
	}

	private void readConfig() {
		Properties properties = new Properties();
		InputStream inputStream = null;
		try {
			inputStream = new FileInputStream(CONFIG_DIR);
			properties.load(inputStream);
			propertiesAtributes = new String[properties.size()];
			int i = 0;
			for(Object prop : properties.keySet()) {
				propertiesAtributes[i++] = (String) properties.get(prop);
			}
		} catch (IOException e) {
			propertiesAtributes = null;
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void makeMigration() {
		//TESTAR MIGRACAO
		boolean sucess = false;

		try {
			//INICIA AS LIGACOES AS DB
			SybaseDAO sybase = new SybaseDAO();
			MongoDAO mongo = new MongoDAO();

			//CONNECTORS
			Connection con = sybase.connect(propertiesAtributes, validateConfig(propertiesAtributes));


			MongoClient mongoclient = mongo.connect(propertiesAtributes, validateConfig(propertiesAtributes));
			MongoDatabase database = mongo.databaseConnect(mongoclient,propertiesAtributes, validateConfig(propertiesAtributes));

			//TRATAR DATA
			sybase.inicioMigracao(con);
			BSONTimestamp timestamp = new BSONTimestamp(sybase.selectRecentDate(con), 1);
			BSONTimestamp timestampAtual = new BSONTimestamp(sybase.selectCurrentDate(con), 1);
			System.out.println(timestamp);System.out.println(timestampAtual);

			//VAI BUSCAR AO MONGO DADOS APOS A DATA E TEMPO QUE PASSO
			FindIterable<Document> dadosRecebidosMongo = mongo.getRecentResults(database,timestamp,timestampAtual,propertiesAtributes, validateConfig(propertiesAtributes));


			//CRIAR O STATEMENT PARA FAZER O BULK INSERT
			PreparedStatement ps = null;
			boolean flag = true;
			//TRATAR DADOS PARA O INSERT DataMedicao, HoraMedicao, ValorMedicaoTemperatura, ValorMedicaoHumidade
			for(Document d : dadosRecebidosMongo){
				System.out.println(d.toJson());
				String ValorMedicaoTemperatura = String.valueOf(d.get("temperature"));
				String ValorMedicaoHumidade = String.valueOf(d.get("humidity"));
				String DataMedicao = String.valueOf(d.get("date"));
				String HoraMedicao = String.valueOf(d.get("time"));
				if(flag) {
					ps = sybase.createStatement(con, DataMedicao, HoraMedicao, ValorMedicaoTemperatura, ValorMedicaoHumidade);
					flag = false;
				}else {
					sybase.prepareInsert(ps, DataMedicao, HoraMedicao, ValorMedicaoTemperatura, ValorMedicaoHumidade);
					sucess = true;
				}
			}

			try {
				if(dadosRecebidosMongo.first() != null) {
					sybase.executeStatement(ps);
				}else {
					sucess = true;
				}
			}catch(SQLException e) {
				sucess = false;
			}

			//ESCREVER DATA DE FIM NA MIGRACAO
			if(sucess)
				sybase.fimMigracao(con);

			//DISCONNECTORS
			sybase.disconnect(con);
			mongo.disconnect(mongoclient);
		}catch(MongoTimeoutException e) {
			System.out.println("N�o existe ligacao ao mongo.");
		}catch(NullPointerException f) {
			System.out.println("Nao existe ligacao ao sybase.");
			f.printStackTrace();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

    public static void main( String args[] ){
        Exporter exportador = new Exporter();
        MakeMigration migration = new MakeMigration(exportador);
        Timer timer = new Timer();
        timer.schedule(migration, 0, 1000 * exportador.getPeriodicity());
        try {
            WatchService watchService =FileSystems.getDefault().newWatchService();
            String dir = System.getProperty("user.dir");
            Path path = Paths.get(dir);
            path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
            WatchKey key;
            while ((key = watchService.take()) != null) {
                for (WatchEvent<?> event : key.pollEvents()) {
                    timer.cancel();
                    exportador = new Exporter();
                    migration = new MakeMigration(exportador);
                    timer = new Timer();
                    timer.schedule(migration, 0, 1000 * exportador.getPeriodicity());
                }
            }
            key.reset();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}