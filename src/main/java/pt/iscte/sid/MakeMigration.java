package pt.iscte.sid;

import java.util.TimerTask;

public class MakeMigration extends TimerTask {

    Exporter exportador = null;

    public MakeMigration(Exporter exportador) {
        this.exportador = exportador;
    }

    @Override
    public void run() {
        exportador.makeMigration();
    }



}
