package com.foranx.cooladapter.config;
import java.io.IOException;
import java.util.logging.*;

public class LoggingConfiguration {
    private LoggingConfiguration() {}

    public static void init(AppConfiguration config) {
        try {
            LogManager.getLogManager().reset();
            Logger root = Logger.getLogger("");
            root.setLevel(Level.INFO);
            FileHandler fh = new FileHandler(config.getLogFolder(), true);
            fh.setFormatter(new SimpleFormatter());
            root.addHandler(fh);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize logging", e);
        }
    }
}
