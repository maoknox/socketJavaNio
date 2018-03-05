/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bDatos;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 *
 * @author USER
 */
public class DbConnect {
    String driver = "org.postgresql.Driver"; // el nombre de nuestro driver Postgres.
//    String connectString = "jdbc:postgresql://ec2-174-129-242-241.compute-1.amazonaws.com:5432/d1b9pv88jl26t9?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"; // llamamos         
//    String user = "ajrbjyuywyluwm"; // usuario postgres
//    String password = "sRVVqYKhD2bdn_qWz6tCYSP71h"; // no tiene password nuestra bd.
    String connectString = "jdbc:postgresql://localhost:5432/telemed"; // llamamos         
    String user = "postgres"; // usuario postgres
    String password = "root"; // no tiene password nuestra bd.
    static final Logger logger = Logger.getLogger("MyLog");
    public Connection conecta() throws ClassNotFoundException, IOException
    {
        FileHandler fh = new FileHandler("/javaprog/LogFile.log", true);
        try
        {
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(connectString, user, password);
            return conn;
        }
        catch(SQLException e)
        {
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);
            logger.info(e.getMessage());
            return null;
        }
    }
}