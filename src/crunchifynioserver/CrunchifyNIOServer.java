/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package crunchifynioserver;

import bDatos.DbConnect;
import static crunchifynioserver.CrunchifyNIOServer.formatter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
 
/**
 * @author Crunchify.com
 *
 */

public class CrunchifyNIOServer {
    //Map para almacenar clave valor para comados en telecontrol
    static Map<String, HashMap<String, String>> comandos = Collections.synchronizedMap(new HashMap<String, HashMap<String, String>>());
    //Base de datos
    static Connection conect;
    static DbConnect con = new DbConnect();
    static SimpleFormatter formatter = new SimpleFormatter();;
    static final Logger logger = Logger.getLogger("MyLog");
    static InetSocketAddress crunchifyAddr;
    static ServerSocketChannel crunchifySocket;
    //static SocketChannel crunchifyClient;
	@SuppressWarnings("unused")
	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException{
            FileHandler fh = new FileHandler("/javaprog/LogFile.log", true);
//            if (args.length < 2) {
//                logger.addHandler(fh);
//                fh.setFormatter(formatter);
//                logger.info("Debe digitar puerto e ip");
//                System.exit(1);
//            }
            
            Selector selector = Selector.open();
            int port = Integer.valueOf(args[0]);
//            int port = Integer.valueOf("8010");
            //String address = args[1];
            try {
                crunchifySocket = ServerSocketChannel.open();//172.31.27.242
                crunchifyAddr = new InetSocketAddress("172.31.27.242", port);
//                crunchifyAddr = new InetSocketAddress("192.168.43.145", port);
                crunchifySocket.bind(crunchifyAddr);
            }
            catch (UnresolvedAddressException e) {
                logger.addHandler(fh);
                fh.setFormatter(formatter);
                String mensaje = "No se puede contectar a la ip:" + args[1] + " " + e.getMessage();
                logger.info(mensaje);
                System.exit(1);
            }
            crunchifySocket.configureBlocking(false);
            int ops = crunchifySocket.validOps();
            SelectionKey selectKy = crunchifySocket.register(selector, ops, null);
            conect = con.conecta();
            if (conect == null) {
                System.exit(1);
            }
            while(true){
                selector.select();
                Set<SelectionKey> crunchifyKeys = selector.selectedKeys();
                Iterator<SelectionKey> crunchifyIterator = crunchifyKeys.iterator();
                while (crunchifyIterator.hasNext()) {
                    SelectionKey myKey = crunchifyIterator.next();
                    if (myKey.isAcceptable()) {
                            //crea instancia de cliente
                        SocketChannel crunchifyClient = crunchifySocket.accept();
                        // Adjusts this channel's blocking mode to false
                        crunchifyClient.configureBlocking(false);
                        // Operation-set bit for read operations
                        crunchifyClient.register(selector, SelectionKey.OP_READ);
                        //log("Connection Accepted: " + crunchifyClient.getLocalAddress() + "\n");
                        // Tests whether this key's channel is ready for reading
                    }
                    else if (myKey.isReadable()) {
//                         try (SocketChannel crunchifyClient = (SocketChannel) myKey.channel()) {
                        try  {
                            SocketChannel crunchifyClient = (SocketChannel) myKey.channel();
                            ByteBuffer crunchifyBuffer = ByteBuffer.allocate(2048);
                            try{
                                crunchifyClient.read(crunchifyBuffer);
                                String result = new String(crunchifyBuffer.array()).trim();
                                if (result.length() > 0) {
                                    String primerCaracter = result.substring(0, 1);
                                    String ultimoCaracter = result.substring(result.length() - 1);
                                    if ("!".equals(primerCaracter) && "*".equals(ultimoCaracter)) {
                                        String[] tramaSplit = result.substring(1, result.length()-1).split(",");
                                        if("C".equals(tramaSplit[0])) {
                                            if (!comandos.containsKey(tramaSplit[1])) {
                                                comandos.put(tramaSplit[1], new HashMap<>());
                                            }
                                            comandos.get(tramaSplit[1]).put(tramaSplit[0], result);
                                        } else if ("N".equals(tramaSplit[0])) {
                                            if (!comandos.containsKey(tramaSplit[1])) {
                                                comandos.put(tramaSplit[1], new HashMap<>());
                                            }
                                            comandos.get(tramaSplit[1]).put(tramaSplit[0], result);
                                        } else {
                                            int hexi = 13;
                                            int hex = 10;
                                            if (comandos.containsKey(tramaSplit[1])) {
                                                comandos.get(tramaSplit[1]).forEach(
                                                    (k, v) -> {
                                                        try {
                                                            ByteBuffer bufComandos = ByteBuffer.wrap((v + (char)hexi + (char)hex).getBytes());
                                                            crunchifyClient.write(bufComandos);
                                                        }
                                                        catch (IOException ex) {
                                                            logger.addHandler(fh);
                                                            //SimpleFormatter formatter = new SimpleFormatter();
                                                            fh.setFormatter(formatter);
                                                            logger.info(ex.getMessage());
                                                        }
                                                    }
                                                );
                                                comandos.remove(tramaSplit[1]);
                                            }
                                            int idEntdev=0;
                                            ByteBuffer bufEnd = ByteBuffer.wrap(("OK" + (char)hexi + (char)hex).getBytes());
                                            crunchifyClient.write(bufEnd);
                                            try {
                                                String sql="SELECT ed.id_entdev FROM entity_device AS ed LEFT JOIN entity AS et ON et.id_entity=ed.id_entity WHERE et.entity_number=? and ed.id_device=?";
                                                try(PreparedStatement prepStmtCons = conect.prepareStatement(sql)){
                                                    prepStmtCons.setString(1, tramaSplit[0]);
                                                    prepStmtCons.setString(2, tramaSplit[1]);
                                                    ResultSet rs;
                                                    rs = prepStmtCons.executeQuery();
                                                    rs.next();
                                                    idEntdev=rs.getInt("id_entdev");
                                                    prepStmtCons.close();
                                                }
                                                catch(SQLException ex){
                                                    logger.addHandler(fh);
                                                    fh.setFormatter(formatter);
                                                    logger.info(ex.getMessage());
                                                }
                                                Date date = new Date();
                                                long time = date.getTime();
                                                Timestamp fechaRegistro = new Timestamp(time);
                                                String str1 = Arrays.toString(tramaSplit);
                                                str1 = str1.substring(1, str1.length() - 1).replaceAll(" ", "");
                                                String[] parts = str1.split(",");
                                                String[] magnitude=Arrays.copyOfRange(parts, 4, parts.length);
                                                String str2= Arrays.toString(magnitude);
                                                str2=str2.substring(1, str2.length() - 1).replaceAll(" ", "");
//                                                System.arraycopy(parts, 3, magnitude, 0, parts.length-1);
                                                String dataTest = "INSERT INTO dataframe (id_entdev,dataframe,dataframe_date) values (?,?,?);";
                                                try (PreparedStatement prepStmt = conect.prepareStatement(dataTest)) {
                                                    prepStmt.setInt(1, idEntdev);
                                                    prepStmt.setString(2,str2);
                                                    prepStmt.setTimestamp(3, fechaRegistro);
                                                    prepStmt.execute();
                                                    prepStmt.close();
                                                }
                                            }
                                            catch (SQLException ex) {
                                                logger.addHandler(fh);
                                                fh.setFormatter(formatter);
                                                logger.info(ex.getMessage());
                                            }
                                        }
                                    }
                                }
                            }
                            catch (IOException e) {
                                logger.addHandler(fh);
                                fh.setFormatter(formatter);
                                logger.info(e.getMessage());
                            }
                        }
                        catch (Exception e) {
                            logger.addHandler(fh);
                            fh.setFormatter(formatter);
                            logger.info(e.getMessage());
//                            System.exit(1);
                        }
                    }
                    crunchifyIterator.remove();
                }
            }
	}
 
	private static void log(String str) {
		System.out.println(str);
	}
}