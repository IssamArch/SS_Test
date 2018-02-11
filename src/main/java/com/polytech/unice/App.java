package com.polytech.unice;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import java.io.*;
import java.nio.charset.Charset;
import org.mariuszgromada.math.mxparser.Argument;
import org.mariuszgromada.math.mxparser.Expression;
import java.util.HashMap;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
public class App {
    public static class Measurement<T> {
        private String sensorName;
        private Long timeStamp;
        private T value;
        public Measurement(String sensorName, long timeStamp, T value) {
            this.sensorName = sensorName;
            this.timeStamp = timeStamp;
            this.value = value;
        }
        public Long getTimeStamp() {
            return timeStamp;
        }
        public T getValue() {
            return value;
        }
        public String getSensorName() {
            return sensorName;
        }
        @Override
        public String toString() {
            return "Measurement{" +
                    "sensorName='" + sensorName + '\'' +
                    ", timeStamp=" + timeStamp +
                    ", value=" + value +
                    '}';
        }
    }
    public static InfluxDB influxDB;
    public static String dbName;
    public static void createDataBase(String name, int port){
        influxDB = InfluxDBFactory.connect("http://localhost:"+port, "root", "root");
        dbName = name;
        if(!influxDB.databaseExists(name)){
            influxDB.createDatabase(name);
        }
    }

    // methode Send to influx Db
    public static void sendToInfluxDB(List<Measurement> measurements) {
        BatchPoints batchPoints = BatchPoints
                .database(dbName)
                .consistency(InfluxDB.ConsistencyLevel.ALL)
                .build();
        for (Measurement measurement : measurements) {
            Map<String, Object> map = new HashMap<>();
            map.put(measurement.getSensorName(), measurement.getValue());
            Point point = Point.measurement(measurement.getSensorName())
                    .time(measurement.getTimeStamp(), TimeUnit.MILLISECONDS)
                    .fields(map)
                    .build();
            batchPoints.point(point);
        }
        influxDB.write(batchPoints);
    }

    public static Measurement createrandomLow(String nameS) {
        String name = nameS;
        long timestamp = System.currentTimeMillis();
        Random random = new Random();
        int randomNumber = random.nextInt(100000000 + 1 - 50000000) + 50000000;
        int r =random.nextInt(2)+1;
        Long t ;
        if(r==1) {
            t = timestamp + randomNumber;
        }else{
            t= timestamp - randomNumber;
        }
        int value = new Random().nextInt() % 10;
        Measurement measurement = new Measurement(name,t,value);
        System.out.println("      new measurement for " + name + " from random law !" );
        return measurement;
    }

    public static Measurement createCSVLow(final String path,String n_sensor,String n_value,String n_time,int n_offset,int t) {
        try {
            File data = new File(path);
            CSVParser parser = CSVParser.parse(data, Charset.defaultCharset(), CSVFormat.DEFAULT);
            List<CSVRecord> list = parser.getRecords();
            list.remove(0);
            if (list.size() <= t) {
                return null;
            }
            CSVRecord ligne = list.get(t);
            System.out.println("      new measurement for " + ligne.get(Integer.parseInt(n_sensor)).trim() + " from file csv law !" );
            return new Measurement<>(ligne.get(Integer.parseInt(n_sensor)).trim(),Long.parseLong(ligne.get(Integer.parseInt(n_time)).trim()),Integer.parseInt(ligne.get(Integer.parseInt(n_value)).trim()) + n_offset);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Measurement createJSONLow(final String path,String n_sensor,String n_value,String n_time,int n_offset,int t) {
        try {
            JSONParser parser = new JSONParser();
            JSONArray jsonArray = (JSONArray) parser.parse(new FileReader(path));
            if(t >= 0 && t <= jsonArray.size() - 1) {
                Object o = jsonArray.get(t);
                JSONObject person = (JSONObject) o;
                String sensorName = (String) person.get(n_sensor);
                Object value = (Object) person.get(n_value);
                Long time = (Long) person.get( n_time);
                System.out.println("      new measurement for " + n_sensor + " from file json law !" );
                return new Measurement<>(sensorName,time,value);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    public static Measurement createfilelow(final String file,String n_sensor,String n_value,String n_time,String dataSource,int n_offset,int t){
        switch (dataSource){
            case "csv":
                Measurement measurement = createCSVLow(file,n_sensor,n_value,n_time,n_offset,t);
                return measurement;
            case "json":
                Measurement measurement1 =createJSONLow(file,n_sensor,n_value,n_time,n_offset,t) ;
                return measurement1;
            default:
                System.out.println("not valide low file");
        }
        return null;
    }
    public static Measurement createLawFunction(String sensName, Map<String,String> funcs, int t ) {
        Object value;
        String function = "iff(";
        for(Map.Entry<String,String> entry : funcs.entrySet()){
            function+= entry.getKey() +"," + entry.getValue()+",";
        }
        function +=")";
        String s = function.substring(0,function.length()-2);
        s+=")";
        Argument x = new Argument("x");
        x.setArgumentValue(t);
        Expression e = new Expression(s, x);
        double result  = e.calculate();
        System.out.println("      new measurement for " + sensName + " from function law ! :" );
        System.out.println("       "+ e.getExpressionString()+ "="+ result + " when x = " + x.getArgumentValue()) ;
        value = result;
        return new Measurement(sensName, System.currentTimeMillis(), value);
    }

    public static void main(String[] args){
        createDataBase("my_database",8086);
        Thread functionlot = new Thread("functionlot") {
            public void run(){
                System.out.println("run by: " + getName());
                for(int t =0; t < 10;t++){
                    List<Measurement> measurements = new ArrayList<>();
                    Map<String,String> listPoly =  new HashMap<>();
                    Map<String,String> listProb =  new HashMap<>();
                    for(int i = 0; i < 1;i++){
                        String sensName;
                        sensName =" functionlot"+Integer.toString(i);
                        Measurement measurement = createfilelow("/home/user/Bureau/testShel/ex.json","sensorName","value","time","json",0,t);
                        if (measurement == null) {
                            continue;
                        }
                        measurements.add(measurement);
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.println("send list n° "+ t + " of measurements to influxDB : "+ measurements);
                    sendToInfluxDB(measurements);
                }
            }
        };
        functionlot.start();

        Thread randomLot = new Thread("randomLot") {
            public void run(){
                System.out.println("run by: " + getName());
                for(int t =0; t < 10;t++){
                    List<Measurement> measurements = new ArrayList<>();
                    Map<String,String> listPoly =  new HashMap<>();
                    Map<String,String> listProb =  new HashMap<>();
                    for(int i = 0; i < 2;i++){
                        String sensName;
                        sensName =" randomLot"+Integer.toString(i);
                        Measurement measurement = createrandomLow(sensName);
                        if (measurement == null) {
                            continue;
                        }
                        measurements.add(measurement);
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.println("send list n° "+ t + " of measurements to influxDB : "+ measurements);
                    sendToInfluxDB(measurements);
                }
            }
        };
        randomLot.start();

        Thread csvLow = new Thread("csvLow") {
            public void run(){
                System.out.println("run by: " + getName());
                for(int t =0; t < 10;t++){
                    List<Measurement> measurements = new ArrayList<>();
                    Map<String,String> listPoly =  new HashMap<>();
                    Map<String,String> listProb =  new HashMap<>();
                    for(int i = 0; i < 1;i++){
                        String sensName;
                        sensName =" csvLow"+Integer.toString(i);
                        Measurement measurement = createfilelow("/home/user/Bureau/testShel/data4.csv","1","8","0","csv",0,t);
                        if (measurement == null) {
                            continue;
                        }
                        measurements.add(measurement);
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.println("send list n° "+ t + " of measurements to influxDB : "+ measurements);
                    sendToInfluxDB(measurements);
                }
            }
        };
        csvLow.start();

        Thread jsonLaw = new Thread("jsonLaw") {
            public void run(){
                System.out.println("run by: " + getName());
                for(int t =0; t < 10;t++){
                    List<Measurement> measurements = new ArrayList<>();
                    Map<String,String> listPoly =  new HashMap<>();
                    Map<String,String> listProb =  new HashMap<>();
                    for(int i = 0; i < 1;i++){
                        String sensName;
                        sensName =" jsonLaw"+Integer.toString(i);
                        Measurement measurement = createfilelow("/home/user/Bureau/testShel/ex.json","sensorName","value","time","json",0,t);
                        if (measurement == null) {
                            continue;
                        }
                        measurements.add(measurement);
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.println("send list n° "+ t + " of measurements to influxDB : "+ measurements);
                    sendToInfluxDB(measurements);
                }
            }
        };
        jsonLaw.start();

    }
}
