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
import net.andreinc.mockneat.MockNeat;
import net.andreinc.mockneat.unit.objects.Probabilities;
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

    public static class Pair<K, T> {
        private K key;
        private T value;
        public Pair(K key, T value){
            this.key = key;
            this.value = value;
        }
        public K getKey() {
            return key;
        }
        public T getValue() {
            return value;
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
    public static ArrayList<Integer> remplirRandom(int nbr){
        ArrayList<Integer> listeRandom = new ArrayList<>();
        for(int i = 0;i < 50;i++){
            Random random = new Random();
            int randomNumber = random.nextInt(100000000 + 1 - 50000000) + 50000000;
            listeRandom.add(randomNumber);
        }
        Collections.sort(listeRandom);
        return  listeRandom;
    }
    public  static ArrayList<Integer> listeRandom = remplirRandom(50);
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
        long timestamp = System.currentTimeMillis();
        Long tt ;
        tt = timestamp + listeRandom.get(t);
        Measurement measurement = new Measurement(sensName,tt,value);
        return measurement;
    }

    public static Measurement createMarkovLow(String sensor, List<Pair<String, String>> input, int t) {
        List<List<Double>> matrice = new ArrayList<List<Double>>();
        HashMap<String, List<Double>> myInput = new HashMap<String, List<Double>>();
        int currState = 0;
        for(Pair<String, String> p : input){
            if(myInput.containsKey(p.getKey())){
                myInput.get(p.getKey()).add(Double.parseDouble(p.getValue()));
            }else{
                List<Double> liste = new ArrayList<Double>();
                liste.add(Double.parseDouble(p.getValue()));
                myInput.put(p.getKey(), liste);
            }
        }
        for(String key : myInput.keySet()){
            List<Double> ligne = new ArrayList<Double>();
            for(Double f : myInput.get(key)) {
                ligne.add(f);
            }
            matrice.add(ligne);
        }
        MockNeat mockNeat = MockNeat.threadLocal();
        Probabilities<Integer> p = mockNeat.probabilites(Integer.class);
        for(int i = 0 ; i <matrice.size(); i++){
            p.add(matrice.get(currState).get(i), i);
        }
        currState = p.val();
        long timestamp = System.currentTimeMillis();
        Long tt ;
        tt = timestamp + listeRandom.get(t);
        Measurement measurement = new Measurement(sensor,tt,currState);
        return measurement;
    }
    // chaos
    static Boolean gorilla_Exist =false;
    static Boolean gorilla = false;
    static String risque = "average";
    public static int RandomGorilla(int duration,String risque){
        if(risque == "low"){
            Random random = new Random();
            int randomNumber = random.nextInt(duration * 2 +1  - 0) + 0;
            return randomNumber;
        }
        else if(risque == "average") {
            Random random = new Random();
            int randomNumber = random.nextInt(duration  +1  - 0) + 0;
            return randomNumber;
        }
        else if(risque ==  "strong"){
            Random random = new Random();
            int randomNumber = random.nextInt((duration/2)  - 0) + 0;
            return randomNumber;
        }
        return 1;
    }
    static String risqueMonkey = "strong";
    static Boolean monkey=false;
    public static ArrayList<Integer> randomMonkey(int nbrSensor, String risque){
        ArrayList<Integer> temp = new ArrayList<>();
        int size = 0;
        if(risque == "strong" && monkey){
            Random random = new Random();
            int randomNumber = random.nextInt(nbrSensor * 2 +1  - 0) + 0;
            size =  randomNumber;
        }
        else if(risque == "average" && monkey) {
            Random random = new Random();
            size = random.nextInt(nbrSensor  +1  - 0) + 0;
        }
        else if(risque ==  "low" && monkey){
            Random random = new Random();
            size= random.nextInt((nbrSensor/2)  - 0) + 0;
        }
        for (int i =0; i< size;i++){
            Random random = new Random();
            temp.add(random.nextInt(nbrSensor +1  - 0) + 0);
        }
        return temp;
    }
    public static void main(String[] args){
        createDataBase("my_database",8086);
        Thread parkingRandom = new Thread("parkingRandom") {
            public void run(){
                int dureeMaxi = 0;
                try {
                    while (gorilla == false && dureeMaxi < 12) {
                        System.out.println("run by: " + getName());
                        ArrayList<Integer> listeRandom = remplirRandom(12);
                        ArrayList<Integer> listeMonkey = randomMonkey(1,risqueMonkey);
                        int k = RandomGorilla(12,risque);
                        for(int t =0; t < 12;t++){
                            List<Measurement> measurements = new ArrayList<>();
                            Map<String,String> listPoly =  new HashMap<>();
                            List<Pair<String, String>> listMarkov = new ArrayList<Pair<String, String>>();
                            for(int i = 0; i < 1;i++){
                                String sensName;
                                sensName =" parkingRandom"+Integer.toString(i);
                                Measurement measurement ;
                                if( listeMonkey.contains(i)){
                                    measurement = null;
                                    System.out.println(" monkey attaque sensor num! "+ i);
                                } else {
                                    measurement = createrandomLow(sensName);
                                }
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
                            if(t ==k && gorilla_Exist == true){
                                gorilla = true;
                            }
                            if(gorilla == true){
                                System.out.println("Gorilla attack !!! we can't finished "+(12- t )+" lists from our 12 lists");
                                break;
                            }
                            dureeMaxi++;
                        }
                        Thread.sleep (2000);
                    }
                }
                catch (InterruptedException exception){}
            }
        };
        parkingRandom.start();

        Thread jardinCsv = new Thread("jardinCsv") {
            public void run(){
                int dureeMaxi = 0;
                try {
                    while (gorilla == false && dureeMaxi < 10) {
                        System.out.println("run by: " + getName());
                        ArrayList<Integer> listeRandom = remplirRandom(10);
                        ArrayList<Integer> listeMonkey = randomMonkey(1,risqueMonkey);
                        int k = RandomGorilla(10,risque);
                        for(int t =0; t < 10;t++){
                            List<Measurement> measurements = new ArrayList<>();
                            Map<String,String> listPoly =  new HashMap<>();
                            List<Pair<String, String>> listMarkov = new ArrayList<Pair<String, String>>();
                            for(int i = 0; i < 1;i++){
                                String sensName;
                                sensName =" jardinCsv"+Integer.toString(i);
                                Measurement measurement ;
                                if( listeMonkey.contains(i)){
                                    measurement = null;
                                    System.out.println(" monkey attaque sensor num! "+ i);
                                } else {
                                    measurement = createfilelow("/home/user/Bureau/dataDemo/dataCsv.csv","1","8","0","csv",0,t);
                                }
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
                            if(t ==k && gorilla_Exist == true){
                                gorilla = true;
                            }
                            if(gorilla == true){
                                System.out.println("Gorilla attack !!! we can't finished "+(10- t )+" lists from our 10 lists");
                                break;
                            }
                            dureeMaxi++;
                        }
                        Thread.sleep (2000);
                    }
                }
                catch (InterruptedException exception){}
            }
        };
        jardinCsv.start();

        Thread bureauJson = new Thread("bureauJson") {
            public void run(){
                int dureeMaxi = 0;
                try {
                    while (gorilla == false && dureeMaxi < 10) {
                        System.out.println("run by: " + getName());
                        ArrayList<Integer> listeRandom = remplirRandom(10);
                        ArrayList<Integer> listeMonkey = randomMonkey(1,risqueMonkey);
                        int k = RandomGorilla(10,risque);
                        for(int t =0; t < 10;t++){
                            List<Measurement> measurements = new ArrayList<>();
                            Map<String,String> listPoly =  new HashMap<>();
                            List<Pair<String, String>> listMarkov = new ArrayList<Pair<String, String>>();
                            for(int i = 0; i < 1;i++){
                                String sensName;
                                sensName =" bureauJson"+Integer.toString(i);
                                Measurement measurement ;
                                if( listeMonkey.contains(i)){
                                    measurement = null;
                                    System.out.println(" monkey attaque sensor num! "+ i);
                                } else {
                                    measurement = createfilelow("/home/user/Bureau/dataDemo/dataJson.json","sensorName","value","time","json",4,t);
                                }
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
                            if(t ==k && gorilla_Exist == true){
                                gorilla = true;
                            }
                            if(gorilla == true){
                                System.out.println("Gorilla attack !!! we can't finished "+(10- t )+" lists from our 10 lists");
                                break;
                            }
                            dureeMaxi++;
                        }
                        Thread.sleep (2000);
                    }
                }
                catch (InterruptedException exception){}
            }
        };
        bureauJson.start();

        Thread salleFunction = new Thread("salleFunction") {
            public void run(){
                int dureeMaxi = 0;
                try {
                    while (gorilla == false && dureeMaxi < 10) {
                        System.out.println("run by: " + getName());
                        ArrayList<Integer> listeRandom = remplirRandom(10);
                        ArrayList<Integer> listeMonkey = randomMonkey(1,risqueMonkey);
                        int k = RandomGorilla(10,risque);
                        for(int t =0; t < 10;t++){
                            List<Measurement> measurements = new ArrayList<>();
                            Map<String,String> listPoly =  new HashMap<>();
                            List<Pair<String, String>> listMarkov = new ArrayList<Pair<String, String>>();
                            for(int i = 0; i < 1;i++){
                                String sensName;
                                sensName =" salleFunction"+Integer.toString(i);
                                listPoly.put("x<1","2");
                                listPoly.put("x>= 1 && x<=3","x^2-3");
                                listPoly.put("x>3","abs(-2*x)");
                                Measurement measurement ;
                                if( listeMonkey.contains(i)){
                                    measurement = null;
                                    System.out.println(" monkey attaque sensor num! "+ i);
                                } else {
                                    measurement= createLawFunction(sensName,listPoly,t);
                                }
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
                            if(t ==k && gorilla_Exist == true){
                                gorilla = true;
                            }
                            if(gorilla == true){
                                System.out.println("Gorilla attack !!! we can't finished "+(10- t )+" lists from our 10 lists");
                                break;
                            }
                            dureeMaxi++;
                        }
                        Thread.sleep (2000);
                    }
                }
                catch (InterruptedException exception){}
            }
        };
        salleFunction.start();

        Thread garageMarkov = new Thread("garageMarkov") {
            public void run(){
                int dureeMaxi = 0;
                try {
                    while (gorilla == false && dureeMaxi < 10) {
                        System.out.println("run by: " + getName());
                        ArrayList<Integer> listeRandom = remplirRandom(10);
                        ArrayList<Integer> listeMonkey = randomMonkey(1,risqueMonkey);
                        int k = RandomGorilla(10,risque);
                        for(int t =0; t < 10;t++){
                            List<Measurement> measurements = new ArrayList<>();
                            Map<String,String> listPoly =  new HashMap<>();
                            List<Pair<String, String>> listMarkov = new ArrayList<Pair<String, String>>();
                            for(int i = 0; i < 1;i++){
                                String sensName;
                                sensName =" garageMarkov"+Integer.toString(i);
                                listMarkov.add(new Pair<String, String>("sunny","0.9"));
                                listMarkov.add(new Pair<String, String>("sunny","0.05"));
                                listMarkov.add(new Pair<String, String>("sunny","0.05"));
                                listMarkov.add(new Pair<String, String>("runny","0.4"));
                                listMarkov.add(new Pair<String, String>("runny","0.4"));
                                listMarkov.add(new Pair<String, String>("runny","0.2"));
                                listMarkov.add(new Pair<String, String>("cloudy","0.4"));
                                listMarkov.add(new Pair<String, String>("cloudy","0.5"));
                                listMarkov.add(new Pair<String, String>("cloudy","0.1"));
                                Measurement measurement ;
                                if( listeMonkey.contains(i)){
                                    measurement = null;
                                    System.out.println(" monkey attaque sensor num! "+ i);
                                } else {
                                    measurement = createMarkovLow(sensName,listMarkov,t);
                                }
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
                            if(t ==k && gorilla_Exist == true){
                                gorilla = true;
                            }
                            if(gorilla == true){
                                System.out.println("Gorilla attack !!! we can't finished "+(10- t )+" lists from our 10 lists");
                                break;
                            }
                            dureeMaxi++;
                        }
                        Thread.sleep (2000);
                    }
                }
                catch (InterruptedException exception){}
            }
        };
        garageMarkov.start();

    }
}
