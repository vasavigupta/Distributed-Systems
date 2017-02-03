package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.provider.Settings;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.database.sqlite.SQLiteDatabase;

public class SimpleDhtProvider extends ContentProvider {

    private SQLiteDatabase db;
    public static final String table_name = "messages";
    static final String[] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    private ContentResolver mContentResolver;
    private Uri uri;
    static String myPort="";
    Hashtable<String,LinkedList<String>> node_table = new Hashtable<String,LinkedList<String>>();

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private static class DatabaseHelper extends SQLiteOpenHelper {
        DatabaseHelper(Context context) {
            super(context, "GroupMessenger", null, 1);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL("CREATE TABLE " + table_name + " (key TEXT PRIMARY KEY, value TEXT NOT NULL, seq_num DOUBLE );");
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            db.execSQL("DROP TABLE IF EXISTS " + table_name);
            onCreate(db);
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        db.delete(table_name,"key = ?",new String[] {selection});

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }
    String predecessor_id;
    String successor_id;
    String joined = "no";

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String[] vk;
        vk = values.toString().split(" ");
        String key = vk[1].split("=")[1];
        Boolean iamstart=false;
        try {
            String key_hash = genHash(key);
            String myId = Integer.toString(Integer.parseInt(myPort) / 2);
            String myId_hash = genHash(myId);
            Log.d("InsertKey",key);
            Log.d("InsertHash",key_hash);
            Log.d("InsertKeyMyID",myId);
            if((node_table.size()==1 && myId.contains("5554")) || joined.contains("no")){
                long newRowId = db.insert(table_name, "", values);
                Log.v("insert", values.toString());
            }
            else{
                //Log.d("KeyHash > myIdhash?",String.valueOf(key_hash.compareTo(myId_hash)));
                Log.d("InsertMy predecessor",predecessor_id);
                Log.d("InsertKeyMy Successor",successor_id);

                if(genHash(predecessor_id).compareTo(myId_hash)>=1){
                    iamstart=true;
                    if(key_hash.compareTo(genHash(predecessor_id)) >=1){
                        long newRowId = db.insert(table_name, "", values);
                        Log.v("insert", values.toString());
                        return uri;
                    }
                }
                if(iamstart && myId_hash.compareTo(key_hash)  >= 1){
                    long newRowId = db.insert(table_name, "", values);
                    Log.v("insert", values.toString());
                    return uri;
                }

                if( myId_hash.compareTo(key_hash)  >= 1 && key_hash.compareTo(genHash(predecessor_id)) >=1 ) {
                    Log.d("Inside insert", "Will I ever get executed?--------------------------------------------------lets see");
                    long newRowId = db.insert(table_name, "", values);
                    Log.v("insert", values.toString());
                }
                else{
                    Log.d("Insert not in my domain","sending it to my successor: "+successor_id);
                    try {
                        Socket serverSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor_id)*2);
                        PrintWriter output = new PrintWriter(serverSocket.getOutputStream(), true);
                        output.println("Insert:"+myPort+":"+values.toString());
                        output.flush();
                    } catch (UnknownHostException e) {
                        Log.e("Simple DHT", "Unknown host exception");
                    } catch (IOException e) {
                        Log.e("Simple DHT", "IO exception");
                        e.printStackTrace();
                    }
                }
            }
        }
        catch (NoSuchAlgorithmException e){
            Log.e("Simple dht", "No such algorithm");
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        Context context = getContext();
        uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e("Simple DHT", "Can't create a ServerSocket");
        }
        try {
            TelephonyManager tel = null;
            if (context != null) {
                tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
                String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
                myPort  = String.valueOf(Integer.parseInt(portStr) * 2);
                String msg = "Join";
                Log.d("I am up", "Yes!!");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        }
        catch (NullPointerException e) {
            Log.e("SimpleDHT", "Null Pointer exception");
        }
        DatabaseHelper dbHelper = new DatabaseHelper(context);
        db = dbHelper.getWritableDatabase();
        return (db != null);

    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        try{
            Boolean iamstart=false;
            Cursor c;
            String myId = Integer.toString(Integer.parseInt(myPort) / 2);
            Log.d("QueryKey",selection);

            Context context = getContext();
            DatabaseHelper dbHelper = new DatabaseHelper(context);
            db = dbHelper.getReadableDatabase();
            Log.d("querying for :",selection);

            if(selection.equalsIgnoreCase("*") || selection.equalsIgnoreCase("@")){
                if(selection.equalsIgnoreCase("@") || ((node_table.size()==1 && myId.contains("5554")) || joined.contains("no"))) {
                    c = db.rawQuery("SELECT key,value from " + table_name, null);
                    return c;
                }
                else if(selection.equalsIgnoreCase("*") && !successor_id.isEmpty() && !successor_id.equalsIgnoreCase(myId)){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(successor_id)*2);
                    String initiator = myId;
                    PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
                    output.println("SelectAll:" + myPort + ":" + "*all*" +":"+initiator);
                    output.flush();
                    BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String inputMessage = input.readLine();
                    Log.d("Initiator * server", inputMessage);
                    MatrixCursor resultCursor= new MatrixCursor(new String[] {"key","value"});
                    String[] results = inputMessage.split("-");
                    String resultKey;
                    String resultValue;
                    for(int j=0;j<results.length;j++) {

                        String[] KV=results[j].split(":");
                        resultKey =KV[0];
                        resultValue = KV[1];
                        Log.d("Query * after server", resultKey + ":" + resultValue);
                        resultCursor.addRow(KV);
                    }
                    return  resultCursor;

                }
                /*else{
                    String remotePort;
                    MatrixCursor resultCursor= new MatrixCursor(new String[] {"key","value"});
                    try {

                        for (int i = 0; i < REMOTE_PORT.length; i++) {
                            remotePort = REMOTE_PORT[i];

                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(remotePort));

                            PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
                            output.println("Query*:" + myPort + ":" + selection);
                            output.flush();
                            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String inputMessage = input.readLine();
                            String resultsKV;
                            Log.d("* query recv frm server",inputMessage);
                            String[] results = inputMessage.split("-");
                            String resultKey;
                            String resultValue;
                            for(int j=0;j<results.length;j++) {

                                String[] KV=results[j].split(":");
                                resultKey =KV[0];
                                resultValue = KV[1];
                                Log.d("Query * after server", resultKey + ":" + resultValue);
                                resultCursor.addRow(KV);
                            }
                        }
                        return resultCursor;
                    }
                    catch (UnknownHostException e){
                        Log.e("Simple DHT","Unknown exception");
                    }
                    catch (IOException e){
                        Log.e("Simple DHT","Unknown exception");
                    }

                }*/

            }
            else{
                try {
                    String key_hash = genHash(selection);
                    String myId_hash = genHash(myId);
                    Log.d("QueryKeyHash",key_hash);
                    Log.d("QueryKeyMyID",myId);

                    if((node_table.size()==1 && myId.contains("5554")) || joined.contains("no")){
                        c = db.rawQuery("SELECT key,value from " + table_name + " where key = ?", new String[] {selection});
                        return c;
                    }
                    else{
                        Log.d("KeyHash > myIdhash?",String.valueOf(key_hash.compareTo(myId_hash)));
                        Log.d("KeyMy predecessor",predecessor_id);
                        Log.d("KeyMy Successor",successor_id);

                        if(genHash(predecessor_id).compareTo(myId_hash)>=1) {
                            iamstart = true;
                        }
                        if(iamstart && key_hash.compareTo(genHash(predecessor_id)) >=1){
                                c = db.rawQuery("SELECT key,value from " + table_name + " where key = ?", new String[] {selection});
                                return c;
                        }
                        else if(iamstart && myId_hash.compareTo(key_hash)  >= 1){
                            c = db.rawQuery("SELECT key,value from " + table_name + " where key = ?", new String[] {selection});
                            return c;
                        }

                        else if( myId_hash.compareTo(key_hash)  >= 1 && key_hash.compareTo(genHash(predecessor_id)) >=1 ) {
                            c = db.rawQuery("SELECT key,value from " + table_name + " where key = ?", new String[] {selection});
                            return c;
                        }
                        else{
                            Log.d("not in my domain","sending it to my successor: "+successor_id);
                            try {
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor_id)*2);
                                PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
                                output.println("SingleQ:"+myPort+":"+selection);
                                output.flush();
                                BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                String inputMessage = input.readLine();
                                String resultsKV;
                                Log.d("input query server", inputMessage);
                                String[] results = inputMessage.split("-");
                                String resultKey="";
                                String resultValue="";
                                String[] KV=results[0].split(":");
                                resultKey =KV[0];
                                resultValue = KV[1];
                                Log.d("queryresult", resultKey + ":" + resultValue);
                                MatrixCursor resultCursor= new MatrixCursor(new String[] {"key","value"});
                                resultCursor.addRow(KV);

                                return resultCursor;

                            } catch (UnknownHostException e) {
                                Log.e("Simple DHT", "Unknown host exception");
                            } catch (IOException e) {
                                Log.e("Simple DHT", "IO exception");
                                e.printStackTrace();
                            }
                        }
                    }

                }
                catch (NoSuchAlgorithmException e){
                    Log.e("Simple dht", "No such algorithm");
                }
            }
        }
        /*catch (UnknownHostException e){
            Log.e("Simple DHT","Unknown exception");
        }*/
        catch (Exception e){
            Log.e("Simple DHT","Unknown exception");
        }
        return null;

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            Log.d("Client", " reached");
            try {
                String[] p_id_s;
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("5554") * 2);
                PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
                output.println(msgs[0] + ":" + msgs[1]);// + ":" + "5554");
                output.flush();
                BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String inputMessage = input.readLine();
                if(!inputMessage.isEmpty()) {
                    p_id_s = inputMessage.split(":");
                    predecessor_id = p_id_s[0];
                    successor_id = p_id_s[2];
                    joined = "yes";
                    //notify predecessor to change its successor to my port
                    Socket predsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(p_id_s[0]) * 2);
                    PrintWriter predoutput = new PrintWriter(predsocket.getOutputStream(), true);
                    predoutput.println("Successor" + ":" + msgs[1]);
                    predoutput.flush();
                    //notify successor to change its predecessor to my port
                    Socket succsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(p_id_s[2]) * 2);
                    PrintWriter succoutput = new PrintWriter(succsocket.getOutputStream(), true);
                    succoutput.println("Predecessor" + ":" + msgs[1]);
                    succoutput.flush();
                    try{
                        Log.d("5554",genHash("5554"));
                        Log.d("5556",genHash("5556"));
                        Log.d("5558",genHash("5558"));
                        Log.d("5560",genHash("5560"));
                        Log.d("5562",genHash("5562"));
                    }
                    catch (NoSuchAlgorithmException e)
                    {
                        Log.e("Simple DHT","No such algorithm exception");
                    }
                }

            }catch (InterruptedIOException e){
                Log.e("Stand Alone","????????????????????????????????????????????????");
            }catch (UnknownHostException e) {
                Log.e("Simple DHT", "Unknown host exception");
            } catch (IOException e) {
                Log.e("Simple DHT", "IO exception");
                e.printStackTrace();
            }
            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            String [] request;
            Socket clientSocket ;
            try{
                while(true) {
                    clientSocket = serverSocket.accept();
                    BufferedReader inFromClient = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    String requestStr = inFromClient.readLine();
                    request = requestStr.split(":");
                    //Log.d("My raw request is" , requestStr);
                    String reqType = request[0];
                    String cliPort = Integer.toString(Integer.parseInt(request[1])/2);
                    Log.d("I received",reqType +" from "+cliPort);
                    if(reqType.contains("Join")){
                        newjoin(node_table, cliPort);
                        PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
                        output.println( node_table.get(cliPort).peekFirst() +":"+ cliPort +":" +node_table.get(cliPort).peekLast() );
                        output.flush();
                        clientSocket.close();
                    }
                    else if(reqType.contains("Predecessor")){
                        predecessor_id = cliPort;
                    }
                    else if(reqType.contains("Successor")){
                        successor_id = cliPort;
                    }
                    else if(reqType.contains("Insert")){
                        Log.d("Insert req",request[0] + ":" + request[1] +":" + request[2]);
                        String values = request[2];
                        String[] vk;
                        vk = values.split(" ");
                        String key = vk[1].split("=")[1];
                        String value = vk[0].split("=")[1];
                        ContentValues cv;
                        cv = new ContentValues();
                        //Log.d("key",key);
                        //Log.d("value",value);
                        cv.put("key", key);
                        cv.put("value", value);
                        Log.d("CV", cv.toString());
                        try {
                            insert(uri, cv);
                            //Log.d("insert","need to call");
                        } catch (Exception e) {
                            Log.e("Simple DHT", e.toString());
                            e.printStackTrace();
                        }
                    }
                    else if(reqType.contains("SingleQ")){
                        Log.d("Query req", request[0] + ":" + request[1] + ":" + request[2]);
                        Cursor resultCursor;// = query(uri, null,request[2], null, null);


                        String selection=request[2];
                        Boolean iamstart=false;
                        //Cursor c;
                        String myId = Integer.toString(Integer.parseInt(myPort) / 2);
                        Log.d("QueryKey",selection);
                        if(selection.contains("*") || selection.contains("@")){
                            resultCursor = query(uri, null,request[2], null, null);
                            String results="";
                            if (resultCursor != null) {
                                resultCursor.moveToFirst();

                                while (!resultCursor.isAfterLast()) {
                                    String resultKV=resultCursor.getString(0)+":" + resultCursor.getString(1);
                                    Log.d("resultcursor", resultKV);
                                    results = resultKV+"-"+results;
                                    resultCursor.moveToNext();
                                }

                                PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
                                output.println(results);
                                output.flush();
                                resultCursor.close();
                                clientSocket.close();
                            }
                        }
                        else if((node_table.size()==1 && myId.contains("5554")) || joined.contains("no")){
                            resultCursor = query(uri, null,request[2], null, null);
                            String results="";
                            if (resultCursor != null) {
                                resultCursor.moveToFirst();

                                while (!resultCursor.isAfterLast()) {
                                    String resultKV=resultCursor.getString(0)+":" + resultCursor.getString(1);
                                    Log.d("resultcursor", resultKV);
                                    results = resultKV+"-"+results;
                                    resultCursor.moveToNext();
                                }

                                PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
                                output.println(results);
                                output.flush();
                                resultCursor.close();
                                clientSocket.close();
                            }
                        }
                        else{
                            try {
                                String key_hash = genHash(selection);
                                String myId_hash = genHash(myId);
                                Log.d("QueryKeyHash",key_hash);
                                Log.d("QueryMyID",myId);
                                Log.d("QueryMyIDHash",myId_hash);

                                if((node_table.size()==1 && myId.contains("5554")) || joined.contains("no")){
                                    resultCursor = query(uri, null, request[2], null, null);
                                    String results="";
                                    if (resultCursor != null) {
                                        resultCursor.moveToFirst();

                                        while (!resultCursor.isAfterLast()) {
                                            String resultKV=resultCursor.getString(0)+":" + resultCursor.getString(1);
                                            Log.d("resultcursor", resultKV);
                                            results = resultKV+"-"+results;
                                            resultCursor.moveToNext();
                                        }

                                        PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
                                        output.println(results);
                                        output.flush();
                                        resultCursor.close();
                                        clientSocket.close();
                                    }
                                }
                                else{
                                    Log.d("KeyHash > myIdhash?",String.valueOf(key_hash.compareTo(myId_hash)));
                                    Log.d("KeyMy predecessor",predecessor_id);
                                    Log.d("KeyMy Successor",successor_id);
                                    Log.d("predhasg>myhash?",String.valueOf(genHash(predecessor_id).compareTo(myId_hash)));

                                    if(genHash(predecessor_id).compareTo(myId_hash)>=1){
                                        iamstart=true;}

                                    if(iamstart && key_hash.compareTo(genHash(predecessor_id)) >=1){
                                            Log.d("Query Executed if","key_hash is greatest and I am start");
                                            resultCursor = query(uri, null, request[2], null, null);
                                            String results="";
                                        if (resultCursor != null) {
                                                resultCursor.moveToFirst();

                                                while (!resultCursor.isAfterLast()) {
                                                    String resultKV=resultCursor.getString(0)+":" + resultCursor.getString(1);
                                                    Log.d("resultcursor", resultKV);
                                                    results = resultKV+"-"+results;
                                                    resultCursor.moveToNext();
                                                }

                                                PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
                                                output.println(results);
                                                output.flush();
                                                resultCursor.close();
                                                clientSocket.close();
                                        }
                                    }
                                    else if(iamstart && myId_hash.compareTo(key_hash)  >= 1){
                                        Log.d("Query Executed if","key_hash is smallest and I am start");
                                        resultCursor = query(uri, null, request[2], null, null);
                                        String results="";
                                        if (resultCursor != null) {
                                            resultCursor.moveToFirst();

                                            while (!resultCursor.isAfterLast()) {
                                                String resultKV=resultCursor.getString(0)+":" + resultCursor.getString(1);
                                                Log.d("resultcursor", resultKV);
                                                results = resultKV+"-"+results;
                                                resultCursor.moveToNext();
                                            }

                                            PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
                                            output.println(results);
                                            output.flush();
                                            resultCursor.close();
                                            clientSocket.close();
                                        }
                                    }

                                    else if( myId_hash.compareTo(key_hash)  >= 1 && key_hash.compareTo(genHash(predecessor_id)) >=1 ) {
                                        resultCursor = query(uri, null, request[2], null, null);
                                        String results="";
                                        if (resultCursor != null) {
                                            resultCursor.moveToFirst();

                                            while (!resultCursor.isAfterLast()) {
                                                String resultKV=resultCursor.getString(0)+":" + resultCursor.getString(1);
                                                Log.d("resultcursor", resultKV);
                                                results = resultKV+"-"+results;
                                                resultCursor.moveToNext();
                                            }

                                            PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
                                            output.println(results);
                                            output.flush();
                                            resultCursor.close();
                                            clientSocket.close();
                                        }
                                    }
                                    else{
                                        Log.d(selection+":"+"not in my domain","sending it to my successor: "+successor_id);
                                        try {
                                            Socket succsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor_id)*2);
                                            PrintWriter output1 = new PrintWriter(succsocket.getOutputStream(), true);
                                            output1.println("SingleQ:"+myPort+":"+selection);
                                            output1.flush();
                                            BufferedReader input = new BufferedReader(new InputStreamReader(succsocket.getInputStream()));
                                            String inputQueryResult = input.readLine();
                                            PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
                                            output.println(inputQueryResult);
                                            output.flush();
                                            clientSocket.close();

                                        } catch (UnknownHostException e) {
                                            Log.e("Simple DHT", "Unknown host exception");
                                        } catch (IOException e) {
                                            Log.e("Simple DHT", "IO exception");
                                            e.printStackTrace();
                                        }
                                    }
                                }

                            }
                            catch (NoSuchAlgorithmException e){
                                Log.e("Simple dht", "No such algorithm");
                            }
                        }
                    }
                    else if(reqType.equalsIgnoreCase("SelectAll")){
                        Cursor resultCursor;
                        Log.d("Query *",request[0]+request[1]+request[2]+request[3]);
                        String initiator = request[3];
                        String selection = request[2];
                        String myId = String.valueOf(Integer.parseInt(myPort) / 2);
                        if (!myId.equalsIgnoreCase(initiator)){
                            Log.d("Query * server","Forwarding to successor but initiator is : "+initiator);
                            Socket succsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(successor_id)*2);
                            PrintWriter output = new PrintWriter(succsocket.getOutputStream(), true);
                            output.println("SelectAll:" + myPort + ":" + selection +":"+initiator);
                            output.flush();
                            BufferedReader input = new BufferedReader(new InputStreamReader(succsocket.getInputStream()));
                            String inputMessage = input.readLine();
                            succsocket.close();
                            resultCursor = query(uri, null, "@", null, null);
                            String results="";
                            if (resultCursor != null) {
                                resultCursor.moveToFirst();

                                while (!resultCursor.isAfterLast()) {
                                    String resultKV=resultCursor.getString(0)+":" + resultCursor.getString(1);
                                    Log.d("resultcursor*@621", resultKV);
                                    results = resultKV+"-"+results;
                                    resultCursor.moveToNext();
                                }
                                results=results.concat(inputMessage);
                                PrintWriter updateoutput = new PrintWriter(clientSocket.getOutputStream(), true);
                                updateoutput.println(results);
                                updateoutput.flush();
                                resultCursor.close();
                                clientSocket.close();
                            }


                        }
                        else{
                            resultCursor = query(uri, null, "@", null, null);
                            String results="";
                            if (resultCursor != null) {
                                resultCursor.moveToFirst();

                                while (!resultCursor.isAfterLast()) {
                                    String resultKV=resultCursor.getString(0)+":" + resultCursor.getString(1);
                                    Log.d("resultcursor*@621", resultKV);
                                    results = resultKV+"-"+results;
                                    resultCursor.moveToNext();
                                }

                                PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
                                output.println(results);
                                output.flush();
                                resultCursor.close();
                                clientSocket.close();
                            }
                        }
                    }
                    /*else if(reqType.contains("Query*")){
                        Cursor resultCursor;
                        Log.d("Query *",request[0]+request[1]+request[2]);
                        resultCursor = query(uri, null, "@", null, null);
                        String results="";
                        if (resultCursor != null) {
                            resultCursor.moveToFirst();

                            while (!resultCursor.isAfterLast()) {
                                String resultKV=resultCursor.getString(0)+":" + resultCursor.getString(1);
                                Log.d("resultcursor*@621", resultKV);
                                results = resultKV+"-"+results;
                                resultCursor.moveToNext();
                            }

                            PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
                            output.println(results);
                            output.flush();
                            resultCursor.close();
                            clientSocket.close();
                        }
                    }*/
                }
            }
            catch (IOException e){
                Log.e("Simple DHT","IOException");
            }
            catch (NullPointerException c){
                Log.e("Simple DHT","Nullpointer exception");
                c.printStackTrace();
            }
            return null;
        }
    }

    public void newjoin(Hashtable<String,LinkedList<String>> node_table, String cliPort){
        if(node_table.size()==0){
            LinkedList<String> pred_succ = new LinkedList<String >();
            pred_succ.addFirst(cliPort);
            pred_succ.addLast(cliPort);
            node_table.put(cliPort,pred_succ);
            return;
        }
        try{
            String main_node = "5554";
            String main_hash = genHash(main_node);
            String cli_hash = genHash(cliPort);
            if(node_table.size()==1) {
                LinkedList<String> pred_succ = new LinkedList<String >();
                pred_succ.addFirst(main_node);
                pred_succ.addLast(main_node);
                node_table.put(cliPort, pred_succ);
                node_table.get(main_node).addFirst(cliPort);
                node_table.get(main_node).addLast(cliPort);
            }
            else if(node_table.size()==2) {
                String mpred = node_table.get(main_node).peekFirst();
                String mpred_hash = genHash(mpred);
                String msucc = node_table.get(main_node).peekLast();
                String msucc_hash = genHash(msucc);
                String first ="";
                String end = "";
                if(mpred_hash.compareTo(main_hash) >=1 ){
                    first=main_node;
                    end=mpred;
                }
                else {
                    first=mpred;
                    end=main_node;
                }
                Log.d("first",first+"not updated");
                Log.d("end",end+"not updated");
                Log.d("newnode",cliPort);
                if(cli_hash.compareTo(genHash(first))>=1){
                    String second = node_table.get(first).peekLast();
                    if(cli_hash.compareTo(genHash(second))>=1){
                        Log.d("3rd Insertion after 2",cliPort);
                        LinkedList<String> pred_succ = new LinkedList<String>();
                        pred_succ.addFirst(node_table.get(first).pollFirst());
                        node_table.get(first).addFirst(cliPort);
                        pred_succ.addLast(node_table.get(second).pollLast());
                        node_table.get(second).addLast(cliPort);
                        node_table.put(cliPort,pred_succ);
                    }
                    else{
                        Log.d("3rd Insertion after 1", cliPort);
                        LinkedList<String> pred_succ = new LinkedList<String>();
                        pred_succ.addFirst(node_table.get(end).pollFirst());
                        node_table.get(end).addFirst(cliPort);
                        pred_succ.addLast(node_table.get(first).pollLast());
                        node_table.get(first).addLast(cliPort);
                        node_table.put(cliPort,pred_succ);
                    }
                }
                else{
                    Log.d("3rd Insertion before 1",cliPort);
                    LinkedList<String> pred_succ = new LinkedList<String>();
                    pred_succ.addFirst(node_table.get(first).pollFirst());
                    node_table.get(first).addFirst(cliPort);
                    pred_succ.addLast(node_table.get(end).pollLast());
                    node_table.get(end).addLast(cliPort);
                    node_table.put(cliPort,pred_succ);
                }
            }
            else if(node_table.size()==3){
                String mpred = node_table.get(main_node).peekFirst();
                String mpred_hash = genHash(mpred);
                String msucc = node_table.get(main_node).peekLast();
                String msucc_hash = genHash(msucc);
                String first ="";
                String end = "";
                if(mpred_hash.compareTo(main_hash) >=0 ){ first=main_node;}
                else if((genHash(node_table.get(mpred).peekFirst())).compareTo(mpred_hash) >= 0) {first = mpred;}
                else if(main_hash.compareTo(msucc_hash) >=0 ) {first = msucc;}
                //if(msucc_hash.compareTo(main_hash) < 0) {end=main_node;}
                //else if((genHash(node_table.get(msucc).peekLast())).compareTo(msucc_hash) < 0){end = msucc;}
                //else if(mpred_hash.compareTo(main_hash) <0 ){end=mpred;}
                end = node_table.get(first).peekFirst();
                Log.d("First",first);
                Log.d("end",end+"didnt get executed?????????????????????????????????????????");
                if (cli_hash.compareTo(genHash(first)) >= 1){
                    String second = node_table.get(first).peekLast();
                    if (cli_hash.compareTo(genHash(second)) >= 1){
                        String last = node_table.get(second).peekLast();
                        if (cli_hash.compareTo(genHash(last)) >= 1){
                            Log.d("4th Insertion after 3",cliPort);
                            LinkedList<String> pred_succ = new LinkedList<String>();
                            pred_succ.addFirst(node_table.get(first).pollFirst());
                            node_table.get(first).addFirst(cliPort);
                            pred_succ.addLast(node_table.get(last).pollLast());
                            node_table.get(last).addLast(cliPort);
                            node_table.put(cliPort,pred_succ);
                        }
                        else {
                            Log.d("4th Insertion after 2",cliPort);
                            LinkedList<String> pred_succ = new LinkedList<String>();
                            pred_succ.addFirst(node_table.get(last).pollFirst());
                            node_table.get(last).addFirst(cliPort);
                            pred_succ.addLast(node_table.get(second).pollLast());
                            node_table.get(second).addLast(cliPort);
                            node_table.put(cliPort,pred_succ);
                        }
                    }
                    else{
                        Log.d("4th Insertion after 1",cliPort);
                        LinkedList<String> pred_succ = new LinkedList<String>();
                        pred_succ.addFirst(node_table.get(second).pollFirst());
                        node_table.get(second).addFirst(cliPort);
                        pred_succ.addLast(node_table.get(first).pollLast());
                        node_table.get(first).addLast(cliPort);
                        node_table.put(cliPort,pred_succ);
                    }
                }
                else {
                    Log.d("4th Insertion before 1",cliPort);
                    LinkedList<String> pred_succ = new LinkedList<String>();
                    pred_succ.addFirst(node_table.get(first).pollFirst());
                    node_table.get(first).addFirst(cliPort);
                    pred_succ.addLast(node_table.get(end).pollLast());
                    node_table.get(end).addLast(cliPort);
                    node_table.put(cliPort,pred_succ);
                }
            }
            else if(node_table.size()==4){
                String mp = node_table.get(main_node).peekFirst();
                String mp_hash = genHash(mp);
                String ms = node_table.get(main_node).peekLast();
                String ms_hash = genHash(ms);
                String first="";
                String end="";
                if(mp_hash.compareTo(main_hash) >=1 ){                    first=main_node;                }
                else{
                    String mpp= node_table.get(mp).peekFirst();
                    String mpp_hash = genHash(mpp);
                    if(mpp_hash.compareTo(mp_hash) >= 1) {                        first = mp;                    }
                    else {
                        String mppp= node_table.get(mpp).peekFirst();
                        String mppp_hash = genHash(mppp);
                        if(mppp_hash.compareTo(mpp_hash) >= 1) {                            first = mpp;                        }
                        else {
                            if((genHash(node_table.get(mppp).peekFirst()).compareTo(mppp_hash)) >= 1)
                                first = mppp;
                        }
                    }
                }
                if(ms_hash.compareTo(main_hash) < 0) {                    end=main_node;                }
                else {
                    String mss= node_table.get(ms).peekLast();
                    String mss_hash = genHash(mss);
                    if(mss_hash.compareTo(ms_hash) < 0) {                        end = ms;                    }
                    else{
                        String msss= node_table.get(mss).peekLast();
                        String msss_hash = genHash(msss);
                        if(msss_hash.compareTo(mss_hash) < 0 ){                            end=mss;                        }
                        else {
                            if( (genHash(node_table.get(msss).peekLast())).compareTo(msss_hash) <0)
                                end = msss;
                        }
                    }
                }
                Log.d("First", first + "in laaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaast");
                Log.d("end", end + "in laaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaast");
                if(cli_hash.compareTo(genHash(first))>= 1){
                    String second = node_table.get(first).peekLast();
                    String second_hash = genHash(second);
                    if(cli_hash.compareTo(second_hash) >= 1){
                        String third = node_table.get(second).peekLast();
                        String third_hash = genHash(third);
                        if(cli_hash.compareTo(third_hash) >=1 ) {
                            String last = node_table.get(third).peekLast();
                            String last_hash = genHash(last);
                            if (cli_hash.compareTo(last_hash) >= 1){
                                Log.d("5th Insertion after 4",cliPort);
                                LinkedList<String> pred_succ = new LinkedList<String>();
                                pred_succ.addFirst(node_table.get(first).pollFirst());
                                node_table.get(first).addFirst(cliPort);
                                pred_succ.addLast(node_table.get(last).pollLast());
                                node_table.get(last).addLast(cliPort);
                                node_table.put(cliPort, pred_succ);
                            }
                            else{
                                Log.d("5th Insertion after 3",cliPort);
                                LinkedList<String> pred_succ = new LinkedList<String>();
                                pred_succ.addFirst(node_table.get(last).pollFirst());
                                node_table.get(last).addFirst(cliPort);
                                pred_succ.addLast(node_table.get(third).pollLast());
                                node_table.get(third).addLast(cliPort);
                                node_table.put(cliPort, pred_succ);
                            }
                        }
                        else{
                            Log.d("5th Insertion after 2",cliPort);
                            LinkedList<String> pred_succ = new LinkedList<String>();
                            pred_succ.addFirst(node_table.get(third).pollFirst());
                            node_table.get(third).addFirst(cliPort);
                            pred_succ.addLast(node_table.get(second).pollLast());
                            node_table.get(second).addLast(cliPort);
                            node_table.put(cliPort, pred_succ);
                        }
                    }
                    else {
                        Log.d("5th Insertion after 1",cliPort);
                        LinkedList<String> pred_succ = new LinkedList<String>();
                        pred_succ.addFirst(node_table.get(second).pollFirst());
                        node_table.get(second).addFirst(cliPort);
                        pred_succ.addLast(node_table.get(first).pollLast());
                        node_table.get(first).addLast(cliPort);
                        node_table.put(cliPort, pred_succ);
                    }
                }
                else {
                    Log.d("5th Insertion before 1",cliPort);
                    LinkedList<String> pred_succ = new LinkedList<String>();
                    pred_succ.addFirst(node_table.get(first).pollFirst());
                    pred_succ.addLast(node_table.get(end).pollLast());
                    node_table.get(first).addFirst(cliPort);
                    node_table.get(end).addLast(cliPort);
                    node_table.put(cliPort, pred_succ);
                }
            }
            Set<String> keys = node_table.keySet();
            for (String key : keys) {
                Log.d("pred+node+succ", node_table.get(key).peekFirst() + ":" + key + ":" + node_table.get(key).peekLast());
            }
        }catch (NoSuchAlgorithmException e){
            Log.e("Simple DHT","No such algorithm exception");
        }
    }
}
