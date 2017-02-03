package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteConstraintException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	private SQLiteDatabase db;
	public static final String table_name = "messages";
	static final String[] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
	static final int SERVER_PORT = 10000;
	private ContentResolver mContentResolver;
	private Uri uri;
	static String myPort = "";
	String TAG = "SimpleDynamo";
	TreeMap<String, String> aliveNodes_map = new TreeMap<String, String>();
	public static final String PREFS_NAME = "MyPrefs";
	String failedNode ="";


	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	private static class DatabaseHelper extends SQLiteOpenHelper {
		DatabaseHelper(Context context) {
			super(context, "SimpleDynamo", null, 1);
		}

		@Override
		public void onCreate(SQLiteDatabase db) {
			db.execSQL("CREATE TABLE " + table_name + " (key TEXT PRIMARY KEY, value TEXT NOT NULL );");
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			db.execSQL("DROP TABLE IF EXISTS " + table_name);
			onCreate(db);
		}
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		if(selection.equalsIgnoreCase("*") || selection.equalsIgnoreCase("@")){
			db.delete(table_name,"key = ?",new String[] {"*"});
			if(selection.equalsIgnoreCase("@")){
				try{
					Map.Entry<String,String> coordSucc1=null;
					Map.Entry<String,String> coordSucc2=null;
					String coordNode = Integer.toString(Integer.parseInt(myPort) / 2);
					String coordHash = genHash(coordNode);
					try {
                        coordSucc1 = aliveNodes_map.higherEntry(coordHash);
                        Log.d("Higher Hash1:Node",coordSucc1.getKey() +":"+ coordSucc1.getValue()) ;
                    }
					catch (NullPointerException e){
						coordSucc1 = aliveNodes_map.firstEntry();
                        Log.d("Higher Hash1:Node",coordSucc1.getKey() +":"+ coordSucc1.getValue()) ;
					}
					try {
                        coordSucc2 = aliveNodes_map.higherEntry(coordSucc1.getKey());
                        Log.d("Higher Hash2:Node",coordSucc2.getKey() +":"+ coordSucc2.getValue()) ;
                    }
					catch (NullPointerException e){
						coordSucc2 = aliveNodes_map.firstEntry();
                        Log.d("Higher Hash2:Node",coordSucc2.getKey() +":"+ coordSucc2.getValue()) ;
					}
					Log.d("Coord n succ",coordNode+":"+coordSucc1.getValue()+":"+coordSucc2.getValue());
					Socket socketsucc1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(coordSucc1.getValue())*2);
					PrintWriter outputsucc1 = new PrintWriter(socketsucc1.getOutputStream(), true);
					outputsucc1.println("DeleteAt:"+myPort+":"+selection);
					outputsucc1.flush();
                    socketsucc1.close();
					Socket socketsucc2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(coordSucc2.getValue())*2);
					PrintWriter outputsucc2 = new PrintWriter(socketsucc2.getOutputStream(), true);
					outputsucc2.println("DeleteAt:" + myPort + ":" + selection);
					outputsucc2.flush();
                    socketsucc2.close();
				}catch (NoSuchAlgorithmException e){
					Log.e(TAG,"No such algorithm");
				}catch (UnknownHostException e){
					Log.e(TAG,"Unknown Host Exception");
				}catch (IOException e){
					Log.e(TAG,"IO Exception");
				}
			}
		}
		else{
			try {
				String keyHash = genHash(selection);
				String[] coordNodes = getCoordinatorNode(keyHash).split(":");
                try {
                    if(coordNodes[0].equalsIgnoreCase(Integer.toString(Integer.parseInt(myPort)/2))) {
                        db.delete(table_name, "key = ?", new String[]{selection});
                    }
                    else{
                        Log.d("not in my domain","sending it to my coord: "+coordNodes[0]);
                        Socket socketNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(coordNodes[0])*2);
                        PrintWriter outputNode = new PrintWriter(socketNode.getOutputStream(), true);
                        outputNode.println("JustDelete:"+myPort+":"+selection);
                        outputNode.flush();
                        socketNode.close();
                    }
                    Socket socketsucc1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(coordNodes[1])*2);
                    PrintWriter outputsucc1 = new PrintWriter(socketsucc1.getOutputStream(), true);
                    outputsucc1.println("JustDelete:"+myPort+":"+selection);
                    outputsucc1.flush();
                    socketsucc1.close();
                    Socket socketsucc2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(coordNodes[2])*2);
                    PrintWriter outputsucc2 = new PrintWriter(socketsucc2.getOutputStream(), true);
                    outputsucc2.println("JustDelete:"+myPort+":"+selection);
                    outputsucc2.flush();
                    socketsucc2.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "Unknown host exception");
				} catch (IOException e) {
					Log.e(TAG, "IO exception");
					e.printStackTrace();
				}
			}catch (NoSuchAlgorithmException e){
				Log.e(TAG,"No such algorithm");
			}
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	private class InsertEstablisher implements Runnable{
		private String remotePort;
		private String msgToSend;

		InsertEstablisher(String port, String msg){
			remotePort=port;
			msgToSend = msg;
		}

		@Override
		public void run() {
			try {
				if(remotePort.equalsIgnoreCase(Integer.toString(Integer.parseInt(myPort)/2)))
				{
					String[] msg = msgToSend.split(":");
					Log.d("Insert in me",msg[0] + ":" + msg[1] +":" + msg[2]);
					String[] vk = msg[2].split(" ");
					String key = vk[1].split("=")[1];
					String value = vk[0].split("=")[1];
					ContentValues cv = new ContentValues();
					cv.put("key", key);
					cv.put("value", value);
					long newRowID = db.insertWithOnConflict(table_name, null, cv, SQLiteDatabase.CONFLICT_REPLACE);
					Log.d("Insert row id", String.valueOf(newRowID));
				}
				else {
					Socket coordSocket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort) * 2);
					PrintWriter output1 = new PrintWriter(coordSocket1.getOutputStream(), true);
					output1.println(msgToSend);
					output1.flush();
					coordSocket1.close();
				}
			} catch (InterruptedIOException e){
				Log.e(TAG,"Interrupted IO exception");
			} catch (UnknownHostException e) {
				Log.e(TAG, "Unknown host exception");
			} catch (IOException e) {
				Log.e(TAG, "IO exception");
				e.printStackTrace();
			}
			Log.d("con established, exit", remotePort);
		}
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		String[] vk;
		DatabaseHelper dbHelper = new DatabaseHelper(getContext());
		db = dbHelper.getWritableDatabase();
		vk = values.toString().split(" ");
		String key = vk[1].split("=")[1];
        String value = vk[0].split("=")[1];
		try {
			String keyHash = genHash(key);
			Log.d("key n hash",key+":::::::::::::::::::::::::::::::::"+keyHash);
			//Log.d("keyHash",keyHash);
			String[] coordPorts = getCoordinatorNode(keyHash).split(":");
			Log.d("Coords are", coordPorts[0] + ":" + coordPorts[1] + ":" + coordPorts[2]);
			try {
					InsertEstablisher node = new InsertEstablisher(coordPorts[0],"InsertMe:"+myPort+":"+values.toString());
					node.run();
					InsertEstablisher succ1 = new InsertEstablisher(coordPorts[1],"Insert1:"+myPort+":"+values.toString());
					succ1.run();
					InsertEstablisher succ2 = new InsertEstablisher(coordPorts[2],"Insert2:"+myPort+":"+values.toString());
					succ2.run();
			} catch (Exception e) {
				Log.e(TAG, "exception");
				e.printStackTrace();
			}
		}catch (NoSuchAlgorithmException e){
			Log.e(TAG,"No such Algorithm");
		}
		return null;
	}

    public synchronized Uri insertReplication(Uri uri,ContentValues cv){
        Log.d(TAG, "in replication");
		DatabaseHelper dbHelper = new DatabaseHelper(getContext());
		db = dbHelper.getWritableDatabase();
        long newRowID = db.insertWithOnConflict(table_name,null,cv,SQLiteDatabase.CONFLICT_REPLACE);
        Log.d("Insert row id", String.valueOf(newRowID));
        Log.v("Insert replication:", cv.toString());
        return uri;
    }


	public synchronized void Recover(String node) {
			try {
				Log.d("In recover method",node);
				String pred = null;
				String ppred = null;
				try {
					pred = aliveNodes_map.lowerEntry(genHash(Integer.toString(Integer.parseInt(myPort) / 2))).getValue();
					Log.d("Predecessor is :",pred);
				}
				catch (NullPointerException e){
					pred = aliveNodes_map.lastEntry().getValue();
					Log.d("Predecessor is :",pred);
				}
				if(node.equalsIgnoreCase(pred)) {
					try {
						ppred = aliveNodes_map.lowerEntry(genHash(pred)).getValue();
						Log.d("Predecessor's pred is :",ppred);
					} catch (NullPointerException e) {
						ppred = aliveNodes_map.lastEntry().getValue();
						Log.d("Predecessor's pred is :",ppred);
					}
				}
				String succ;
				try {
					succ = aliveNodes_map.higherEntry(genHash(Integer.toString(Integer.parseInt(myPort) / 2))).getValue();
					Log.d("Succ is :",succ);
				}
				catch (NullPointerException e){
					succ = aliveNodes_map.firstEntry().getValue();
					Log.d("succ is :",succ);
				}
				Socket socketnode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(node)*2);
				PrintWriter outputnode = new PrintWriter(socketnode.getOutputStream(), true);
				outputnode.println("SelectAll:" + myPort + ":" + "@");
				outputnode.flush();
				BufferedReader inputpnode= new BufferedReader(new InputStreamReader(socketnode.getInputStream()));
				String inputMessage = inputpnode.readLine();
				socketnode.close();
				String[] results = null;
				try {
					Log.d("Node's @", node+":"+inputMessage);
					results = inputMessage.split("-");
				}catch (NullPointerException e){
					Log.e("Previous failed node:",failedNode);
					failedNode = node;
					Log.e("Current failed node:",failedNode);
					//e.printStackTrace();
					if(node.equalsIgnoreCase(succ)) {
						Map.Entry<String,String> succsucc;
						try {
							succsucc = aliveNodes_map.higherEntry(succ);
							Log.d("Succ's succ is :",succsucc.getValue());
						}
						catch (NullPointerException e1){
							succsucc = aliveNodes_map.firstEntry();
							Log.d("succ's succ is :",succsucc.getValue());
						}
						Socket socketsucc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(succsucc.getValue()) * 2);
						PrintWriter outputsucc = new PrintWriter(socketsucc.getOutputStream(), true);
						outputsucc.println("SelectAll:" + myPort + ":" + "@");
						outputsucc.flush();
						BufferedReader inputsucc = new BufferedReader(new InputStreamReader(socketsucc.getInputStream()));
						inputMessage = inputsucc.readLine();
						socketsucc.close();
						results = inputMessage.split("-");
					}
					else if(node.equalsIgnoreCase(pred)){
						Socket socketpred = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(ppred) * 2);
						PrintWriter outputpred = new PrintWriter(socketpred.getOutputStream(), true);
						outputpred.println("SelectAll:" + myPort + ":" + "@");
						outputpred.flush();
						BufferedReader inputpred = new BufferedReader(new InputStreamReader(socketpred.getInputStream()));
						inputMessage = inputpred.readLine();
						socketpred.close();
						results = inputMessage.split("-");
					}
				}
				for(int j=0;j<results.length;j++) {
					String[] KV=results[j].split(":");
					String keyHash = genHash(KV[0]);
					Log.d("key:keyHash",KV[0]+"______________________________________________"+keyHash);
					String[] coordNodes= getCoordinatorNode(keyHash).split(":");
					Log.d("coords",coordNodes[0]+":"+coordNodes[1]+":"+coordNodes[2]);
					if(coordNodes[0].equalsIgnoreCase(Integer.toString(Integer.parseInt(myPort)/2)) ||
							(coordNodes[1].equalsIgnoreCase(Integer.toString(Integer.parseInt(myPort)/2)) && node.equalsIgnoreCase(pred)) ||
							(coordNodes[2].equalsIgnoreCase(Integer.toString(Integer.parseInt(myPort)/2)) && node.equalsIgnoreCase(pred)))
					{
						ContentValues cv = new ContentValues();
						cv.put("key", KV[0]);
						cv.put("value", KV[1]);
						DatabaseHelper dbHelper = new DatabaseHelper(getContext());
						db = dbHelper.getWritableDatabase();
						long newRowID = db.insertWithOnConflict(table_name,null,cv,SQLiteDatabase.CONFLICT_REPLACE);
						Log.d("Insert row id", String.valueOf(newRowID));
						Log.d("Insert after recovery:",cv.toString());
					}
				}
			}
			catch (NoSuchAlgorithmException e){
				Log.e(TAG, "No such algorithm");
			}
			catch (UnknownHostException e){
				Log.e(TAG, "Unknown host exception");
			}
			catch (IOException e){
				Log.e(TAG, "IO exception");
			}
		}

	@Override
	public boolean onCreate() {
		Context context = getContext();
		uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider");

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		}
		catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
		}
		try {
			TelephonyManager tel = null;
			if (context != null) {
				tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
				String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
				myPort  = String.valueOf(Integer.parseInt(portStr) * 2);
				String msg = "Join";
				Log.d("I am up", myPort);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
			}
		}
		catch (NullPointerException e) {
			Log.e(TAG, "Null Pointer exception");
		}

        DatabaseHelper dbHelper = new DatabaseHelper(context);
		db = dbHelper.getWritableDatabase();
		return (db != null);
	}

	public String failureHandle(String nodeSucc1, String selection){
		String inputMessage = "";
		try {
			Socket socketsucc1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nodeSucc1) * 2);
			PrintWriter outputsucc1 = new PrintWriter(socketsucc1.getOutputStream(), true);
			outputsucc1.println("FailedQ:" + myPort + ":" + selection);
			outputsucc1.flush();
			BufferedReader inputsucc1 = new BufferedReader(new InputStreamReader(socketsucc1.getInputStream()));
			inputMessage = inputsucc1.readLine();
			socketsucc1.close();
		}
		catch (UnknownHostException e)
		{
            e.printStackTrace();
        }
		catch (IOException e){
            e.printStackTrace();
		}
		catch (Exception e){
            e.printStackTrace();
		}
		return inputMessage;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		try{
            Cursor c;
			Log.d("QueryKey", selection);
			Context context = getContext();
			DatabaseHelper dbHelper = new DatabaseHelper(context);
			db = dbHelper.getReadableDatabase();

			if(selection.equalsIgnoreCase("*") || selection.equalsIgnoreCase("@")){
				if(selection.equalsIgnoreCase("@") ) {
					c = db.rawQuery("SELECT key,value from " + table_name, null);
					return c;
				}
				else if(selection.equalsIgnoreCase("*")){
					MatrixCursor resultCursor= new MatrixCursor(new String[] {"key","value"});
					Hashtable<String,String> key_value = new Hashtable<String,String>();
					String resultKey;
					String resultValue;

					for (String aREMOTE_PORT : REMOTE_PORT) {
						Log.d("getting * from ", aREMOTE_PORT);
                        if(!(failedNode.isEmpty())){
                            Log.d("Failed node:",failedNode);
    						if (aREMOTE_PORT.equalsIgnoreCase(Integer.toString(Integer.parseInt(failedNode) * 2)))
	    						continue;
						}
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(aREMOTE_PORT));
						PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
						output.println("SelectAll:" + myPort + ":" + "@");
						output.flush();
						BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						String inputMessage = input.readLine();
						String[] results=null;
						socket.close();
                        try {
                            Log.d("Everybody's * from", inputMessage);
							results = inputMessage.split("-");
                        }catch (NullPointerException e){
                            failedNode=aREMOTE_PORT;
                            continue;
                        }
						for (String result : results) {
							String[] KV = result.split(":");
							resultKey = KV[0];
							resultValue = KV[1];
							Log.d("Query * ", resultKey + ":" + resultValue);
							if (!(key_value.containsKey(resultKey)))
                                key_value.put(resultKey, resultValue);
						}
					}
					for (String key:key_value.keySet() ) {
						String[] KVCursor = {key,key_value.get(key)};
						resultCursor.addRow(KVCursor);
					}
					return  resultCursor;
				}
			}
			else{
				try {
					String key_hash = genHash(selection);
					Log.d("QueryHash", key_hash);
					String[] coordNodes=getCoordinatorNode(key_hash).split(":");
					String myNode = Integer.toString(Integer.parseInt(myPort)/2);
					if((coordNodes[2].equalsIgnoreCase(myNode) || (coordNodes[1].equalsIgnoreCase(myNode) ) || coordNodes[0].equalsIgnoreCase(myNode))){
						Log.d("available with me",myPort);
						c = db.rawQuery("SELECT key,value from " + table_name + " where key = ?", new String[] {selection});
						while (c == null){
							Thread.sleep(10);
							Log.d("Waitin to be made avail",myPort);
							c = db.rawQuery("SELECT key,value from " + table_name + " where key = ?", new String[] {selection});
						}
                        Log.d("found key",selection);
						return c;
					}
					else if(!failedNode.equalsIgnoreCase(coordNodes[2])){
						Log.d("not in my domain","sending it to my coord[2]: "+coordNodes[2]);
						Log.d("failed node",failedNode+"??");
						String inputMessage="";
						try {
							Socket socketsucc2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(coordNodes[2])*2);
							try {
								PrintWriter outputsucc2 = new PrintWriter(socketsucc2.getOutputStream(), true);
								outputsucc2.println("SingleQ:"+myPort+":"+selection);
								outputsucc2.flush();
								BufferedReader inputsucc2 = new BufferedReader(new InputStreamReader(socketsucc2.getInputStream()));
								inputMessage = inputsucc2.readLine();
								Log.d("input query server", inputMessage);
                                socketsucc2.close();
							}
							catch (NullPointerException e){
								Log.e(TAG,"Null pointer @423");
								failedNode = coordNodes[2];
                                //return null;
								if(coordNodes[1].equalsIgnoreCase(Integer.toString(Integer.parseInt(myPort)/2))){
									return db.rawQuery("SELECT key,value from " + table_name + " where key = ?", new String[] {selection});
								}
								else
									inputMessage = failureHandle(coordNodes[1],selection);

							}
							catch (InterruptedIOException e){
								Log.e(TAG,"Interrupted IO Exception @ 433");
								failedNode = coordNodes[2];
								if(coordNodes[1].equalsIgnoreCase(Integer.toString(Integer.parseInt(myPort)/2))){
									return db.rawQuery("SELECT key,value from " + table_name + " where key = ?", new String[] {selection});
								}
								else
									inputMessage = failureHandle(coordNodes[1],selection);
							}
							catch (StreamCorruptedException e){
								Log.e(TAG,"Stream Socket corrupted");
								failedNode = coordNodes[2];
								if(coordNodes[1].equalsIgnoreCase(Integer.toString(Integer.parseInt(myPort)/2))){
									return db.rawQuery("SELECT key,value from " + table_name + " where key = ?", new String[] {selection});
								}
								else
									inputMessage = failureHandle(coordNodes[1],selection);
							}
							Log.d("INput message I read:",inputMessage);
							String[] results = inputMessage.split("-");
							String[] KV=results[0].split(":");
							Log.d("queryresult", KV[0] + ":" + KV[1]);
							MatrixCursor resultCursor= new MatrixCursor(new String[] {"key","value"});
							resultCursor.addRow(KV);
							return resultCursor;
						}
						catch (NullPointerException e){
							Log.e(TAG,"Null pointer exception @ 454");
							failedNode = coordNodes[2];
						}
                        catch (InterruptedIOException e) {
							Log.e(TAG, "Interrupted IO exception @ 458");
							failedNode = coordNodes[2];
						}
                        catch (UnknownHostException e) {
							Log.e(TAG, "Unknown host exception");
							failedNode = coordNodes[2];

						} catch (IOException e) {
							Log.e(TAG, "IO exception");
							failedNode = coordNodes[2];
							e.printStackTrace();
						}
						catch (ArrayIndexOutOfBoundsException e){
							Log.e(TAG,inputMessage);
							e.printStackTrace();
						}
					}
					else if(failedNode.equalsIgnoreCase(coordNodes[2])){
						Log.d("not in my domain","sending it to my"+coordNodes[1] +"since coord[2] failed: "+coordNodes[2] +"failed node is actually"+failedNode);
						if(coordNodes[1].equalsIgnoreCase(Integer.toString(Integer.parseInt(myPort)/2))){
							return db.rawQuery("SELECT key,value from " + table_name + " where key = ?", new String[] {selection});
						}
						else {
							MatrixCursor resultCursor = new MatrixCursor(new String[]{"key", "value"});
							try {
								String inputMessage = failureHandle(coordNodes[1], selection);
								try
								{Log.d("input query server", inputMessage);}
								catch (NullPointerException e){
									failedNode = coordNodes[1];
									inputMessage = failureHandle(coordNodes[2],selection);
								}
								String[] results = inputMessage.split("-");
								String[] KV = results[0].split(":");
								Log.d("queryresult", KV[0] + ":" + KV[1]);
								resultCursor.addRow(KV);
								return resultCursor;
							}
							catch (Exception e) {
								Log.e(TAG, "IO exception");
								e.printStackTrace();
							}
						}
					}
				}
				catch (NoSuchAlgorithmException e){
					Log.e(TAG, "No such algorithm");
				}
			}
		}
		catch (Exception e){
			Log.e(TAG,"Unknown exception");
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
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

	public void newjoin( String cliPort) {
		try{
			String cliPortBy2 = Integer.toString(Integer.parseInt(cliPort) / 2);
			String cliHash = genHash(cliPortBy2);
			aliveNodes_map.put(cliHash, cliPortBy2);
		}
		catch (NoSuchAlgorithmException e){
			Log.e(TAG,"No such algorithm exception");
		}
	}

	public String getCoordinatorNode(String keyHash){
		//SortedMap<String,String> greaterNodes = new TreeMap<String, String>();
		while (aliveNodes_map.size() <= 4);
		Map.Entry<String,String> coordNode = null;
		Map.Entry<String,String> coordSucc1 = null;
		Map.Entry<String,String> coordSucc2 = null;
		try{
			try {
				coordNode = aliveNodes_map.higherEntry(keyHash);
				Log.d("coord node",coordNode.getKey()+":"+coordNode.getValue());
			}
			catch (NullPointerException e){
				coordNode = aliveNodes_map.firstEntry();
				Log.d("coord node",coordNode.getKey()+":"+coordNode.getValue());
			}
			try {
				coordSucc1 = aliveNodes_map.higherEntry(coordNode.getKey());
				Log.d("coord succ",coordSucc1.getKey()+":"+coordSucc1.getValue());
			}
			catch (NullPointerException e){
				coordSucc1 = aliveNodes_map.firstEntry();
				Log.d("coord succ",coordSucc1.getKey()+":"+coordSucc1.getValue());
			}
			try {
				coordSucc2 = aliveNodes_map.higherEntry(coordSucc1.getKey());
				Log.d("coord succ1",coordSucc2.getKey()+":"+coordSucc2.getValue());
			}
			catch (NullPointerException e){
				coordSucc2 = aliveNodes_map.firstEntry();
				Log.d("coord succ1",coordSucc2.getKey()+":"+coordSucc2.getValue());
			}
		}catch (Exception e){
			Log.e(TAG,"Exception in getcoordinator @ 567");
		}
		Log.d("Coord n succ",coordNode.getValue()+":"+coordSucc1.getValue()+":"+coordSucc2.getValue());

		return (coordNode.getValue()+":"+coordSucc1.getValue()+":"+coordSucc2.getValue());
	}

    public void deleteAt(String coordPort){
        try{
            String coordNode = Integer.toString(Integer.parseInt(coordPort) / 2);
			DatabaseHelper dbHelper = new DatabaseHelper(getContext());
			db = dbHelper.getReadableDatabase();
            Cursor c = db.rawQuery("SELECT key,value from " + table_name, null);
            while (c == null){
                Thread.sleep(50);
                c = db.rawQuery("SELECT key,value from " + table_name, null);
            }
            c.moveToFirst();
            while (!c.isAfterLast()) {
                String key = c.getString(0);
                String value = c.getString(1);
                Log.d("cursor@DeleteAt", key + ":" + value);
                String[] coords = getCoordinatorNode(genHash(key)).split(":");
                if (coords[0].equalsIgnoreCase(coordNode)) {
                    db.delete(table_name, "key = ?", new String[]{key});
                }
                c.moveToNext();
                c.close();
            }
        }catch (NoSuchAlgorithmException e){
            Log.e(TAG,"No Such Algorithm Exception");
        }catch (InterruptedException e){
            Log.e(TAG,"Interrupted Exception");
        }
    }

    public synchronized String failedQ(String selection){
        try {
            Cursor resultCursor;
			DatabaseHelper dbHelper = new DatabaseHelper(getContext());
			db = dbHelper.getReadableDatabase();
            resultCursor = db.rawQuery("SELECT key,value from " + table_name + " where key = ?", new String[]{selection});
            while (resultCursor == null) {
                Thread.sleep(10);
                resultCursor = db.rawQuery("SELECT key,value from " + table_name + " where key = ?", new String[]{selection});
            }
            String results = "";
            resultCursor.moveToFirst();
            while (!resultCursor.isAfterLast()) {
                String resultKV = resultCursor.getString(0) + ":" + resultCursor.getString(1);
                Log.d("resultcursor*@621", resultKV);
                results = resultKV + "-" + results;
                resultCursor.moveToNext();
            }
            resultCursor.close();
            return results;
        }
        catch (InterruptedException e){
            Log.e(TAG,"Interrupted Exception");
        }
        return null;
    }

    private class connEstablisher implements Runnable{
        private String remotePort;
        private String msgToSend;

        connEstablisher(String port, String msg){
            remotePort=port;
            msgToSend = msg;
        }

        @Override
        public void run() {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
                output.println(msgToSend);
                output.flush();
                BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                try {
                    String inputMessage = input.readLine();

                    if (!inputMessage.isEmpty()) {
                        newjoin(inputMessage.trim());
                    }
                }catch (NullPointerException e){
                }
                socket.close();
            } catch(InterruptedIOException e){
                Log.e(TAG, "Interrupted IO");
            }catch(UnknownHostException e){
                Log.e(TAG, "Unknown host exception");
            }catch(IOException e){
                Log.e(TAG, "IO exception");
                e.printStackTrace();
            }
            Log.d("con established, exit",remotePort);
        }
    }

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			Socket clientSocket = null;
			String msgsReceived;
			String[] msg;
			try {
				while(true) {
					clientSocket = serverSocket.accept();
					BufferedReader inFromClient = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					msgsReceived=inFromClient.readLine();
					Log.d(TAG,msgsReceived);
					msg = msgsReceived.split(":");
					if(msg[0].equalsIgnoreCase("Join")){
						newjoin(msg[1]);
						PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
						output.println(myPort);
						output.flush();
					}
					else if (msg[0].equalsIgnoreCase("Insert1") || (msg[0].equalsIgnoreCase("Insert2")) || (msg[0].equalsIgnoreCase("InsertMe"))){
						Log.d("Insert req",msg[0] + ":" + msg[1] +":" + msg[2]);
						String[] vk = msg[2].split(" ");
						String key = vk[1].split("=")[1];
						String value = vk[0].split("=")[1];
						ContentValues cv = new ContentValues();
						cv.put("key", key);
						cv.put("value", value);
                        try {
                            insertReplication(uri, cv);
                        } catch (Exception e) {
                            Log.e(TAG, e.toString());
                            e.printStackTrace();
                        }
					}
					else if(msg[0].equalsIgnoreCase("SelectAll")) {
                        Cursor resultCursor;
                        Log.d("Query *", msg[0] + msg[1] + msg[2]);
						resultCursor = db.rawQuery("SELECT key,value from " + table_name, null);
						String results = "";
						if (resultCursor != null) {
							resultCursor.moveToFirst();
							while (!resultCursor.isAfterLast()) {
								String resultKV = resultCursor.getString(0) + ":" + resultCursor.getString(1);
								Log.d("resultcursor*@621", resultKV);
								results = resultKV + "-" + results;
								resultCursor.moveToNext();
							}
							PrintWriter updateoutput = new PrintWriter(clientSocket.getOutputStream(), true);
							updateoutput.println(results);
							updateoutput.flush();
							resultCursor.close();
							clientSocket.close();
						}
					}
					else if(msg[0].equalsIgnoreCase("SingleQ")) {
                        Log.d("Query *", msg[0] + msg[1] + msg[2]);
                        String results = failedQ(msg[2].trim());
                        PrintWriter updateoutput = new PrintWriter(clientSocket.getOutputStream(), true);
						updateoutput.println(results);
						updateoutput.flush();
                        clientSocket.close();
					}
                    else if(msg[0].equalsIgnoreCase("FailedQ")) {
                        Log.d("Query *", msg[0] + msg[1] + msg[2]);
                        String results = failedQ(msg[2].trim());
						PrintWriter updateoutput = new PrintWriter(clientSocket.getOutputStream(), true);
                        updateoutput.println(results);
                        updateoutput.flush();
                        clientSocket.close();
					}
					else if(msg[0].equalsIgnoreCase("DeleteAt")){
                        Log.d("Delete@",msg[0]+":"+msg[1]+msg[2]);
                        deleteAt(msg[1].trim());
                        clientSocket.close();
					}
                    else if(msg[0].equalsIgnoreCase("JustDelete")){
                        Log.d("Received:",msg[0]+":"+msg[1]+":"+msg[2]);
                        db.delete(table_name, "key = ?", new String[]{msg[2].trim()});
                        clientSocket.close();
                    }
					Log.d("Alive nodes", aliveNodes_map.values().toString());
				}
			}
            catch (InterruptedIOException e){
                Log.e(TAG, "Interrupted IO exception");
            }
			catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {
		@Override
		protected Void doInBackground(String... msgs) {
			//String remotePort;
			//for (int i = 0; i < 5; i++) {
            //    remotePort = REMOTE_PORT[i];
            String msgToSend = msgs[0] + ":" + msgs[1];
            connEstablisher t0 = new connEstablisher(REMOTE_PORT[0],msgToSend);
            t0.run();
            connEstablisher t1 = new connEstablisher(REMOTE_PORT[1],msgToSend);
            t1.run();
            connEstablisher t2 = new connEstablisher(REMOTE_PORT[2],msgToSend);
            t2.run();
            connEstablisher t3 = new connEstablisher(REMOTE_PORT[3],msgToSend);
            t3.run();
            connEstablisher t4 = new connEstablisher(REMOTE_PORT[4],msgToSend);
            t4.run();
			//}
            SharedPreferences myPrefs= getContext().getSharedPreferences(PREFS_NAME, 0);
            if (myPrefs.contains("rejoin")){
                try {
					failedNode = "";
                    //db.delete(table_name,"1",null);
                    Map.Entry<String, String> pred = null;
                    Map.Entry<String, String> succ = null;
                    try{
                        pred = aliveNodes_map.lowerEntry(genHash(Integer.toString(Integer.parseInt(myPort) / 2)));
                        Log.d("pred is :",pred.getValue());

                    }catch (NullPointerException e){
                        pred=aliveNodes_map.lastEntry();
                        Log.d("pred is :",pred.getValue());
                    }
					Recover(pred.getValue());
                    //predRecover.run();
                    try{
                        succ = aliveNodes_map.higherEntry(genHash(Integer.toString(Integer.parseInt(myPort) / 2)));
                        Log.d("succ is :", succ.getValue());
                    }
                    catch (NullPointerException e){
                        succ = aliveNodes_map.firstEntry();
                        Log.d("Succ is :",succ.getValue());
                    }
                    Recover(succ.getValue());
					//succRecover.run();
                }catch (NoSuchAlgorithmException e){
                    Log.e(TAG,"np such algo");
                }
            }
            else {
                //Set Preference
                //SharedPreferences myPrefs = context.getSharedPreferences(PREFS_NAME, 0);
                SharedPreferences.Editor prefsEditor = myPrefs.edit();
                prefsEditor.putString("rejoin", "0");
                prefsEditor.commit();
            }
			return null;
		}
	}
}