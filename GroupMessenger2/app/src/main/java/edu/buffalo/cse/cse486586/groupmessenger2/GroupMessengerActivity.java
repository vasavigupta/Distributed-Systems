package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;



/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */

public class GroupMessengerActivity extends Activity {
    int count=0;

    static final String[] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    private ContentResolver mContentResolver;
    private Uri mUri;
    //static int count = 0;
    String failedPort="";

    class MessageObject {
        private Double order_id;
        private String msg;
        private Integer identifier;
        boolean deliverable;
        String originPort;
    }

    class MyComparator implements Comparator<MessageObject> {

        public int compare(MessageObject a, MessageObject b) {
            return a.order_id.compareTo(b.order_id);
        }
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);


        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e("GroupMessenger2", "Can't create a ServerSocket");
            return;
        }


        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        final EditText editText = (EditText) findViewById(R.id.editText1);

        final Button button = (Button) findViewById(R.id.button4);
        button.setOnClickListener(new View.OnClickListener() {

            @Override
            public void onClick(View v) {

                String msg = editText.getText().toString() + "\n";
                editText.setText(""); // This is one way to reset the input box.

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);

            }
        });


        mContentResolver = getContentResolver();
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");

        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

    }

    Integer id=0;
    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {

                String remotePort;
                String[] msgIdProp;
                Integer msgId;
                Double proposeReceived;
                Map<Integer,ArrayList<Double>>  proposedMap = new Hashtable<Integer, ArrayList<Double>>();
                Double agreedNum;
                Double highest_prop=0.0;

                Integer localMsgId=Integer.parseInt(msgs[1] + String.valueOf(id++));

                for (int i = 0; i < 5; i++) {
                    //Log.d("154","inside for loop");

                    remotePort = REMOTE_PORT[i];
                    if(!failedPort.isEmpty()){
                        Log.d("Client after failure","Happening!!!"+failedPort);
                        if(remotePort.equalsIgnoreCase(failedPort)) {
                            continue;
                        }
                    }
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));

                    //Log.d("160 con established?", remotePort);


                    PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
                    //Log.d("164 Am i being reached?","yes");

                    String localPort=msgs[1];
                    String msgToSend = String.valueOf(localMsgId) + ":" +localPort+":"+remotePort+ ":" + msgs[0]  ;
                    //Log.d("167 Msg to send", msgToSend);
                    output.println(msgToSend);
                    //Log.d("what about this position?", "yes");
                    output.flush();

                    socket.setSoTimeout(1500);
                    try {
                        //read the proposed numbers from servers
                        BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String inputMessage = input.readLine();
                        msgIdProp = inputMessage.split(":");
                        msgId = Integer.parseInt(msgIdProp[0]);
                        proposeReceived = Double.parseDouble(msgIdProp[1]);
                        highest_prop = (highest_prop > proposeReceived) ? highest_prop : proposeReceived;
                        ArrayList<Double> tempList;
                        //Log.d("164", "I am here");
                        if (proposedMap.containsKey(msgId)) {
                            tempList = proposedMap.get(msgId);
                            if (tempList == null)
                                tempList = new ArrayList<Double>();
                            tempList.add(proposeReceived);
                        } else {
                            tempList = new ArrayList<Double>();
                            tempList.add(proposeReceived);
                        }
                        proposedMap.put(msgId, tempList);
                        //Log.d("175 Propose received", Double.toString(proposeReceived));
                    }
                    catch (SocketException e){
                        Log.e("Timed out","no response from: "+ remotePort);
                        failedPort=remotePort;
                        continue;
                    }
                    catch (InterruptedIOException e){
                        Log.e("Timed out","no response from: "+ remotePort);
                        failedPort=remotePort;
                        //continue;
                    }
                    catch (NullPointerException e)
                    {
                        failedPort=remotePort;
                        //Log.e("null pointer exception", e.getMessage());
                        //throw new  IllegalStateException("I am screwed",e);
                        continue;
                    }
                    socket.close();
                }
                //if((proposedMap.get(localMsgId).size() == 5 && failedPort.isEmpty()) || (!failedPort.isEmpty() && proposedMap.get(localMsgId).size() == 4)){
                    ArrayList<Double> temp_list = proposedMap.get(localMsgId);
                    //Log.d("Msg ID: ",Integer.toString(localMsgId));
                    //for (int j=0;j<temp_list.size();j++){
                    //    Log.d("Proposed numbers: ",Double.toString(temp_list.get(j)));
                    //}
                    agreedNum = Double.parseDouble(String.valueOf(Collections.<Double>max(temp_list)));
                    //Log.d("Chosen agreed number ",Double.toString(agreedNum));
                    for (int i = 0; i < 5; i++) {

                        remotePort = REMOTE_PORT[i];
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));
                        //Log.d("188 Sending agreed num",String.valueOf(remotePort));

                        PrintWriter output = new PrintWriter(socket.getOutputStream(), true);

                        output.println(String.valueOf(localMsgId) +":" + failedPort + ":" + Double.toString(highest_prop)  );
                        socket.close();
                    }
                //}

            }
            catch(UnknownHostException e){
                Log.e("GroupMessenger2", "ClientTask UnknownHostException");
            }
            catch(IOException e){
                Log.e("GroupMessenger2", "ClientTask socket IOException");
            }
            return null;
        }
    }



    Double agree_double =-1.0 ,propose_double = -1.0;
    Comparator<MessageObject> comparator = new MyComparator();

    private class ServerTask extends AsyncTask<ServerSocket, String, Void>{

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            PriorityBlockingQueue<MessageObject> hold_back = new PriorityBlockingQueue<MessageObject>(11,comparator);
            Socket clientSocket ;
            String msgToDisplay;

            String proposedString;
            Double agreedfinal_double;
            Integer msgId;
            String [] msgString;

            try {
                while(true) {
                    ////Log.d("Server", "waiting for connect");
                    clientSocket = serverSocket.accept();
                    ////Log.d("Accepted the con","from client");

                    //To read message from client
                    BufferedReader inFromClient = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    msgToDisplay = inFromClient.readLine();
                    msgString = msgToDisplay.split(":");
                    //Log.d("240 I read from client",msgToDisplay);
                    if (msgString.length == 4) {
                        msgId = Integer.parseInt(msgString[0]);

                        ////Log.d("244", String.valueOf(msgId));


                        //Calculating the proposed number to send out to client
                        propose_double = ((agree_double >= propose_double) ? agree_double : propose_double) + Double.valueOf("1." + msgString[2]);
                        proposedString = Double.toString(propose_double);
                        Log.d("250 I am proposing:",msgId+":"+proposedString);

                        //adding message and proposed number in hold back queue
                        MessageObject new_hold = new MessageObject();
                        new_hold.order_id = propose_double;
                        new_hold.msg = msgString[3];
                        new_hold.identifier = msgId;
                        new_hold.deliverable = false;
                        new_hold.originPort = msgString[1];
                        hold_back.add(new_hold);

                        ////Log.d("At 260", "B4 sending proposed number");

                        //send the msgId:proposed number to client as acknowledgement
                        PrintWriter output = new PrintWriter(clientSocket.getOutputStream(), true);
                        output.println(msgId + ":" + proposedString);
                        output.flush();
                        clientSocket.close();

                        Log.d("266", "Sent proposed number");
                    }

                    else {//if (msgString.length == 3) {

                        //Reading the agreed from Client

                        int msg_id = Integer.parseInt(msgString[0]);
                        agreedfinal_double = Double.parseDouble(msgString[2]);
                        failedPort=msgString[1];
                        //Log.d("277 msg_id",Integer.toString(msg_id));
                        //Log.d("278 agreed num",msgString[1]);

                        clientSocket.close();

                        /*Iterator<MessageObject> it1 = hold_back.iterator();
                        while (it1.hasNext()) {
                            MessageObject current = it1.next();
                            Log.d("B4-id:del:ID:Port:prop",String.valueOf(current.deliverable)+":"+
                                    String.valueOf(current.identifier)+":"+
                                    String.valueOf(current.originPort)+":"+
                                    String.valueOf(current.order_id));
                        }*/

                        //Updating the Message object with agreed number and making it deliverable
                        Iterator<MessageObject> it = hold_back.iterator();
                        while (it.hasNext()) {
                            MessageObject current = it.next();
                            //Log.d("284",String.valueOf(current.identifier));
                            if (current.identifier == msg_id) {
                                //Log.d("286 order id", String.valueOf(current.order_id.intValue()));
                                MessageObject temp_object = new MessageObject();
                                temp_object.identifier=current.identifier;
                                temp_object.msg=current.msg;
                                temp_object.originPort=current.originPort;
                                hold_back.remove(current);
                                temp_object.order_id = agreedfinal_double;
                                temp_object.deliverable = true;
                                hold_back.add(temp_object);
                                break;
                            }
                        }

                        Iterator<MessageObject> it4 = hold_back.iterator();
                        while (it4.hasNext()) {
                            MessageObject t4= it4.next();
                            if (!failedPort.isEmpty() && hold_back.size() != 0 && t4.originPort.contains(failedPort)) {
                                Log.d("lastfailedmsgobject1:","#################################################################/"+t4.originPort);
                                hold_back.remove(t4);
                            }
                        }
                        if(!failedPort.isEmpty() && hold_back.size() !=0 && hold_back.peek().originPort.contains(failedPort) ){// && !hold_back.peek().deliverable) {
                            hold_back.remove(hold_back.peek());
                            //Log.e("Yeah in side if", "nanananananananananananananananananananananana");
                        }

                        /*Iterator<MessageObject> it2 = hold_back.iterator();
                        while (it2.hasNext()) {
                            MessageObject current = it2.next();
                            Log.d("Aft-id:del:ID:Port:prop",String.valueOf(current.deliverable)+":"+
                                    String.valueOf(current.identifier)+":"+
                                    String.valueOf(current.originPort)+":"+
                                    String.valueOf(current.order_id)+":---------------------------------------"+":"+failedPort+"-------");
                        }*/

                        //Updated the current agreed value
                        agree_double = ((agree_double > agreedfinal_double) ? agree_double : agreedfinal_double);

                        //loop through hold_back queue until a node with deliverable=false is reached and put all the values in db
                        //Iterator<MessageObject> it1 = hold_back.iterator();

                        while (hold_back.size() !=0 && hold_back.peek().deliverable) {
                            ContentValues cv;
                            cv = new ContentValues();
                            cv.put("key",count);
                            cv.put("seq_num", hold_back.peek().order_id);
                            cv.put("value", hold_back.peek().msg);
                            count++;

                            try {
                                mContentResolver.insert(mUri, cv);
                            } catch (Exception e) {
                                Log.e("GroupMessenger2", e.toString());
                            }
                            publishProgress(hold_back.peek().msg);
                            hold_back.remove(hold_back.peek());
                            if(!failedPort.isEmpty() && hold_back.size() !=0 && hold_back.peek().originPort.contains(failedPort))// && !hold_back.peek().deliverable)
                                hold_back.remove(hold_back.peek());
                            //if(hold_back.size()==0)
                            //    break;
                        }

                    }

                }

            }
            catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
            return;
        }


    }
}
