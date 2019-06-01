package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {

	static final int SERVER_PORT = 10000;
	static String node_id;
	static String portStr;
	static String myPort;
	private static Uri uri;
	static int successorIndex = -1;
	static int predecessorIndex = -1;
	ArrayList<String> activeNodes = new ArrayList<String>();
	HashMap<String, String> allNodes = new HashMap<String, String>();
	HashMap<String, String> nodeHash  =new HashMap<String, String>();
	ArrayList<String> fileCopy1 = new ArrayList<String>();
	ArrayList<String> fileCopy2 = new ArrayList<String>();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		if(selection.equals("@")){
			for (File file : getContext().getFilesDir().listFiles()){
				if(!file.delete()){
					System.out.println("File Deletion Failed");
				}
			}
		}else if(selection.equals("*")){
			for (String node: activeNodes){
				String message = "Delete*"+"\n";
				Socket socket;
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(allNodes.get(nodeHash.get(node))));
					BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
					bWriter.write(message);
					bWriter.flush();

					BufferedReader bReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					do {
						// Waiting for Acknowledgement from Server
					}while(bReader.readLine() == null);
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}else{
			File file = getContext().getFileStreamPath(selection);
			if(file == null || !file.exists()){
				String node = null;
				try {
					for (int i = 0; i < activeNodes.size(); i++) {
						int prevIndex = i - 1;
						if (prevIndex < 0) {
							prevIndex = activeNodes.size() - 1;
							if ((genHash(selection).compareTo(activeNodes.get(prevIndex)) > 0) || (genHash(selection).compareTo(activeNodes.get(i)) <= 0)) {
								node = nodeHash.get(activeNodes.get(i));
								break;
							}
						} else {
							if ((genHash(selection).compareTo(activeNodes.get(prevIndex)) > 0) && (genHash(selection).compareTo(activeNodes.get(i)) <= 0)) {
								node = nodeHash.get(activeNodes.get(i));
								break;
							}
						}
					}
					String message = "Delete" + "-" + selection + "\n";
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, allNodes.get(node));

					int cIndex = activeNodes.indexOf(genHash(node));
					int n1 = cIndex + 1;
					if (n1 >= activeNodes.size()) {
						n1 = 0;
					}
					message = "Delete1" + "-" + selection + "\n";
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, allNodes.get(nodeHash.get(activeNodes.get(n1))));

					int n2 = n1 + 1;
					if(n2 >= activeNodes.size()){
						n2 = 0;
					}
					message = "Delete2" + "-" + selection + "\n";
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, allNodes.get(nodeHash.get(activeNodes.get(n2))));
				} catch(NoSuchAlgorithmException e){
					e.printStackTrace();
				}
				return 0;
			}else{
				if(!file.delete()){
					System.out.println("File Deletion Failed");
				}
				String message = "Delete1" + "-" + selection + "\n";
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, allNodes.get(nodeHash.get(activeNodes.get(successorIndex))));

				int nIndex = successorIndex + 1;
				if(nIndex >= activeNodes.size()){
					nIndex = 0;
				}
				message = "Delete2" + "-" + selection + "\n";
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, allNodes.get(nodeHash.get(activeNodes.get(nIndex))));
			}
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		Set<Map.Entry<String, Object>> set = values.valueSet();
		Iterator itr = set.iterator();

		String key = "";
		String value = "";
		while(itr.hasNext()) {
			Map.Entry entry = (Map.Entry) itr.next();
			value = entry.getValue().toString();
			entry = (Map.Entry) itr.next();
			key = entry.getValue().toString();
		}

		try {
			String node = null;
			for(int i=0; i<activeNodes.size(); i++){
				int prevIndex = i-1;
				if(prevIndex < 0){
					prevIndex = activeNodes.size()-1;
					if((genHash(key).compareTo(activeNodes.get(prevIndex)) > 0) || (genHash(key).compareTo(activeNodes.get(i)) <= 0)){
						node = nodeHash.get(activeNodes.get(i));
						break;
					}
				}else{
					if((genHash(key).compareTo(activeNodes.get(prevIndex)) > 0) && (genHash(key).compareTo(activeNodes.get(i)) <= 0)){
						node = nodeHash.get(activeNodes.get(i));
						break;
					}
				}
			}

			if(portStr.equals(node)){
				FileOutputStream outputStream;
				outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
				outputStream.write(value.getBytes());
				outputStream.close();
				String message = "Replication1"+"-"+key+"-"+value+"\n";
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, allNodes.get(nodeHash.get(activeNodes.get(successorIndex)))).get(1000, TimeUnit.MILLISECONDS);

				int nextIndex = successorIndex + 1;
				if(nextIndex >= activeNodes.size()){
					nextIndex = 0;
				}
				message = "Replication2"+"-"+key+"-"+value+"\n";
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, allNodes.get(nodeHash.get(activeNodes.get(nextIndex)))).get(1000, TimeUnit.MILLISECONDS);

			} else{
				String message = "Insert"+"-"+key+"-"+value+"\n";
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, allNodes.get(node)).get(1000, TimeUnit.MILLISECONDS);
				int cIndex = activeNodes.indexOf(genHash(node));
				int n1 = cIndex + 1;
				if(n1 >= activeNodes.size()){
					n1 = 0;
				}
				int n2 = n1 + 1;
				if(n2 >= activeNodes.size()){
					n2 = 0;
				}
				message = "Replication1"+"-"+key+"-"+value+"\n";
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, allNodes.get(nodeHash.get(activeNodes.get(n1)))).get(1000, TimeUnit.MILLISECONDS);
				message = "Replication2"+"-"+key+"-"+value+"\n";
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, allNodes.get(nodeHash.get(activeNodes.get(n2)))).get(1000, TimeUnit.MILLISECONDS);
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (IOException e) {
			Log.e(TAG, "File write failed");
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		allNodes.put("5554","11108");
		allNodes.put("5556","11112");
		allNodes.put("5558","11116");
		allNodes.put("5560","11120");
		allNodes.put("5562","11124");

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		try {
			node_id = genHash(portStr);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		try {
			activeNodes.add(genHash("5554"));
			activeNodes.add(genHash("5556"));
			activeNodes.add(genHash("5558"));
			activeNodes.add(genHash("5560"));
			activeNodes.add(genHash("5562"));
			nodeHash.put(genHash("5554"), "5554");
			nodeHash.put(genHash("5556"), "5556");
			nodeHash.put(genHash("5558"), "5558");
			nodeHash.put(genHash("5560"), "5560");
			nodeHash.put(genHash("5562"), "5562");

			Collections.sort(activeNodes);
			int currentIndex = activeNodes.indexOf(node_id);
			successorIndex = currentIndex + 1;
			predecessorIndex = currentIndex - 1;
			if(predecessorIndex < 0){
				predecessorIndex = activeNodes.size() - 1;
			}
			if(successorIndex >= activeNodes.size()){
				successorIndex = 0;
			}

			for(String n: activeNodes){
				Log.d("node", nodeHash.get(n));
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		String message = "getFiles" + "-" + portStr + "\n";
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, allNodes.get(portStr));

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
		}

		uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider");
		return true;
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

		if(selection.equals("@")){
			String[] columns = new String[]{"key", "value"};
			MatrixCursor m = new MatrixCursor(columns);

			for (File file : getContext().getFilesDir().listFiles()){
				byte[] input = new byte[(int)file.length()];
				try {
					FileInputStream inputStream = new FileInputStream(file);
					inputStream.read(input);
					inputStream.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

				String value = new String(input);
				m.newRow().add("key", file.getName()).add("value", value);
			}
			return m;
		}else if(selection.equals("*")){
			String[] columns = new String[]{"key", "value"};
			MatrixCursor m = new MatrixCursor(columns);
			for (String node: activeNodes){
				String message = "Query*"+"\n";
				Socket socket;
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(allNodes.get(nodeHash.get(node))));
					BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
					bWriter.write(message);
					bWriter.flush();

					BufferedReader bReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String temp;
					int timer=0;
					do {
						// Waiting for Acknowledgement from Server
						temp = bReader.readLine();
						timer++;
						if(timer >= 500){
							break;
						}
					}while(temp == null);

					if(temp != null) {
						if (temp.equals("Empty")) {
							continue;
						}
						String msgs[] = temp.split("-");
						for (int i = 0; i < msgs.length; i += 2) {
							if (msgs.length == 1) {
								break;
							}
							m.newRow().add("key", msgs[i]).add("value", msgs[i + 1]);
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return m;
		}

		File file = getContext().getFileStreamPath(selection);

		if(file == null || !file.exists()) {
			String[] columns = new String[]{"key", "value"};
			MatrixCursor m = new MatrixCursor(columns);

			String message = "Query" + "-" + selection + "\n";
			String node = null;
			String temp1 = null;
			String temp2 = null;
			String temp3 = null;
			int n1 = 0, n2;
			try{
				for (int i = 0; i < activeNodes.size(); i++) {
					int prevIndex = i - 1;
					if (prevIndex < 0) {
						prevIndex = activeNodes.size() - 1;
						if ((genHash(selection).compareTo(activeNodes.get(prevIndex)) > 0) || (genHash(selection).compareTo(activeNodes.get(i)) <= 0)) {
							node = nodeHash.get(activeNodes.get(i));
							break;
						}
					} else {
						if ((genHash(selection).compareTo(activeNodes.get(prevIndex)) > 0) && (genHash(selection).compareTo(activeNodes.get(i)) <= 0)) {
							node = nodeHash.get(activeNodes.get(i));
							break;
						}
					}
				}

				int cIndex = activeNodes.indexOf(genHash(node));
				n1 = cIndex + 1;
				if(n1 >= activeNodes.size()){
					n1 = 0;
				}
				n2 = n1 + 1;
				if(n2 >= activeNodes.size()){
					n2 = 0;
				}

				Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(allNodes.get(nodeHash.get(activeNodes.get(n2)))));
				BufferedWriter bWriter3 = new BufferedWriter(new OutputStreamWriter(socket3.getOutputStream()));
				bWriter3.write(message);
				bWriter3.flush();

				BufferedReader bReader3 = new BufferedReader(new InputStreamReader(socket3.getInputStream()));
				int timer3=0;
				do {
					// Waiting for Acknowledgement from Server
					temp3 = bReader3.readLine();
					timer3++;
					if(timer3 >= 500){
						break;
					}
				} while (temp3 == null);
				socket3.close();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			try {
				Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(allNodes.get(nodeHash.get(activeNodes.get(n1)))));
				BufferedWriter bWriter2 = new BufferedWriter(new OutputStreamWriter(socket2.getOutputStream()));
				bWriter2.write(message);
				bWriter2.flush();

				BufferedReader bReader2 = new BufferedReader(new InputStreamReader(socket2.getInputStream()));
				int timer2 = 0;
				do {
					// Waiting for Acknowledgement from Server
					temp2 = bReader2.readLine();
					timer2++;
					if (timer2 >= 500) {
						break;
					}
				} while (temp2 == null);
				socket2.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}

			try{
				Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(allNodes.get(node)));
				BufferedWriter bWriter1 = new BufferedWriter(new OutputStreamWriter(socket1.getOutputStream()));
				bWriter1.write(message);
				bWriter1.flush();

				BufferedReader bReader1 = new BufferedReader(new InputStreamReader(socket1.getInputStream()));
				int timer1=0;
				do {
					// Waiting for Acknowledgement from Server
					temp1 = bReader1.readLine();
					timer1++;
					if(timer1 >= 500){
						break;
					}
				} while (temp1 == null);
				socket1.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			if (temp3 != null && temp2 != null) {
				if(temp3.equals(temp2)) {
					Object[] data = new Object[2];
					data[0] = selection;
					data[1] = temp3;
					m.addRow(data);
					return m;
				}
			} if(temp3 != null && temp1 != null){
				if(temp3.equals(temp1)){
					Object[] data = new Object[2];
					data[0] = selection;
					data[1] = temp3;
					m.addRow(data);
					return m;
				}
			} if(temp2 != null && temp1 != null){
				if(temp2.equals(temp1)){
					Object[] data = new Object[2];
					data[0] = selection;
					data[1] = temp2;
					m.addRow(data);
					return m;
				}
			}
			return m;
		}else{
			FileInputStream inputStream;
			byte[] input = new byte[(int)file.length()];
			try{
				inputStream = getContext().openFileInput(selection);
				inputStream.read(input);
				inputStream.close();
			} catch (Exception e) {
				Log.e(TAG, "File read failed");
			}

			String value = new String(input);
			String[] columns = new String[]{"key", "value"};

			MatrixCursor m = new MatrixCursor(columns);
			m.newRow().add("key", selection).add("value", value);
			return m;
		}
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

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... serverSockets) {
			ServerSocket serverSocket = serverSockets[0];
			while(true){
				try {
					Socket socket = serverSocket.accept();
					BufferedReader bReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String message = bReader.readLine();
					BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
					String[] msgs = message.split("-");

					if(msgs[0].equals("Insert")){
						FileOutputStream outputStream;
						outputStream = getContext().openFileOutput(msgs[1], Context.MODE_PRIVATE);
						outputStream.write(msgs[2].getBytes());
						outputStream.close();
						bWriter.write("Message Received by Server");
						bWriter.flush();
						socket.close();
					} else if(msgs[0].equals("Replication1")){
						FileOutputStream outputStream;
						outputStream = getContext().openFileOutput(msgs[1], Context.MODE_PRIVATE);
						outputStream.write(msgs[2].getBytes());
						outputStream.close();
						fileCopy1.add(msgs[1]);
						bWriter.write("Message Received by Server");
						bWriter.flush();
						socket.close();
					} else if(msgs[0].equals("Replication2")){
						FileOutputStream outputStream;
						outputStream = getContext().openFileOutput(msgs[1], Context.MODE_PRIVATE);
						outputStream.write(msgs[2].getBytes());
						outputStream.close();
						fileCopy2.add(msgs[1]);
						bWriter.write("Message Received by Server");
						bWriter.flush();
						socket.close();
					}  else if(msgs[0].equals("getFiles")) {
						String messg = "getFilesSuccessor" + "-" + portStr + "\n";
						Socket cSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(allNodes.get(nodeHash.get(activeNodes.get(successorIndex)))));
						BufferedWriter cBW = new BufferedWriter(new OutputStreamWriter(cSocket.getOutputStream()));
						cBW.write(messg);
						cBW.flush();

						BufferedReader cBR = new BufferedReader(new InputStreamReader(cSocket.getInputStream()));
						String temp1;
						int timer=0;
						do {
							// Waiting for Acknowledgement from Server
							temp1 = cBR.readLine();
							timer++;
							if(timer >= 500){
								break;
							}
						} while (temp1 == null);
						cSocket.close();
						if(temp1 != null){
							String temps[] = temp1.split("//");
							if(!temps[0].equals("empty")) {
								String keyValue1[] = temps[0].split("-");
								for (int i = 0; i < keyValue1.length; i = i + 2) {
									FileOutputStream outputStream;
									outputStream = getContext().openFileOutput(keyValue1[i], Context.MODE_PRIVATE);
									outputStream.write(keyValue1[i + 1].getBytes());
									outputStream.close();
								}
							}
							if(!temps[1].equals("empty")) {
								String keyValue2[] = temps[1].split("-");
								for (int i = 0; i < keyValue2.length; i = i + 2) {
									FileOutputStream outputStream;
									outputStream = getContext().openFileOutput(keyValue2[i], Context.MODE_PRIVATE);
									outputStream.write(keyValue2[i + 1].getBytes());
									outputStream.close();
									fileCopy1.add(keyValue2[i]);
								}
							}
						}

						messg = "getFilesPredecessor" + "-" + portStr + "\n";
						cSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(allNodes.get(nodeHash.get(activeNodes.get(predecessorIndex)))));
						cBW = new BufferedWriter(new OutputStreamWriter(cSocket.getOutputStream()));
						cBW.write(messg);
						cBW.flush();

						String temp2;
						cBR = new BufferedReader(new InputStreamReader(cSocket.getInputStream()));
						int counter=0;
						do {
							// Waiting for Acknowledgement from Server
							temp2 = cBR.readLine();
							counter++;
							if(counter >= 500){
								break;
							}
						} while (temp2 == null);
						cSocket.close();
						if(temp2 != null){
							if(!temp2.equals("empty")) {
								String keyValue[] = temp2.split("-");
								for (int i = 0; i < keyValue.length; i = i + 2) {
									FileOutputStream outputStream;
									outputStream = getContext().openFileOutput(keyValue[i], Context.MODE_PRIVATE);
									outputStream.write(keyValue[i + 1].getBytes());
									outputStream.close();
									fileCopy2.add(keyValue[i]);
								}
							}
						}
						bWriter.write("Message Received by Server");
						bWriter.flush();
						socket.close();
					} else if(msgs[0].equals("getFilesSuccessor")){
						StringBuilder fc1 = new StringBuilder();
						StringBuilder fc2 = new StringBuilder();
						for (File file : getContext().getFilesDir().listFiles()){
							byte[] input = new byte[(int)file.length()];
							try {
								FileInputStream inputStream = new FileInputStream(file);
								inputStream.read(input);
								inputStream.close();
							} catch (FileNotFoundException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
							String value = new String(input);
							if(fileCopy1.contains(file.getName())){
								fc1.append(file.getName());
								fc1.append("-");
								fc1.append(value);
								fc1.append("-");
							}else if (fileCopy2.contains(file.getName())){
								fc2.append(file.getName());
								fc2.append("-");
								fc2.append(value);
								fc2.append("-");
							}
						}
						if((fileCopy1.isEmpty()) || (fc1.toString().length() == 0)){
							fc1 = new StringBuilder("empty");
						}else{
							if(fc1.toString().length()!=0){
								fc1 = fc1.deleteCharAt(fc1.toString().length()-1);
							}
						}
						if((fileCopy2.isEmpty()) || (fc2.toString().length() == 0)){
							fc2 = new StringBuilder("empty");
						}else{
							if(fc2.toString().length() != 0){
								fc2 = fc2.deleteCharAt(fc2.toString().length()-1);
							}
						}
						bWriter.write(fc1.toString() + "//" + fc2.toString() + "\n");
						bWriter.flush();
						socket.close();
					} else if(msgs[0].equals("getFilesPredecessor")){
						StringBuilder fc1 = new StringBuilder();
						for (File file : getContext().getFilesDir().listFiles()){
							byte[] input = new byte[(int)file.length()];
							try {
								FileInputStream inputStream = new FileInputStream(file);
								inputStream.read(input);
								inputStream.close();
							} catch (FileNotFoundException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
							String value = new String(input);
							if(fileCopy1.contains(file.getName())){
								fc1.append(file.getName());
								fc1.append("-");
								fc1.append(value);
								fc1.append("-");
							}
						}
						if((fileCopy1.isEmpty()) || (fc1.toString().length() == 0)){
							fc1 = new StringBuilder("empty");
						}else{
							if(fc1.toString().length() != 0){
								fc1 = fc1.deleteCharAt(fc1.toString().length()-1);
							}
						}
						bWriter.write(fc1.toString() + "\n");
						bWriter.flush();
						socket.close();
					} else if(msgs[0].equals("Query")){
						Cursor c = query(uri, null, msgs[1], null, null);
						if(c.moveToFirst()){
							bWriter.write(c.getString(1) + "\n");
							bWriter.flush();
						}
						socket.close();
					}else if(msgs[0].equals("Query*")){
						int ct = 0;
						StringBuilder reply = new StringBuilder();
						Cursor c = query(uri, null, "@", null, null);
						if(c.isBeforeFirst()){
							c.moveToFirst();
						}
						while(!c.isAfterLast()){
							ct++;
							reply.append(c.getString(0));
							reply.append("-");
							reply.append(c.getString(1));
							reply.append("-");
							c.move(1);
						}
						if(ct == 0){
							bWriter.write("Empty" + "\n");
							bWriter.close();
							socket.close();
						}else{
							reply.append("\n");
							bWriter.write(reply.toString());
							bWriter.close();
							socket.close();
						}
					} else if(msgs[0].equals("Delete*")){
						bWriter.write("Message Received by Server");
						bWriter.flush();
						socket.close();
						delete(uri, "@", null);
					} else if(msgs[0].equals("Delete")){
						bWriter.write("Message Received by Server");
						bWriter.flush();
						socket.close();
						File file = getContext().getFileStreamPath(msgs[1]);
						if(file != null && file.exists()) {
							if (!file.delete()) {
								System.out.println("File Deletion Failed");
							}
						}
					}else if(msgs[0].equals("Delete1")){
						bWriter.write("Message Received by Server");
						bWriter.flush();
						socket.close();
						File file = getContext().getFileStreamPath(msgs[1]);
						if(file != null && file.exists()) {
							if (file.delete()) {
								if (fileCopy1.contains(msgs[1])) {
									fileCopy1.remove(msgs[1]);
								}
							}
						}
					} else if(msgs[0].equals("Delete2")){
						bWriter.write("Message Received by Server");
						bWriter.flush();
						socket.close();
						File file = getContext().getFileStreamPath(msgs[1]);
						if(file != null && file.exists()) {
							if (file.delete()) {
								if (fileCopy2.contains(msgs[1])) {
									fileCopy2.remove(msgs[1]);
								}
							}
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(msgs[1]));
				socket.setSoTimeout(100);
				BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
				bWriter.write(msgs[0]);
				bWriter.flush();

				BufferedReader bReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				int timer = 0;
				do {
					 //Waiting for Acknowledgement from Server
					timer++;
					if(timer > 500){
						break;
					}
				}while(bReader.readLine() == null);
				socket.close();
			}  catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
	}
}
