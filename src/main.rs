use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader, ReadHalf}, net::{TcpListener, TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf, WriteHalf}}, stream, sync::{ Mutex, mpsc::{self, Receiver, Sender}}};
use std::{collections::HashMap, error::Error, fmt::format, rc::Rc, sync::Arc};


enum ClientEvent{
    Msg,
    List,
    Broadcast,
    Quit,
    Help,
    Error,
}

struct MessagePayload {
    Sender_Username: String,
    Receiver_Username: String,
    Message: String
}
struct Command {
    Event: ClientEvent,
    Payload: MessagePayload
}




#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Username and transmitter of the the clients tasks 
    let streams_channels_tx : Arc<Mutex<HashMap<String, Sender<String>>> >= Arc::new(Mutex::new(HashMap::new()));
    let streams_channels_tx_clone = Arc::clone(&streams_channels_tx);
    let (server_tx, mut server_rx) = mpsc::channel::<Command>(100);
    // let raw_streams: Arc<Mutex<TcpStream>>;
    
    tokio::spawn(async move {
        handle_server(server_rx, streams_channels_tx_clone).await;
    });

    
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    loop {
        let (stream, _)  = listener.accept().await?;
        
        let shared_stream = Arc::new(Mutex::new(stream));
        let shared_stream_clone_1 = Arc::clone(&shared_stream);

        let streams_channels_tx_clone_1 = Arc::clone(&streams_channels_tx);
        
        // 1. tokio server task
        // 2. task for asking namees from the streams and giving then than spawns a new tokio task after 
        
        // REASON - the reason i am spawning a thread for asking the username is i don't want to block the main thread so it keeps accepting clients and adding new transmitters in the global state.
        let server_tx_clone = server_tx.clone();
        tokio::spawn(async move {
            ask_username(shared_stream_clone_1, streams_channels_tx_clone_1, server_tx_clone).await;
        });
    }
   
}

/*
    1. will receive messages from all the other clients 
    2. will send message to other clients via channels( from , message)
    3. parse the payload message 
    4. also will check if the user exists or not if not that will notify the sender user not found
    5. Use tokio select  if needed
*/
async fn handle_server(mut server_rx: Receiver<Command>, streams_channels_tx: Arc<Mutex<HashMap<String, Sender<String>>>> ) {
    println!("Receiver channel open for the server");
    while let Some(payload) = server_rx.recv().await {

        let mut streams_tx_locked = streams_channels_tx.lock().await;
        match payload.Event {
            ClientEvent::Msg => {
                println!("MSG: RECIEVED PAYLOAD FROM THE CLIENT {:?}", payload.Payload.Message);
                
                match streams_tx_locked.get(&payload.Payload.Receiver_Username) {
                    Some(client_tx) => { 
                        println!("MESSAGE SENT TO THE CLIENT FROM SERVER");
                        let fromatted_message = format!("{}:{}", payload.Payload.Sender_Username, payload.Payload.Message);
                        client_tx.send(fromatted_message.to_string()).await;
                    },
                    _ =>{
                        // Send message to sender from server that you entered username does not exist or entered the username incorrect
                        println!("user not found");
                        println!("THIS IS THE SENDER USERNAME : IN HANDLE CLIENT {}",payload.Payload.Sender_Username);
                        let sender_tx =  streams_tx_locked.get(&payload.Payload.Sender_Username).expect("Sender Stream Not Found");
                        let initial_formatting_message = format!("User {} not found, please enter correct username", payload.Payload.Sender_Username);
                        let formatted_message = format!("Server : {}", initial_formatting_message);
                        sender_tx.send(formatted_message.to_string()).await;
                    }
                }
            },
            ClientEvent::Broadcast => {
                println!("BROADCAST: broadcast cmd received from the client");
                for (receiver_username, receiver_tx) in streams_tx_locked.iter()  {
                    if receiver_username != &payload.Payload.Sender_Username {
                        let formatted_message = format!("{}: {}", payload.Payload.Sender_Username, payload.Payload.Message);
                        receiver_tx.send(formatted_message.to_string()).await;
                    }
                }
                println!("OUTSIDE the FOR LOOP OF BROADCAST");
            },
            ClientEvent::List => {
                println!("LIST: list cmd received from the client");
                let mut usernames = String::new();
                for (receiver_username, _)  in streams_tx_locked.iter() {
                    let formatted_message = format!("{receiver_username}\n");
                    usernames.push_str(formatted_message.as_str());
                }
                match streams_tx_locked.get(&payload.Payload.Sender_Username) {
                    Some(sender_tx) => {
                        let formatted_message = format!("Server : {}", usernames);
                        sender_tx.send(formatted_message).await;
                    }
                    _ => {
                        println!("Stream Not Found");
                    }
                }
            }
            ClientEvent::Quit => {
                println!("QUIT: quit cmd received from the client");
                streams_tx_locked.remove(&payload.Payload.Sender_Username);
            }
            _ =>{ }
        }
        // println!("Recieved a message to the server");
    }
    println!("Server receiver channel closed");
}

async fn ask_username(stream: Arc<Mutex<TcpStream>>, streams_transmitters: Arc<Mutex<HashMap<String, Sender<String>>>>, server_tx: Sender<Command>) {
    let formatted_message = &format!("Please Enter Your Username: ");
    let stream_task = Arc::try_unwrap(stream).expect("Stream in use elsewhere").into_inner();
    let (read_stream, mut write_stream) = stream_task.into_split();
   
    (write_stream).write_all(formatted_message.as_bytes()).await;

    let mut reader  = BufReader::new(read_stream);

    // TODO - Add a check that checks whether the username already exists or not 
    let mut username: String = String::new();
    match reader.read_line(&mut username).await {
        Ok(0) => {println!("User Disconnected Before Entering Username. ")},
        Ok(_) => {println!("This is the username {:?}", username.trim())},
        Err(e) => {println!("Failed to readline")}
    }

    let username = username.trim().to_string();
    let (client_tx, client_rx) = mpsc::channel(100);
    println!("THIS IS THE USERNAME CLONE {:?}", username.clone());

    {
        let mut streams_tx_locked = streams_transmitters.lock().await;
        match streams_tx_locked.get(&username) {
            Some(val) => {
                let formatted_message = &format!("Username {:?} already exist, please enter another username", username);
                write_stream.write_all(formatted_message.as_bytes()).await;
            },
            None => {
                let username_clone = (username.clone()).trim().to_string();
                streams_tx_locked.insert(username_clone, client_tx);
            }
        }
    }

    tokio::spawn(async move {
        handle_client(reader, write_stream, username, client_rx, server_tx).await;
    });
}


/*
    1. Should receive the stream (Params) (DONE)
    2. Split the stream into read and write (DONE)
    3. Should receive the server transmitter (Params) (DONE)
    4. Should Receive the username (Params) (DONE)
    5. Should receive the receiver of the client itself (Params) (DONE)
    6. This function should implement select for seclecting between the future that are ready for the client (Read, or to write to someone , or recieveing a message via channels) (DONE)
    7. Add a feature in which when user discinnects the tx should be removed from the global state in the Ok(0) whe user disconnects 
    8. Now this function should listen to the receiver end of the mpsc channel (DONE)
    9. The mpsc channel message should be also be decode what does it say,so parse that message too 
    10. // Self -- List, Error, Help  Server - Msg, Broad, Quit  (DONE)
*/
async fn handle_client(mut buf_read_stream: BufReader<OwnedReadHalf>, mut write_stream: OwnedWriteHalf, username: String, mut client_rx: Receiver<String>, server_tx: Sender<Command>) {
    println!("Inside the handle_client");
 
    let mut reader_message = String::new();
    loop {
        tokio::select! {
            // val = channel_message = client_rx.recv().await
            read_result =  buf_read_stream.read_line(&mut reader_message) => {
                match read_result {
                    Ok(0)=>{
                        println!("User Disconnected.");
                        let quit_command = Command{ Event: ClientEvent::Quit, Payload: MessagePayload{Sender_Username: username.clone(), Receiver_Username: String::new(), Message: String::new()}};
                        server_tx.send(quit_command).await;
                        break;
                    },
                    Ok(_) => {
                        println!("This is the reader_message {:?}", reader_message.trim());
                        let command = parse_message(username.clone(), &reader_message);
                        
                        match command.Event {
                           ClientEvent::Msg => {
                            println!("SENDING MESSAGE TO SERVER VIA CHANNELS IN HANDLE CLIENT");
                            match server_tx.send(command).await {
                                Ok(_) => {println!("MESSAGE SENT TO THE SERVER VIA HANDLE CLIENT");}
                                _ => {return}
                            }
                           }, 
                           ClientEvent::Broadcast => {
                            server_tx.send(command).await;
                           },
                           ClientEvent::Quit => {
                            match server_tx.send(command).await {
                                Ok(_) => {println!("Message Payload to Server")},
                                Err(e) => {println!("Message Not sent to the Server {e:?}")}
                            }
                            println!("User disconnected successfully");
                            break;
                           },
                           ClientEvent::List => {
                            server_tx.send(command).await;
                           },
                           ClientEvent::Help => {
                            let message = command.Payload.Message;
                            let formatted_message = format!("\n Server: {} \n", message);
                            write_stream.write_all(formatted_message.as_bytes()).await;
                            // write_stream.write_all()
                           },
                           ClientEvent::Error => {
                            println!("Inside the Erro of the handle_client function");
                            let message = command.Payload.Message;
                            let formatted_message = &format!("\n Server: ERROR {} \n", message);
                            write_stream.write_all(formatted_message.as_bytes()).await;
                           },
                           _ => {}
                        }
                        // Msg, Braod, List, Quit, Help ,Error
                        reader_message.clear();
                    },
                    Err(e) => {println!("Failed to readline {e}")}
                }
            }
            
            messages_from_channel = client_rx.recv() =>{
                println!("This is the message recived from the channel {messages_from_channel:?}");
                match messages_from_channel {
                    Some(raw_message) => {
                        let cleaned_message = raw_message.replace("\\n", "").replace('\r', "").replace('"', "");
                        println!("RAW MESSAGE {raw_message:?}");
                        println!("CLEANED MESSAGE {cleaned_message:?}");
                        // TODO - add pattern matching for the below split message[0] and [1] also add that when i receive message from the server
                        let spilt_message:Vec<&str> = cleaned_message.split(':').collect();
                        let sender_username = spilt_message[0];
                        println!("THIS IS THE USERNAME FROM THE SERVER {sender_username:?}");
                        let message = spilt_message[1].trim();
                        println!("THIS IS THE MESSAGE FROM THE SERVER {message:?}");
                        let prefix = if sender_username == "Server" {"Server"} else {sender_username};
                        let formatted_message = format!("{}: {}\n", prefix , message);
                        write_stream.write_all(formatted_message.as_bytes()).await;
                    },
                    _ =>{
                        break;
                    }
                }
            }
        }
    } 
}


fn parse_message(sender_username: String, message: &String) -> Command {
    
    if message.starts_with("/") {
        println!("this is the message while parsing");
        let splitting_message: Vec<&str> = message.splitn(4, "/").collect();
        println!("this is the spltting message first index PARSE MESSAGE {:?}", splitting_message[1] );
        match  splitting_message[1].trim() {
            
            "msg" => {
                match (splitting_message.get(2), splitting_message.get(3)) {
                    (Some(receiver_username), Some(receiver_message)) => {
                        println!("This is the receiver username {:?}", receiver_username);
                        println!("This is message to the receiver {:?}", receiver_message.trim());
                        Command { Event: ClientEvent::Msg, Payload: MessagePayload{Sender_Username: sender_username, Receiver_Username: receiver_username.to_string(), Message:  (receiver_message.trim()).to_string()}}
                    },
                    (None, _) => {
                        println!("Username empty, please enter a Username");
                        let formatted_message = format!("Username empty, please enter a Username");
                        Command { Event: ClientEvent::Error, Payload: MessagePayload{Sender_Username: sender_username.clone(), Receiver_Username: sender_username.clone(), Message:  formatted_message.to_string()}}
                    },
                    (_, None) => {
                        println!("Message empty, please enter a message");
                        let formatted_message = format!("Message empty, please enter a message");
                        Command { Event: ClientEvent::Error, Payload: MessagePayload{Sender_Username: sender_username.clone(), Receiver_Username: sender_username.clone(), Message:  formatted_message.to_string()}}
                    }
                }
            },
            "list" => {
                println!("Inside list");
                Command { Event: ClientEvent::List, Payload: MessagePayload{Sender_Username: sender_username.clone(), Receiver_Username: String::new(), Message:  String::new()}}
            },
            "broadcast" => {
                println!("Inside broadcast");
                let formatted_message = format!("Broadcast");
                Command { Event: ClientEvent::Broadcast, Payload: MessagePayload{Sender_Username: sender_username.clone(), Receiver_Username: String::new(), Message: formatted_message.to_string()}}
            },
            "quit" => {
                println!("Inside quit");
                Command { Event: ClientEvent::Quit, Payload: MessagePayload{Sender_Username: sender_username.clone(), Receiver_Username: String::new(), Message:  String::new()}}
            },
            "help" => {
                println!("Inside help");
                let formatted_message = format!("
                            /msg - msg that particular user \r\n
                            /list - list all the users connected to the server \r\n
                            /broadcast - broadcast message to all the users in the server \r\n
                            /help - list all the cmds with the description on what do they do \r\n
                            /quit - quit the message or disconnect the user \r\n");
                Command { Event: ClientEvent::Help, Payload: MessagePayload{Sender_Username: sender_username.clone(), Receiver_Username: sender_username.clone(), Message:  formatted_message.to_string()}}
            },
            _ => {
                let formatted_message = format!("Cmd Not Found");
                Command { Event: ClientEvent::Error, Payload: MessagePayload{Sender_Username: sender_username.clone(), Receiver_Username: sender_username.clone(), Message:  formatted_message.to_string()}}
            }
        }
    }
    else {
        println!("Unknown command. Use /msg/username/message");
        let formatted_message = format!("Unknown command. Use /msg/username/message");
        Command { Event: ClientEvent::Error, Payload: MessagePayload{Sender_Username: sender_username.clone(), Receiver_Username: sender_username.clone(), Message:  formatted_message.to_string()}}
    }
}
























// // 1. I need to create a loop trhat accepts the clients 
// // 2. concentrate on importing it correctly anmd addin it in th cargo.toml


// use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader, ReadHalf}, net::{TcpListener, TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf, WriteHalf}}, stream};
// use std::{collections::HashMap, error::Error, sync::{Mutex, Arc}, rc::Rc};


// #[tokio::main]
// async fn main()-> Result<(), Box<dyn Error>> {

//     let mut streams : Arc<Mutex<HashMap<String, Arc<OwnedWriteHalf>>>> = Arc::new(Mutex::new(HashMap::new()));
//     let mut streams_clone = Arc::clone(&streams);
    
//     let listener  = TcpListener::bind("127.0.0.1:8080").await?;


//     loop {
//         let (stream, _) = listener.accept().await?;
//         let (read_stream, mut write_stream) = stream.into_split();
//         let formatted_messsage = &format!("\n Please enter your username: ");
//         write_stream.write_all(formatted_messsage.as_bytes()).await? ;
//         let mut reader = BufReader::new(read_stream);
//         let mut username  = String::new();
//         reader.read_line(&mut username).await?;

//         { 
//             println!("This is the the username {username:?}");
//             let mut streams_locked = streams_clone.lock().unwrap();
//             (streams_locked).insert(username.clone(), write_stream.into());
//         }
//         // 1. ASk username Done
//         // I need to spilt the stream into read and write half of the stream and create clones of write half and store write half in global state variables  DONE
//         // ANd then after adding them to global state i need to DONE
//         let streams_clone_1 = Arc::clone(&streams);
//         tokio::spawn(async move {
//                 read_messages(username, reader, streams_clone_1).await;
//             }
//         );
//     }
    
// }


// async fn read_messages(username: String, mut buf_reader_stream: BufReader<OwnedReadHalf>, streams : Arc<Mutex<HashMap<String, Arc<OwnedWriteHalf>>>>) -> Result<(), Box<dyn Error>>  {
//     println!("Inside the read one");
//     loop{
//         let mut message = String::new();
//         buf_reader_stream.read_line(&mut message).await;
//         // streams
//         println!("This is the message {message:?}");
//         // let result = handle_message(username.clone(), message, streams);
//         handle_message(username.clone(), message, Arc::clone(&streams));
//     }
// }

// async fn handle_message(username: String, message: String, streams : Arc<Mutex<HashMap<String, Arc<OwnedWriteHalf>>>> ) {

//     if message.starts_with("/") {
//         let spliting_message: Vec<&str> = message.splitn(4, "/").collect();
//         // if spliting_message[1]
//         println!("THis is the first split index  {:?}", spliting_message[1]);
//         // match spliting_message[1].trim_end() {
//         //     "msg" => {
//         //         let streams_locked = streams.lock().expect("Username not found");
//         //         let mut stream = streams_locked.get(&username).expect("Stream not found of the user");
//         //         let formatted_message = &format!("\n {:?}", spliting_message[2]);
//         //         (stream).write_all(formatted_message.as_bytes()).await.expect("Message not sent to the user");
//         //     },
//         //     _ => {
//         //         println!("Pleasse tell user to enter the correct cmds");
//         //     }
//         // }

//     }
//     else {
//         // here message from the server will go to the user that please send a message with appropriate syntax 

//     }
    
// }

// fn message_user(sender_username: String, receiver_username: String, streams: Arc<Mutex<HashMap<String, OwnedWriteHalf>>>, from_server: bool) {
//     //   add a check if the message is from the server that format the message in that way only 

// }