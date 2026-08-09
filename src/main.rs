use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader, ReadHalf}, net::{TcpListener, TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf, WriteHalf}}, stream, sync::{ Mutex, mpsc::{self, Receiver, Sender}}};
use std::{collections::HashMap, error::Error, rc::Rc, sync::{Arc}};


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Username and transmitter of the the clients tasks 
    let streams_channels_tx : Arc<Mutex<HashMap<String, Sender<String>>> >= Arc::new(Mutex::new(HashMap::new()));
    
    // let (server_tx, mut server_rx) = mpsc::channel(100);
    // let raw_streams: Arc<Mutex<TcpStream>>;
    
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    loop {
        let (stream, _)  = listener.accept().await?;

        let shared_stream = Arc::new(Mutex::new(stream));
        let shared_stream_clone_1 = Arc::clone(&shared_stream);

        let streams_channels_tx_clone_1 = Arc::clone(&streams_channels_tx);
        
        // 1. tokio server task
        // 2. task for asking namees from the streams and giving then than spawns a new tokio task after 

        // REASON - the reason i am spawning a thread for asking the username is i don't want to block the main thread so it keeps accepting clients and adding new transmitters in the global state.
        tokio::spawn(async move {
            ask_username(shared_stream_clone_1, streams_channels_tx_clone_1).await;
        });
    }
   
}

async fn ask_username(stream: Arc<Mutex<TcpStream>>, streams_transmitters: Arc<Mutex<HashMap<String, Sender<String>>>>) {
    let formatted_message = &format!("Please Enter Your Username: ");
    {   
        let mut locked_shared_stream = stream.lock().await;
        
        (locked_shared_stream).write_all(formatted_message.as_bytes()).await;
    }
    
    let mut stream_task = Arc::try_unwrap(stream).expect("Stream in use elsewhere").into_inner();
    let (read_stream, write_stream) = stream_task.into_split();
    let mut reader  = BufReader::new(read_stream);

    // TODO - Add a check that checks whether the username already exists or not 
    let mut username: String = String::new();
    match reader.read_line(&mut username).await {
        Ok(0) => {println!("User Disconnected Before Entering Username. ")},
        Ok(_) => {println!("This is the username {:?}", username.trim())},
        Err(e) => {println!("Failed to readline")}
    }

    let (client_tx, client_rx) = mpsc::channel(100);
    streams_transmitters.lock().await.insert(username.clone(), client_tx.clone());

    tokio::spawn(async move {
        handle_client(reader, write_stream, username, client_tx, client_rx).await;
    });
}


/*
    1. Should receive the stream (Params) DONE
    2. Split the stream into read and write DONE
    3. Should receive the server transmitter (Params)
    4. Should Receive the username (Params) DONE
    5. Should receive the receiver of the client itself (Params) DONE
    6. This function should implement select for seclecting between the future that are ready for the client (Read, or to write to someone , or recieveing a message via channels)
*/
async fn handle_client(mut buf_read_stream: BufReader<OwnedReadHalf>, write_stream: OwnedWriteHalf, username: String, client_tx: Sender<String>, mut client_rx: Receiver<String>) {
    println!("Inside the handle_client");
    let mut message= String::new();

    loop {
        match buf_read_stream.read_line(&mut message).await {
            Ok(0)=>{
                        println!("User Disconnected.");
                        break;
                    },
            Ok(_) => {println!("This is the message {:?}", message.trim())},
            Err(e) => {println!("Failed to readline")}
        }
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