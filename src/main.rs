// 1. I need to create a loop trhat accepts the clients 
// 2. concentrate on importing it correctly anmd addin it in th cargo.toml


use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader, ReadHalf}, net::{TcpListener, TcpStream, tcp::WriteHalf, tcp::{OwnedReadHalf, OwnedWriteHalf}}};
use std::{collections::HashMap, error::Error, sync::{Mutex, Arc}, rc::Rc};


#[tokio::main]
async fn main()-> Result<(), Box<dyn Error>> {

    let mut streams : Arc<Mutex<HashMap<String, OwnedWriteHalf>>> = Arc::new(Mutex::new(HashMap::new()));
    let mut streams_clone = Arc::clone(&streams);
    
    let listener  = TcpListener::bind("127.0.0.1:8080").await?;


    loop {
        let (stream, _) = listener.accept().await?;
        let (read_stream, mut write_stream) = stream.into_split();
        let formatted_messsage = &format!("\n Please enter your username: ");
        write_stream.write_all(formatted_messsage.as_bytes()).await? ;
        let mut reader = BufReader::new(read_stream);
        let mut username  = String::new();
        reader.read_line(&mut username).await?;

        { 
            println!("This is the the username {username:?}");
            let mut streams_locked = streams_clone.lock().unwrap();
            (streams_locked).insert(username.clone(), write_stream);
        }
        // 1. ASk username Done
        // I need to spilt the stream into read and write half of the stream and create clones of write half and store write half in global state variables  DONE
        // ANd then after adding them to global state i need to DONE
        let streams_clone_1 = Arc::clone(&streams);
        tokio::spawn(async move {
                read_messages(username, reader, streams_clone_1).await;
            }
        );
    }
    
}


async fn read_messages(username: String, mut buf_reader_stream: BufReader<OwnedReadHalf>, streams : Arc<Mutex<HashMap<String, OwnedWriteHalf>>>) -> Result<(), Box<dyn Error>>  {
    println!("Inside the read one");
    loop{
        let mut message = String::new();
        buf_reader_stream.read_line(&mut message).await;
        // streams
        println!("This is the message {message:?}");
        // let result = handle_message(username.clone(), message, streams);
        handle_message(username.clone(), message, Arc::clone(&streams));
    }
}

async fn handle_message(username: String, message: String, streams : Arc<Mutex<HashMap<String, OwnedWriteHalf>>> ) {

    if message.starts_with("/") {
        let spliting_message: Vec<&str> = message.splitn(4, "/").collect();
        // if spliting_message[1]
        println!("THis is the first split index  {:?}", spliting_message[1]);
        // match spliting_message[1].trim_end() {
        //     "msg" => {
        //         let streams_locked = streams.lock().expect("Username not found");
        //         let mut stream = streams_locked.get(&username).expect("Stream not found of the user");
        //         let formatted_message = &format!("\n {:?}", spliting_message[2]);
        //         (stream).write_all(formatted_message.as_bytes()).await.expect("Message not sent to the user");
        //     },
        //     _ => {
        //         println!("Pleasse tell user to enter the correct cmds");
        //     }
        // }

    }
    else {
        // here message from the server will go to the user that please send a message with appropriate syntax 

    }
    
}

fn message_user(sender_username: String, receiver_username: String, streams: Arc<Mutex<HashMap<String, OwnedWriteHalf>>>, from_server: bool) {
    //   add a check if the message is from the server that format the message in that way only 

}