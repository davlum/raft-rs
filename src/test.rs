
#[cfg(test)]
mod tests {
    use crate::main;
    use std::net::{TcpStream};
    use std::io::{Read, Write};
    use std::str::from_utf8;
    use std::thread;
    use std::time;

    #[test]
    fn test_run_with_config() {
        let addr = "127.0.0.1:7878";
        thread::spawn(move || main());
        let one_sec= time::Duration::from_secs(1);
        let mut stream = TcpStream::connect(addr);
        while let Err(_) = stream {
            println!("Failed to connect. Sleeping...");
            std::thread::sleep(one_sec);
            let mut stream = TcpStream::connect(addr);
        }
        let mut stream = stream.unwrap();
        let ten_millis = time::Duration::from_millis(2000);
        println!("Successfully connected to server in port 7878");

        let msg = r#"{"RV":{"node_id":"1","term":1}}\n"#;
        println!("msg is {}", msg);
        stream.write(msg.as_bytes()).unwrap();
        println!("Sent Hello, awaiting reply...");

        let mut data: [u8; 1024] = [0; 1024];
        match stream.read(&mut data) {
            Ok(_) => {
                if &data == msg.as_bytes() {
                    println!("Reply is ok!");
                } else {
                    let text = from_utf8(&data).unwrap();
                    println!("Unexpected reply: {}", text);
                }
            },
            Err(e) => {
                println!("Failed to receive data: {}", e);
            }
        }
    }
}
