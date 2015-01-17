extern crate gelf;

use gelf::receiver::Receiver;
use std::io::net::ip::{Ipv4Addr, SocketAddr};

fn main() {
    fn message_printer(message: String) {
        println!("{}", message);
    };
    
    let listen_addr = SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 9600 };
    Receiver::new(message_printer).listen(listen_addr);
    // udp_receiver_smoke_test(|&: message: String| {
    //     println!("{}", message);
    // });
}
