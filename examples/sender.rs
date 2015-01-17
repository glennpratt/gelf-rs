extern crate gelf;

use gelf::message::*;
use std::io::net::udp::*;
use std::io::test::*;
use std::sync::mpsc::channel;
use std::thread::Thread;
use std::io::net::ip::{Ipv4Addr, SocketAddr};

fn main() {
    udp_sender();
}

fn udp_sender() {
    let listen_addr = SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 9600 };
    let send_addr = SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 9601 };
    let json = r#"{"message":"foo","host":"bar","_utf8":"✓"}"#;

    match UdpSocket::bind(send_addr) {
        Ok(ref mut client) => {
            println!("sending from {} to {}", send_addr, listen_addr);
            client.send_to(json.as_bytes(), listen_addr).unwrap()
        }
        Err(..) => panic!()
    }
}
