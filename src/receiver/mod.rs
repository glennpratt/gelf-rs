use message::*;
use std::io::net::udp::*;
use std::io::test::*;
use std::sync::{Arc, TaskPool};
use std::sync::mpsc::channel;
use std::thread::Thread;
use std::io::net::ip::{Ipv4Addr, SocketAddr};

pub struct Receiver<H> {
    handler: H
}

impl<H: Handler> Receiver<H> {
    pub fn new(handler: H) -> Receiver<H> {
        Receiver { handler: handler }
    }
    
    pub fn listen(self, listen_addr: SocketAddr) {
        // let listen_addr = SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 9600 };
        // println!("{}", listen_addr);
        
        match UdpSocket::bind(listen_addr) {
            Ok(ref mut server) => {
                let pool = TaskPool::new(100);
                let handler = Arc::new(self.handler);
                loop {
                    // From gelfclient... CHUNK_MAGIC_BYTES(2) + messageId(8) + sequenceNumber(1) + sequenceCount(1) + MAX_CHUNK_SIZE(1420)
                    let mut buf = [0; 1432];
                    match server.recv_from(&mut buf) {
                        
                        Ok((n_read, _)) => {
                            let handler = handler.clone();
                            pool.execute(move || {
                                
                                let packet = buf.as_slice().slice_to(n_read);
                                match unpack(packet).unwrap() {
                                    Partial(_) => assert!(false, "Expected 'Complete' result."),
                                    Complete(s) => handler.call(s)
                                }
                            });
                        }
                        Err(..) => panic!()
                    }
                }
            }
            Err(..) => panic!()
        }
    }
}

// pub fn udp_receiver_smoke_test<H: Handler>(handler: H) {
//     let listen_addr = SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 9600 };
//     println!("{}", listen_addr);
// 
//     match UdpSocket::bind(listen_addr) {
//         Ok(ref mut server) => {
//             let pool = TaskPool::new(100);
//             let handler = Arc::new(handler);
//             loop {
//                 // From gelfclient... CHUNK_MAGIC_BYTES(2) + messageId(8) + sequenceNumber(1) + sequenceCount(1) + MAX_CHUNK_SIZE(1420)
//                 let mut buf = [0; 1432];
//                 match server.recv_from(&mut buf) {
// 
//                     Ok((n_read, _)) => {
//                         let handler = handler.clone();
//                         pool.execute(move || {
//                             println!("hi mom");
//                         
//                             let packet = buf.as_slice().slice_to(n_read);
//                             match unpack(packet).unwrap() {
//                                 Partial(_) => assert!(false, "Expected 'Complete' result."),
//                                 Complete(s) => handler.call(s)
//                             }
//                         });
//                     }
//                     Err(..) => panic!()
//                 }
//             }
//         }
//         Err(..) => panic!()
//     }
// }

pub trait Handler: Send + Sync {
    /// Produce a `Response` from a Request, with the possibility of error.
    ///
    /// If this returns an Err, `catch` is called with the error.
    fn call(&self, String);
}

impl<F: Send + Sync + for<'a> Fn(String)> Handler for F {
    fn call(&self, message: String) {
        (*self)(message)
    }
}
