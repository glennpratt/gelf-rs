#![feature(collections, core, io, std_misc, test)]
#![feature(old_io)]

extern crate flate2;
extern crate time;
#[cfg(test)]
extern crate rand;

pub mod message;
pub mod receiver;
