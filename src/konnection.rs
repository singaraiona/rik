use std::io::{self, BufReader, BufWriter};
use std::io::prelude::*;
use std::mem;
use std::net::*;
use std::result::Result;
use std::os::unix::io::{IntoRawFd, FromRawFd};

#[derive(Debug)]
pub struct Konnection {
    rx: BufReader<TcpStream>,
    tx: BufWriter<TcpStream>,
    pub buf: Vec<u8>,
    hp: String,
    cred: String,
    ver: u8,
}

#[derive(Debug)]
#[repr(packed)]
struct KMessageHeader {
    endian: i8,
    msg_type: i8,
    compress: i8,
    unused: i8,
    size: i32,
}

#[derive(Debug)]
#[repr(packed)]
struct KVectorHeader {
    val_type: i8,
    attrib: i8,
    len: i32,
}

fn struct_to_bytes<T>(s: &T) -> &[u8] {
    unsafe {
        ::std::slice::from_raw_parts(s as *const _ as *const _, mem::size_of::<T>())
    }
}

fn struct_to_bytes_mut<T>(s: &mut T) -> &mut [u8] {
    unsafe {
        ::std::slice::from_raw_parts_mut(s as *mut _ as *mut _, mem::size_of::<T>())
    }
}

fn read_all(r: &mut BufReader<TcpStream>, buf: &mut [u8]) {
    let len = buf.len();
    let mut n = 0;
    while n < len {
        n += r.read(&mut buf[n..]).unwrap();
    }
}

impl Konnection {

    pub fn konnect(hostport: &str, name: &str, passwd: &str)
                   -> io::Result<Konnection> {

        let hp = String::from(hostport);
        let sock = try!(TcpStream::connect(hostport));
        let cred = format!("{}:{}", name, passwd);
        let msg = format!("{}\x01\x00", cred);

        let sockfd = sock.into_raw_fd();
        let rx = unsafe { BufReader::new(TcpStream::from_raw_fd(sockfd)) };
        let tx = unsafe { BufWriter::new(TcpStream::from_raw_fd(sockfd)) };

        let mut konn = Konnection { rx: rx,
                                    tx: tx,
                                    buf: Vec::with_capacity(0),
                                    hp: hp,
                                    cred: cred,
                                    ver: 0,
        };

        konn.tx.write(msg.as_bytes()).unwrap();
        konn.tx.flush().unwrap();

        let mut resp = [0u8];
        let rd = konn.rx.read(&mut resp).unwrap();
        assert!(rd == 1);

        konn.ver = resp[0];

        Ok(konn)
    }

    pub fn query(&mut self, q: &str) -> i32 {
        let size = (mem::size_of::<KMessageHeader>()
            + mem::size_of::<KVectorHeader>()
            + q.len()) as i32;

        let mhdr = KMessageHeader { endian:1, msg_type:1, compress:0, unused:0, size:size };
        let vhdr = KVectorHeader { val_type:10, attrib:0, len:q.len() as i32 };

        self.tx.write_all(struct_to_bytes(&mhdr)).unwrap();
        self.tx.write_all(struct_to_bytes(&vhdr)).unwrap();
        self.tx.write_all(q.as_bytes()).unwrap();
        self.tx.flush().unwrap();

        size
    }

    pub fn read_message(&mut self) -> &[u8] {
        let mut mhdr: KMessageHeader = unsafe { mem::uninitialized() };
        read_all(&mut self.rx, struct_to_bytes_mut(&mut mhdr));
        let payload_size = mhdr.size as usize - mem::size_of::<KMessageHeader>();
        unsafe {
            self.buf.reserve(payload_size);
            self.buf.set_len(payload_size);
            read_all(&mut self.rx, self.buf.as_mut_slice());
        }
        self.buf.as_slice()
    }
}

