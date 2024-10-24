use super::message::Message;
use super::peer;
use super::server::Handle as ServerHandle;
use crate::types::hash::H256;

use log::{debug, warn, error};

use std::thread;

#[cfg(any(test,test_utilities))]
use super::peer::TestReceiver as PeerTestReceiver;
#[cfg(any(test,test_utilities))]
use super::server::TestReceiver as ServerTestReceiver;
#[derive(Clone)]
pub struct Worker {
    msg_chan: smol::channel::Receiver<(Vec<u8>, peer::Handle)>,
    num_worker: usize,
    server: ServerHandle,
}


impl Worker {
    pub fn new(
        num_worker: usize,
        msg_src: smol::channel::Receiver<(Vec<u8>, peer::Handle)>,
        server: &ServerHandle,
    ) -> Self {
        Self {
            msg_chan: msg_src,
            num_worker,
            server: server.clone(),
        }
    }

    pub fn start(self) {
        let num_worker = self.num_worker;
        for i in 0..num_worker {
            let cloned = self.clone();
            thread::spawn(move || {
                cloned.worker_loop();
                warn!("Worker thread {} exited", i);
            });
        }
    }

    fn worker_loop(&self) {
        let mut buffer = Vec::new()
        loop {
            let result = smol::block_on(self.msg_chan.recv());
            if let Err(e) = result {
                error!("network worker terminated {}", e);
                break;
            }
            let (msg_bytes, mut peer) = result.unwrap();
            let msg: Message = match bincode::deserialize(&msg_bytes){
                Ok(m) => m,
                Err(e) => {
                    error!("Failed to deserialize message: {}", e);
                    continue;
                },
            };

            match msg {
                Message::NewBlockHashes(hashes) => {
                    let missing_hashes: Vec<_> = hashes.iter()
                        .filter(|hash| !self.server.has_block(hash))
                        .cloned()
                        .collect();
                    if !missing_hashes.is_empty() {
                        peer.write(Message::GetBlocks(missing_hashes)).unwrap();
                    }
                },
                Message::GetBlocks(hashes) => {
                    let blocks: Vec<_> = hashes.iter()
                        .filter_map(|hash| self.server.get_block(hash))
                        .collect();
                    if !blocks.is_empty() {
                        peer.write(Message::Blocks(blocks)).unwrap();
                    }
                },
                Message::Blocks(new_blocks) => {
                    let mut new_hashes = Vec::new();

                    for block in new_blocks {
                        if !self.server.insert_block(&block) {
                            continue;
                        }
                        new_hashes.push(block.hash());

                        if let Some(parent_hash) = block.parent_hash() {
                            buffer.retain(|b| {
                                if b.parent_hash() == Some(parent_hash) {
                                    self.server.insert_block(b);
                                    false
                                } else {
                                    true
                                }
                            });
                        }
                    }
                    if !new_hashes.is_empty() {
                        self.server.broadcast(Message::NewBlockHashes(new_hashes)).unwrap();
                    }
                },
                Message::Ping(nonce) => {
                    debug!("Ping: {}", nonce);
                    peer.write(Message::Pong(nonce.to_string()));
                }
                Message::Pong(nonce) => {
                    debug!("Pong: {}", nonce);
                }
                _ => unimplemented!(),
            }
        }
    }
}

#[cfg(any(test,test_utilities))]
struct TestMsgSender {
    s: smol::channel::Sender<(Vec<u8>, peer::Handle)>
}
#[cfg(any(test,test_utilities))]
impl TestMsgSender {
    fn new() -> (TestMsgSender, smol::channel::Receiver<(Vec<u8>, peer::Handle)>) {
        let (s,r) = smol::channel::unbounded();
        (TestMsgSender {s}, r)
    }

    fn send(&self, msg: Message) -> PeerTestReceiver {
        let bytes = bincode::serialize(&msg).unwrap();
        let (handle, r) = peer::Handle::test_handle();
        smol::block_on(self.s.send((bytes, handle))).unwrap();
        r
    }
}
#[cfg(any(test,test_utilities))]
/// returns two structs used by tests, and an ordered vector of hashes of all blocks in the blockchain
fn generate_test_worker_and_start() -> (TestMsgSender, ServerTestReceiver, Vec<H256>) {
    let (server, server_receiver) = ServerHandle::new_for_test();

    let mut blockchain = server.get_blockchain();
    let genesis_block = blockchain.create_genesis_block();
    blockchain.add_block(genesis_block.clone());

    let (test_msg_sender, msg_chan) = TestMsgSender::new();

    let worker = Worker::new(1, msg_chan, &server);
    worker.start(); 

    let block_hashes = blockchain.longest_chain_hashes(); 

    (test_msg_sender, server_receiver, block_hashes)
}

// DO NOT CHANGE THIS COMMENT, IT IS FOR AUTOGRADER. BEFORE TEST

#[cfg(test)]
mod test {
    use ntest::timeout;
    use crate::types::block::generate_random_block;
    use crate::types::hash::Hashable;

    use super::super::message::Message;
    use super::generate_test_worker_and_start;

    #[test]
    #[timeout(60000)]
    fn reply_new_block_hashes() {
        let (test_msg_sender, _server_receiver, v) = generate_test_worker_and_start();
        let random_block = generate_random_block(v.last().unwrap());
        let mut peer_receiver = test_msg_sender.send(Message::NewBlockHashes(vec![random_block.hash()]));
        let reply = peer_receiver.recv();
        if let Message::GetBlocks(v) = reply {
            assert_eq!(v, vec![random_block.hash()]);
        } else {
            panic!();
        }
    }
    #[test]
    #[timeout(60000)]
    fn reply_get_blocks() {
        let (test_msg_sender, _server_receiver, v) = generate_test_worker_and_start();
        let h = v.last().unwrap().clone();
        let mut peer_receiver = test_msg_sender.send(Message::GetBlocks(vec![h.clone()]));
        let reply = peer_receiver.recv();
        if let Message::Blocks(v) = reply {
            assert_eq!(1, v.len());
            assert_eq!(h, v[0].hash())
        } else {
            panic!();
        }
    }
    #[test]
    #[timeout(60000)]
    fn reply_blocks() {
        let (test_msg_sender, server_receiver, v) = generate_test_worker_and_start();
        let random_block = generate_random_block(v.last().unwrap());
        let mut _peer_receiver = test_msg_sender.send(Message::Blocks(vec![random_block.clone()]));
        let reply = server_receiver.recv().unwrap();
        if let Message::NewBlockHashes(v) = reply {
            assert_eq!(v, vec![random_block.hash()]);
        } else {
            panic!();
        }
    }
}

// DO NOT CHANGE THIS COMMENT, IT IS FOR AUTOGRADER. AFTER TEST
