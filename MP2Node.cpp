/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

#include <sstream>
static stringstream my_stream2;
#define MYLOG2(stream) my_stream2 << "{N" << memberNode->addr.getAddress() << " @" << par->getcurrtime() << "} " << stream << "\n"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

  stringstream mlist;
  for (auto member : curMemList) {
    mlist << member.nodeAddress.getAddress() << "(" << member.nodeHashCode << "),";
  }
  
  if (ring.empty()) {
    MYLOG2("initializing ring from membership list: " + mlist.str());
    for (auto node : curMemList) {
      ring.push_back(node);
    }
  }
	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
  // also inject my self
  curMemList.emplace_back(Node(this->memberNode->addr));
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
  int transID = transaction_keys.size();
  transaction_type[transID] = CREATE;
  transaction_keys[transID] = key;
  transaction_values[transID] = value;
  auto replicas = findNodes(key);
  ReplicaType repType = PRIMARY;
  for (auto replica : replicas) {
    Message msgCreate(
      transID,  // transID
      memberNode->addr,  // source address (me!)
      CREATE,  // message type
      key,
      value,
      repType  // replica type
    );
    repType = (ReplicaType)(((int)repType) + 1);
    int ret = emulNet->ENsend(&memberNode->addr, &replica.nodeAddress, msgCreate.toString());
  }
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
  int transID = transaction_keys.size();
  transaction_type[transID] = DELETE;
  transaction_keys[transID] = key;
  auto replicas = findNodes(key);
  for (auto replica : replicas) {
    Message msgDelete(
      transID,  // transID
      memberNode->addr,  // source address (me!)
      DELETE,  // message type
      key
    );
    int ret = emulNet->ENsend(&memberNode->addr, &replica.nodeAddress, msgDelete.toString());
  }
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
  return ht->create(key, value);
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
  return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message_str(data, data + size);
    Message message(message_str);
		/*
		 * Handle the message types here
		 */
    switch(message.type) {
      case CREATE: {
        bool success = createKeyValue(message.key, message.value, message.replica);
        if (success) {
          log->logCreateSuccess(&message.fromAddr, false, message.transID, message.key, message.value);
        } else {
          log->logCreateFail(&message.fromAddr, false, message.transID, message.key, message.value);
        }
        Message reply(message.transID, message.fromAddr, REPLY, success);
        emulNet->ENsend(&memberNode->addr, &message.fromAddr, reply.toString());
        break;
      }
      
      case DELETE: {
        bool success = deletekey(message.key);
        if (success) {
          log->logDeleteSuccess(&message.fromAddr, false, message.transID, message.key);
        } else {
          log->logDeleteFail(&message.fromAddr, false, message.transID, message.key);
        }
        Message reply(message.transID, message.fromAddr, REPLY, success);
        emulNet->ENsend(&memberNode->addr, &message.fromAddr, reply.toString());
        break;
      }
    
      case REPLY: {
        MYLOG2("Got REPLY message from " << message.fromAddr.getAddress() << " transaction id: " << message.transID);
        if (completed_transactions.find(message.transID) == completed_transactions.end()) {
          transactions[message.transID].push_back(message.fromAddr);
          bool fail_quorum=false, success_quorum=false;
          if (message.success) {
            success_replies[message.transID].push_back(message.fromAddr);
            if (success_replies[message.transID].size() >= QUORUM) {
              success_quorum = true;
              MYLOG2("Got success quorum for transaction " << message.transID);
            }
          } else {
            fail_replies[message.transID].push_back(message.fromAddr);
            if (fail_replies[message.transID].size() >= QUORUM) {
              fail_quorum = true;
              MYLOG2("Got failure quorum for transaction " << message.transID);
            }
          }
          if (success_quorum || fail_quorum) {
            completed_transactions.insert(message.transID);
            switch (transaction_type[message.transID]) {
              case CREATE: {
                if (success_quorum) {
                  log->logCreateSuccess(&memberNode->addr, true, message.transID, transaction_keys[message.transID], transaction_values[message.transID]);
                } else {
                  log->logCreateFail(&memberNode->addr, true, message.transID, transaction_keys[message.transID], transaction_values[message.transID]);
                }
                break;
              }
              
              case DELETE: {
                if (success_quorum) {
                  log->logDeleteSuccess(&message.fromAddr, true, message.transID, transaction_keys[message.transID]);
                } else {
                  log->logDeleteFail(&message.fromAddr, true, message.transID, transaction_keys[message.transID]);
                }
                break;
              }
            }
          }
        }
        break;
      }
    
      default: {
        MYLOG2("don't know what to do with message " << message_str);
      }
    }
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
}
