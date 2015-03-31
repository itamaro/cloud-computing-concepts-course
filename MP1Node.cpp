/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"


#include <sstream>
static stringstream my_stream;
#define MYLOG(stream) my_stream << "{N" << memberNode->addr.getAddress() << " @" << par->getcurrtime() << "} " << stream << "\n"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
  swimState = Idle;
  timeToNextPing = SWIM_PERIOD;
  //pingTimeout = -1;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return 0;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
  // int id = *(int*)(&memberNode->addr.addr);
  // int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
  // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
  initMemberListTable(memberNode);

  return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
#ifdef DEBUGLOG
    static char s[1024];
#endif
    // create initial notification that I'm alive
    Notification alive;
    alive.addr = memberNode->addr;
    alive.type = Alive;
    alive.stateTimestamp = par->getcurrtime();
    alive.incarnation = 1;
    insertNotification(alive);
    //notifications.push_front(alive);

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif
        sendMessage(joinaddr, Ping);
    }

    return 1;
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
  MYLOG("called finishUpThisNode");
  return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
  if (memberNode->bFailed) {
  	return;
  }

  // Check my messages
  checkMessages();

  // Wait until you're in the group...
  if( !memberNode->inGroup ) {
  	return;
  }

  // ...then jump in and share your responsibilites!
  nodeLoopOps();

  return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
  void *ptr;
  int size;

  // Pop waiting messages from memberNode's mp1q
  while ( !memberNode->mp1q.empty() ) {
  	ptr = memberNode->mp1q.front().elt;
  	size = memberNode->mp1q.front().size;
  	memberNode->mp1q.pop();
  	recvCallBack((void *)memberNode, (char *)ptr, size);
  }
  return;
}

int MP1Node::sendMessage(Address* dest_addr, MsgTypes msg_type) {
  // allocate maximal buffer for message
  char * buffer = (char *)malloc(par->MAX_MSG_SIZE * sizeof(char));
  char * pos = buffer;
  // set message type and destination address
  ((MessageHdr*)pos)->msgType = msg_type;
  pos += sizeof(MessageHdr);
  memcpy(pos, &memberNode->addr.addr, sizeof(memberNode->addr.addr));
  pos += sizeof(memberNode->addr.addr);
  // add notifications, from newest to oldest, as long as it fits in the buffer
  unsigned count = 0;
  for (const auto& notification : notifications) {
    if (pos + notification.size() - buffer > par->MAX_MSG_SIZE) {
      MYLOG("breaking out of notifications loop after " << count << " notifications");
      break;
    }
    pos += notification.serialize(pos);
    ++count;
  }
  // compute effective message length and send it
  size_t buffer_len = pos - buffer;
  int ret = emulNet->ENsend(&memberNode->addr, dest_addr, buffer, buffer_len);
  free(buffer);
  return ret;
}

vector<MyNode>::iterator MP1Node::getNode(Address addr) {
  vector<MyNode>::iterator it;
  for (it = nodes.begin(); it != nodes.end(); ++it) {
    if (it->addr == addr) {
      // got it
      break;
    }
  }
  return it;
}

// returns 0 if no node with address addr in removed nodes
// otherwise returns incarnation number (1..)
unsigned MP1Node::getRemovedNodeIncarnation(Address addr) {
  vector<MyNode>::iterator it;
  for (auto& node : removed_nodes) {
    if (node.addr == addr) {
      // got it
      return node.incarnation;
    }
  }
  return 0;
}

void MP1Node::insertNotification(const Notification& notification) {
  // insert the notification before the first element with older stateTimestamp
  auto it = notifications.begin();
  while (it != notifications.end()) {
    if (notification.stateTimestamp > it->stateTimestamp) {
      // found it
      break;
    }
    ++it;
  }
  notifications.insert(it, notification);
}

void MP1Node::garbageCollectNotifications() {
  // remove old notifications (TODO)
  // remove contradicting notifications
  list<Address> seen_addresses;
  auto it = notifications.begin();
  while (it != notifications.end()) {
    bool found = false;
    for (const auto& seen_addr : seen_addresses) {
      if (it->addr == seen_addr) {
        found = true;
        break;
      }
    }
    if (!found) {
      // first notification for this address
      seen_addresses.push_back(it->addr);
      ++it;
    } else {
      // repeat notification for this address - remove
      MYLOG("Removing repeat notification " << it->toString());
      it = notifications.erase(it);
    }
  }
}

void MP1Node::addNodeFromNotification(Notification notification) {
  // I'm assuming this node is not in my current nodes list
  assert(getNode(notification.addr) == nodes.end());
  MyNode node(notification.addr, notification.type, notification.stateTimestamp, notification.incarnation);
  nodes.push_back(node);
  MemberListEntry mle;
	memcpy(&mle.id, &node.addr.addr[0], sizeof(int));
	memcpy(&mle.port, &node.addr.addr[4], sizeof(short));
  memberNode->memberList.push_back(mle);
  log->logNodeAdd(&memberNode->addr, &node.addr);
  MYLOG("Added node after processing notification " << notification.toString());

  return;
  
  // also delete it from removed nodes if it's there
  for (auto it = removed_nodes.begin(); it != removed_nodes.end(); ++it) {
    if (it->addr == node.addr) {
      removed_nodes.erase(it);
      break;
    }
  }
}

bool MP1Node::processNotification(Notification notification) {
  // not handling notifications on self!
  assert(!(notification.addr == memberNode->addr));
  
  // is this a removed node?
  unsigned removed_incarnation = getRemovedNodeIncarnation(notification.addr);
  if (removed_incarnation > 0) {
    // got a notification about a node that I've previously removed
    if (notification.type == Fail || notification.type == Leave) {
      // this is not interesting - I already removed it
      return false;
    }
    // notification that node that I've removed is Alive or Suspect
    if (notification.incarnation > removed_incarnation) {
      // notification has info about new incarnation of this node, so I should add it
      addNodeFromNotification(notification);
      return true;
    }
    return false;
  }
  
  // got notification about a node that I did not previously remove
  // is this node on my nodes list?
  auto node = getNode(notification.addr);
  if (node == nodes.end()) {
    // got a notification about a node that I'm not familiar with
    // propagate the notification
    // add the node, if it's in an addable state
    if (notification.type == Alive || notification.type == Suspect) {
      addNodeFromNotification(notification);
    }
    return true;
  }
  
  // got a notification about a node that I'm familiar with, and is on my nodes list
  if (notification.incarnation > node->incarnation) {
    // the notification has newer incarnation, so I should adopt it verbatim, and propagate the notification
    node->state = notification.type;
    if (node->state == Suspect) {
      node->timeToRemove = TREMOVE;
    }
    node->incarnation = notification.incarnation;
    return true;
  }
  if (notification.incarnation < node->incarnation) {
    // older incarnation - discard notification
    return false;
  }
  // notification has same incarnation number - so use state vs. type
  if (node->state == Fail || node->state == Leave) {
    // a previous notification in this cycle already marked this node as failed/left, so nothing more to do now
    return false;
  }
  if (notification.type == Fail || notification.type == Leave) {
    // oh my, I should mark the node as failed / left, so it is removed when I do LoopOps
    node->state = notification.type;
    return true;
  } else if (notification.type == Suspect) {
    if (node->state == Alive) {
      // Suspect takes over Alive
      node->state = Suspect;
      node->timeToRemove = TREMOVE;
      return true;
    }
    // else - We're both on Suspect, so nothing to do
    if (node->state != Suspect) {
      MYLOG("node->state=" + to_string(node->state));
    }
    if (notification.type != Suspect) {
      MYLOG("notification.type=" + notification.typeString());
    }
    assert(node->state == Suspect && notification.type == Suspect);
    return false;
  }
  // else - notification says Alive, so not interesting to me
  assert(notification.type == Alive);
  return false;
}

void MP1Node::processNotifications(char * data_begin, char * data_end) {
  char * pos = data_begin;
  while (pos < data_end) {
    Notification incoming_notification;
    pos += incoming_notification.parseFromBuffer(pos);
    //MYLOG("Processing notification " << );
    if (incoming_notification.addr == memberNode->addr) {
      // hey, it's me!
      if (incoming_notification.type == Suspect) {
        // say what?
        // TODO
      }
      // TODO other types?
    } else {
      // not me
      if (processNotification(incoming_notification)) {
        // propagate notification
        MYLOG("Propagating notification " << incoming_notification.toString());
        insertNotification(incoming_notification);
      }
    }
  }
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size) {
  MsgTypes msgType = ((MessageHdr*)data)->msgType;
  Address src_addr;
  memcpy(&src_addr, data + sizeof(MessageHdr), sizeof(Address));
	//MYLOG("Received message from " << src_addr.getAddress() << ", msgType=" << msgType);
  
  processNotifications(data + sizeof(MessageHdr) + sizeof(Address), data + size);

  switch(msgType) {
    case Ping:
    sendMessage(&src_addr, Pong);
    break;
    
    case Pong:
    {
      memberNode->inGroup = true;
      // if the Pong is in response to "join Ping", then I'm done - otherwise, I need to reset my swimState
      if (swimState == DirectPingSent) {
        swimState = Idle;
      }
    }
    break;
    
    default:
    MYLOG("unhandled message");
  }
  return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
  --timeToNextPing;

  list<vector<MyNode>::iterator> to_remove;
  // check for nodes with expired timeouts (doing this before SWIM and after processing received messages, so decisions are up-to-date, and generated notifications will be able to piggyback on SWIM messages)
  for (auto it = nodes.begin(); it != nodes.end(); ++it) {
    bool fail = false;
    if (it->state == Suspect) {
      // check time to remove
      assert(it->timeToRemove > 0);
      fail = (--it->timeToRemove == 0);
    } else if (it->state == Fail) {
      fail = true;
    }
    if (fail) {
      MYLOG("About to remove node " << it->addr.getAddress() << " after suspect timeout expired (generating FAIL notification)");
      to_remove.push_front(it);
      Notification fail;
      fail.addr = it->addr;
      fail.type = Fail;
      fail.stateTimestamp = par->getcurrtime();
      fail.incarnation = it->incarnation;
      insertNotification(fail);
    }
  }
  
  switch (swimState) {
    case Idle:
    // start new ping cycle if it's time
    if (timeToNextPing <= 0) {
      timeToNextPing = SWIM_PERIOD;
      // pick node to ping
      if (!nodes.empty()) {
        int member_idx = rand() % nodes.size();
        MyNode& node = nodes[member_idx];
        directPingAddr = node.addr;
        pingTimeout = TFAIL;
        swimState = DirectPingSent;
        //MYLOG(memberNode->addr.getAddress() << " chose to ping " << directPingAddr.getAddress());
        sendMessage(&directPingAddr, Ping);
      }
    }
    break;
    
    case DirectPingSent:
    // check timeout for ongoing direct ping cycle
    if (--pingTimeout <= 0) {
      MYLOG("Ping timeout expired when pinging " << directPingAddr.getAddress());
      // mark node as suspect
      auto node = getNode(directPingAddr);
      // TODO: what if node was removed since cycle started?
      node->state = Suspect;
      node->timeToRemove = TREMOVE;
      //node->stateTime = par->getcurrtime();
      Notification suspect;
      suspect.addr = directPingAddr;
      suspect.type = Suspect;
      suspect.stateTimestamp = par->getcurrtime();
      suspect.incarnation = node->incarnation;
      insertNotification(suspect);
      swimState = IndirectPingsSent;
    }
    break;
    
    case IndirectPingsSent:
    // TODO
    swimState = Idle;
  }
  
  // removing in reverse order to preserve validity of iterators
  for (auto node_it : to_remove) {
    MYLOG("Removing FAILED node " << node_it->addr.getAddress());
    assert(getRemovedNodeIncarnation(node_it->addr) == 0);
    removed_nodes.push_back(*node_it);

    int id;
    short port;
  	memcpy(&id, &node_it->addr.addr[0], sizeof(int));
  	memcpy(&port, &node_it->addr.addr[4], sizeof(short));
    for (auto mle_it = memberNode->memberList.begin(); mle_it != memberNode->memberList.end(); mle_it++) {
      if (mle_it->id == id && mle_it->port == port) {
        memberNode->memberList.erase(mle_it);
        break;
      }
    }
    
    log->logNodeRemove(&memberNode->addr, &node_it->addr);
    nodes.erase(node_it);
  }

  garbageCollectNotifications();
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
