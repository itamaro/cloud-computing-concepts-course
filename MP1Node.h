/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include <list>

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"

/**
 * Macros
 */
#define TREMOVE 10
#define TFAIL 2
#define SWIM_PERIOD 10

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MsgTypes {
  Ping,
  Pong,

    // JOINREQ,
    // JOINREP,
    // PING,
    // PONG,
    // IPING,
    // IPONG,
  DUMMYLASTMSGTYPE
};

enum NodeStates {
  Alive,
  Leave,
  Suspect,
  Fail
};

struct Notification {
  Address addr;
  NodeStates type;
  long stateTimestamp;
  unsigned incarnation;
  
  size_t serialize(char * buffer) const {
    memcpy(buffer, &addr, sizeof(addr));
    memcpy(buffer + sizeof(addr), &type, sizeof(type));
    memcpy(buffer + sizeof(addr) + sizeof(type), &stateTimestamp, sizeof(stateTimestamp));
    memcpy(buffer + sizeof(addr) + sizeof(type) + sizeof(stateTimestamp), &incarnation, sizeof(incarnation));
    return size();
  }
  
  size_t parseFromBuffer(char * buffer) {
    memcpy(&addr, buffer, sizeof(addr));
    memcpy(&type, buffer + sizeof(addr), sizeof(type));
    memcpy(&stateTimestamp, buffer + sizeof(addr) + sizeof(type), sizeof(stateTimestamp));
    memcpy(&incarnation, buffer + sizeof(addr) + sizeof(type) + sizeof(stateTimestamp), sizeof(incarnation));
    return size();
  }
  
  size_t size() const {
    return sizeof(addr) + sizeof(type) +  sizeof(stateTimestamp) + sizeof(incarnation);
  }
  
  string typeString() const {
    switch (type) {
      case Alive: return "Alive";
      case Leave: return "Leave";
      case Suspect: return "Suspect";
      case Fail: return "Fail";
    }
    return "UNKNOWN";
  }
  
  string toString() {
    return "<" + addr.getAddress() + "," + typeString() + ",@" + to_string(stateTimestamp) + "," + to_string(incarnation) + ">";
  }
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
	enum MsgTypes msgType;
} MessageHdr;

enum SwimStates {
  Idle,
  DirectPingSent,
  IndirectPingsSent
};

class MyNode {
 public:
	Address addr;
  NodeStates state;
  long stateTime;
  unsigned incarnation;
  long timeToRemove;
  
  MyNode(const Address& addr, NodeStates state, long stateTime, unsigned incarnation)
    : addr(addr),
      state(state),
      stateTime(stateTime),
      incarnation(incarnation),
      timeToRemove(-1) { /* nop */ }
};

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
 private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
  vector<MyNode> nodes;
  vector<MyNode> removed_nodes;
  list<Notification> notifications;
	char NULLADDR[6];
  int timeToNextPing;
  SwimStates swimState;
  Address directPingAddr;
  int pingTimeout;
  
  int sendMessage(Address* dest_addr, MsgTypes msg_type);
  void addNodeFromNotification(Notification notification);
  bool processNotification(Notification notification);
  void processNotifications(char * data_begin, char * data_end);
  vector<MyNode>::iterator getNode(Address addr);
  unsigned getRemovedNodeIncarnation(Address addr);
  void insertNotification(const Notification& notification);
  void garbageCollectNotifications();

public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
	void initMemberListTable(Member *memberNode);
	void printAddress(Address *addr);
	virtual ~MP1Node();
};

#endif /* _MP1NODE_H_ */
