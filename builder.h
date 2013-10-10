
#ifndef CYCLUS_BUILDER_H_
#define CYCLUS_BUILDER_H_

#include "cyclus/cyclus.h"

namespace cyc = cyclus;

class Builder : public cyc::TimeAgent, public cyc::Communicator {

 public:
  /// a list of prototypes to build on a given timestep
  typedef std::vector<std::string> Queue;

  Builder(cyc::Context* ctx);

  virtual ~Builder() { };

  virtual cyc::Model* Clone();

  void ReceiveMessage(cyc::Message::Ptr msg) {
    msg->SendOn();
  };

  virtual void HandleTick(int time) { };

  virtual void HandleTock(int time);

  virtual void HandleDailyTasks(int time, int day) { };

  void Schedule(std::string prototype, int build_time);

 private:
  std::map<int, Queue> schedule_;
};
#endif

