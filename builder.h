
#ifndef CYCLUS_BUILDER_H_
#define CYCLUS_BUILDER_H_

#include "cyclus/cyclus.h"

namespace cyc = cyclus;

class Builder : public cyc::TimeAgent {

 public:
  /// a list of prototypes to build on a given timestep
  typedef std::vector<std::string> Queue;

  Builder(cyc::Context* ctx);

  virtual ~Builder() { };

  virtual cyc::Model* Clone();

  virtual void HandleTick(int time);

  virtual void HandleTock(int time);

  virtual void HandleDailyTasks(int time, int day) { };

 private:

  std::map<int, Queue> schedule_;

};
#endif

