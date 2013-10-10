#ifndef CYCLUS_SOURCE_H_
#define CYCLUS_SOURCE_H_

#include "cyclus/cyclus.h"

namespace cyc = cyclus;

class Source : public cyc::TimeAgent {
 public:
  Source(cyc::Context* ctx);

  virtual ~Source() { };

  virtual cyc::Model* Clone();

  virtual std::vector<cyc::Resource::Ptr> RemoveResource(cyc::Transaction order);

  virtual void HandleTick(int time);

  virtual void HandleTock(int time);

  virtual void HandleDailyTasks(int time, int day) { };

 private:

  cyc::ResourceBuff inventory_;
};
#endif

