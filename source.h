#ifndef CYCLUS_SOURCE_H_
#define CYCLUS_SOURCE_H_

#include "cyclus/model.h"
#include "cyclus/time_agent.h"
#include "cyclus/resource.h"
#include "cyclus/transaction.h"
#include "cyclus/message.h"
#include "cyclus/context.h"

namespace cyc = cyclus;

class Source : public cyc::TimeAgent {
 public:
  Source(cyc::Context* ctx);

  virtual ~Source() { };

  virtual cyc::Model* Clone();

  virtual void Deploy(cyc::Model* parent);

  virtual std::vector<cyc::Resource::Ptr> RemoveResource(cyc::Transaction order);

  virtual void AddResource(cyc::Transaction trans,
                           std::vector<cyc::Resource::Ptr> manifest);

  virtual void HandleTick(int time);

  virtual void HandleTock(int time);

  virtual void HandleDailyTasks(int time, int day) { };

 private:

};
#endif

