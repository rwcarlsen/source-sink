#ifndef CYCLUS_SOURCE_H_
#define CYCLUS_SOURCE_H_

#include "cyclus/cyclus.h"

namespace cyc = cyclus;

class Source : public cyc::TimeAgent, public cyc::Communicator {
 public:
  Source(cyc::Context* ctx);

  virtual ~Source() { };

  virtual cyc::Model* Clone();

  virtual void Deploy(cyc::Model* parent);

  virtual std::vector<cyc::Resource::Ptr> RemoveResource(cyc::Transaction order);

  void ReceiveMessage(cyc::Message::Ptr msg);

  virtual void HandleTick(int time);

  virtual void HandleTock(int time) { };

  virtual void HandleDailyTasks(int time, int day) { };

  void set_rate(double rate) {
    inventory_.set_capacity(rate);
  };

  void set_qual(std::string qual) {
    qual_ = qual;
  };

  void set_units(std::string units) {
    units_ = units;
  };

 private:
  std::string qual_;
  std::string units_;
  cyc::ResourceBuff inventory_;
};
#endif

