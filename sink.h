#ifndef CYCLUS_SINK_H_
#define CYCLUS_SINK_H_

#include "cyclus/cyclus.h"

namespace cyc = cyclus;

class Sink : public cyc::TimeAgent, public cyc::Communicator {
 public:
  Sink(cyc::Context* ctx);

  virtual ~Sink() { };

  virtual cyc::Model* Clone();

  virtual void Deploy(cyc::Model* parent);

  void AddResource(cyc::Transaction trans,
                   std::vector<cyc::Resource::Ptr> manifest);

  void ReceiveMessage(cyc::Message::Ptr msg) { };

  virtual void HandleTick(int time);

  virtual void HandleTock(int time) { };

  virtual void HandleDailyTasks(int time, int day) { };

  void set_rate(double rate) {
    rate_ = rate;
  };

  void set_cap(double cap) {
    inventory_.SetCapacity(cap);
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
  double rate_;
  cyc::ResourceBuff inventory_;
};
#endif

