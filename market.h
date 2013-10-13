#ifndef CYCLUS_MARKET_H_
#define CYCLUS_MARKET_H_

#include <list>

#include "cyclus/cyclus.h"

namespace cyc = cyclus;

class Market : public cyclus::MarketModel {
 public:
  Market(cyclus::Context* ctx);

  virtual ~Market() { };

  virtual cyc::Model* Clone();

  virtual void ReceiveMessage(cyc::Message::Ptr msg);

  virtual void Resolve();

 private:
  std::list<cyc::Message::Ptr> reqs_;
  std::list<cyc::Message::Ptr> offs_;
};
#endif
