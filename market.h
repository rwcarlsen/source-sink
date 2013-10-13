#ifndef CYCLUS_MARKET_H_
#define CYCLUS_MARKET_H_

#include <list>

#include "cyclus.h"

class Market : public cyclus::MarketModel {
 public:
  Market(cyclus::Context* ctx);

  virtual ~Market();

  virtual cyclus::Model* Clone() {
    Market* m = new Market(*this);
    m->InitFrom(this);
    return m;
  }

  virtual void ReceiveMessage(cyclus::Message::Ptr msg);

  virtual void Resolve();

 private:
  std::list<cyc::Message::Ptr> reqs_;
  std::list<cyc::Message::Ptr> offs_;

};
#endif
