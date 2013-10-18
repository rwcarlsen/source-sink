
#include "sink.h"
#include "boost/pointer_cast.hpp"

typedef cyc::GenericResource Gres;

Sink::Sink(cyc::Context* ctx) : cyc::TimeAgent::TimeAgent(ctx) {
  inventory2_.set_capacity(100000000);
}

cyc::Model* Sink::Clone() {
  Sink* m = new Sink(*this);
  m->InitFrom(this);
  return m;
}

void Sink::Deploy(cyc::Model* parent) {
  Model::Deploy(parent);
  context()->RegisterTicker(this);
}

void Sink::AddResource(cyc::Transaction trans,
                               std::vector<cyc::Resource::Ptr> manifest) {
  Gres::Ptr r = cyc::ResCast<Gres>(manifest[0]);
  for (int i = 1; i < manifest.size(); ++i) {
    r->Absorb(cyc::ResCast<Gres>(manifest[i]));
  }
  if (inventory_.count() > 0) {
    r->Absorb(inventory_.Pop<Gres>());
  }
  inventory_.Push(r);
}

void Sink::HandleTick(int time) {
  // update inventory
  if (inventory_.space() <= 0) {
    return;
  }

  // build and send request
  cyc::GenericResource::Ptr r = cyc::GenericResource::CreateUntracked(
                                  std::min(inventory_.space(), rate_),
                                  qual_,
                                  units_
                                );
  cyc::Transaction trans(this, cyc::REQUEST);
  trans.SetCommod(qual_);
  trans.SetResource(r);
  cyc::MarketModel* market = cyc::MarketModel::MarketForCommod(qual_);
  cyc::Communicator* recipient = dynamic_cast<cyc::Communicator*>(market);
  cyc::Message::Ptr msg(new cyc::Message(this, recipient, trans));
  msg->SendOn();

  if (inventory_.count() > 0) {
    cyc::Resource::Ptr rr = inventory_.Pop();
    inventory2_.Push(rr->ExtractRes(rr->quantity() / 2));
    inventory_.Push(rr);
  }
}

extern "C" cyc::Model* ConstructSink(cyc::Context* ctx) {
  return new Sink(ctx);
}
