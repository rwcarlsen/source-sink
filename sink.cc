
#include "sink.h"

Sink::Sink(cyc::Context* ctx) : cyc::TimeAgent::TimeAgent(ctx) { }

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
  inventory_.PushAll(manifest);
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
}

extern "C" cyc::Model* ConstructSink(cyc::Context* ctx) {
  return new Sink(ctx);
}
