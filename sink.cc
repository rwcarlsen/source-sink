
#include "sink.h"
#include "boost/pointer_cast.hpp"

Sink::Sink(cyc::Context* ctx) : cyc::TimeAgent::TimeAgent(ctx) {
  inventory2_.SetCapacity(100000000);
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
  cyc::GenericResource::Ptr r = boost::dynamic_pointer_cast<cyc::GenericResource>(manifest[0]);
  for (int i = 1; i < manifest.size(); ++i) {
    r->Absorb(boost::dynamic_pointer_cast<cyc::GenericResource>(manifest[i]));
  }
  if (inventory_.count() > 0) {
    r->Absorb(boost::dynamic_pointer_cast<cyc::GenericResource>(inventory_.PopOne()));
  }
  inventory_.PushOne(boost::dynamic_pointer_cast<cyc::Resource>(r));

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
    cyc::Resource::Ptr rr = inventory_.PopOne();
    inventory2_.PushOne(rr->ExtractRes(rr->quantity() / 2));
    inventory_.PushOne(rr);
  }
}

extern "C" cyc::Model* ConstructSink(cyc::Context* ctx) {
  return new Sink(ctx);
}
