
#include "source.h"

Source::Source(cyc::Context* ctx) : cyc::TimeAgent::TimeAgent(ctx) { }

cyc::Model* Source::Clone() {
  Source* m = new Source(*this);
  m->InitFrom(this);
  return m;
}

void Source::Deploy(cyc::Model* parent) {
  Model::Deploy(parent);
  context()->RegisterTicker(this);
}

std::vector<cyc::Resource::Ptr> Source::RemoveResource(cyc::Transaction
                                                       order) {
  return inventory_.PopQty(order.resource()->quantity());
}

void Source::HandleTick(int time) {
  // update inventory
  if (inventory_.space() > 0) {
    cyc::GenericResource::Ptr r = cyc::GenericResource::Create(this,
                                                               inventory_.space(), qual_, units_);
    inventory_.Push(r);
  }

  // build and send offer
  cyc::GenericResource::Ptr r = cyc::GenericResource::CreateUntracked(
                                  inventory_.quantity(),
                                  qual_,
                                  units_
                                );
  cyc::Transaction trans(this, cyc::OFFER);
  trans.SetCommod(qual_);
  trans.SetResource(r);
  cyc::MarketModel* market = cyc::MarketModel::MarketForCommod(qual_);
  cyc::Communicator* recipient = dynamic_cast<cyc::Communicator*>(market);
  cyc::Message::Ptr msg(new cyc::Message(this, recipient, trans));
  msg->SendOn();
}

void Source::ReceiveMessage(cyc::Message::Ptr msg) {
  if (msg->trans().supplier() != this) {
    throw cyc::Error("SourceFacility is not the supplier of this msg.");
  }
  msg->trans().ApproveTransfer();
}

extern "C" cyc::Model* ConstructSource(cyc::Context* ctx) {
  return new Source(ctx);
}
