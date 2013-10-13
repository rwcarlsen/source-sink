
#include "market.h"

Market::Market(cyclus::Context* ctx) : cyclus::MarketModel(ctx) {}

void Market::ReceiveMessage(cyclus::Message::Ptr msg) {
  if (msg->trans().IsOffer()) {
    offs_.push_back(msg);
  } else {
    reqs_.push_back(msg);
  }
}

void Market::Resolve() {
  cyc::Message::Ptr curr_req = reqs_.front();
  cyc::Message::Ptr curr_off = offs_.front();
  reqs_.pop_front();
  offs_.pop_front();
  double matched = 0
  while (reqs_.size() > 0 && offs_.size() > 0) {
    double req_qty = curr_req->trans().resource().quantity();
    double off_qty = curr_off->trans().resource().quantity();
    if ((req_qty - matched) > off_qty) {
      curr_off->trans().MatchWith(curr_req->trans());
      curr_off->SetDir(cyclus::DOWN_MSG);
      curr_off->SendOn();

      matched += off_qty;
      curr_off = offs_.front();
      offs_.pop_front();
    } else if ((req_qty - matched) < off_qty) {
      cyc::Message::Ptr leftover = curr_off->Clone();
      cyc::Resource::Ptr match = leftover->trans().resource()->ExtractRes(req_qty - matched);
      curr_off->SetResource(match);
      curr_off->trans().MatchWith(curr_req->trans());
      curr_off->SetDir(cyclus::DOWN_MSG);
      curr_off->SendOn();

      matched = 0;
      curr_off = leftover;
      curr_req = reqs_.front();
      reqs_.pop_front();
    } else {
      curr_off->trans().MatchWith(curr_req->trans());
      curr_off->SetDir(cyclus::DOWN_MSG);
      curr_off->SendOn();

      matched = 0
      curr_req = reqs_.front();
      reqs_.pop_front();
      curr_off = offs_.front();
      offs_.pop_front();
    }
  }
}

extern "C" cyclus::Model* ConstructMarket(cyclus::Context* ctx) {
  return new Market(ctx);
}
