
#include "builder.h"

Builder::Builder(cyc::Context* ctx) : cyc::TimeAgent::TimeAgent(ctx) {}

cyc::Model* Builder::Clone() {
  Builder* m = new Builder(context());
  m->InitFrom(this);
  return m;
}

void Builder::HandleTick(int time) {
}

void Builder::HandleTock(int time) {
  Queue protos = schedule_[time];
  for (int i = 0; i < protos.size(); ++i) {
    Model* m = context()->CreateModel<Model>(protos[i]);
    m->Deploy(this);
  }
}
