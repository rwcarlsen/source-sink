
#include "builder.h"

Builder::Builder(cyc::Context* ctx) : cyc::TimeAgent::TimeAgent(ctx) { }

cyc::Model* Builder::Clone() {
  Builder* m = new Builder(context());
  m->InitFrom(this);
  return m;
}

void Builder::Deploy(cyc::Model* parent) {
  Model::Deploy(parent);
  context()->RegisterTicker(this);
}

void Builder::HandleTock(int time) {
  Queue protos = schedule_[time];
  for (int i = 0; i < protos.size(); ++i) {
    Model* m = context()->CreateModel<Model>(protos[i]);
    m->Deploy(this);
  }
}

void Builder::Schedule(std::string prototype, int build_time) {
  schedule_[build_time].push_back(prototype);
}

extern "C" cyc::Model* ConstructBuilder(cyc::Context* ctx) {
  return new Builder(ctx);
}
