
#include "source.h"

Source::Source(cyc::Context* ctx) : cyc::TimeAgent::TimeAgent(ctx) {}

cyc::Model* Source::Clone() {
  Source* m = new Source(context());
  m->InitFrom(this);
  return m;
}

void Source::Deploy(cyc::Model* parent) {
  Model::Deploy(parent);
}

std::vector<cyc::Resource::Ptr> Source::RemoveResource(cyc::Transaction order) {
}

void Source::HandleTick(int time) {
}

void Source::HandleTock(int time) {
}
