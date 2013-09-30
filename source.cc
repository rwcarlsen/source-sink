
#include "source.h"

Source::Source(cyc::Context* ctx) : cyc::Model::Model(ctx) {
}

cyc::Model* Source::Clone() {
  Model* m = new Source(context());
  m->InitFrom(this);
  return m;
}

void Source::Deploy(cyc::Model* parent) {
  Model::Deploy(parent);
}

std::vector<cyc::Resource::Ptr> Source::RemoveResource(cyc::Transaction order) {
}

void Source::AddResource(cyc::Transaction trans,
                         std::vector<cyc::Resource::Ptr> manifest) {
}

void Source::HandleTick(int time) {
}

void Source::HandleTock(int time) {
}
