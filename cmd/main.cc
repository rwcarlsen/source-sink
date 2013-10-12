
#include "cyclus/cyclus.h"
#include "cyclus/dynamic_module.h"

#include "source.h"
#include "sink.h"
#include "builder.h"

namespace cyc = cyclus;

int main(int argc, char** argv) {
  cyc::Logger::ReportLevel() = cyc::LEV_INFO5;

  cyc::DynamicModule dyn_src("Source");
  cyc::DynamicModule dyn_snk("Sink");
  cyc::DynamicModule dyn_build("Builder");
  cyc::DynamicModule dyn_mkt("NullMarket");

  cyc::Timer ti;
  cyc::EventManager em;
  cyc::Context ctx(&ti, &em);

  int start = 0;
  int dur = 25;
  int decay = 0;
  ctx.InitTime(start, dur, decay);

  cyc::Model* m;

  m = dyn_src.ConstructInstance(&ctx);
  m->SetModelImpl("Source");
  m->SetModelType("Facility");
  m->SetName("dairy source");
  Source* src = dynamic_cast<Source*>(m);
  src->set_rate(100);
  src->set_qual("milk");
  src->set_units("kg");
  ctx.AddPrototype("dairy farm", src);

  m = dyn_snk.ConstructInstance(&ctx);
  m->SetModelImpl("Sink");
  m->SetModelType("Facility");
  m->SetName("dairy sink");
  Sink* snk = dynamic_cast<Sink*>(m);
  snk->set_rate(50);
  snk->set_cap(1000);
  snk->set_qual("milk");
  snk->set_units("kg");
  ctx.AddPrototype("milk incinerator", snk);

  m = dyn_build.ConstructInstance(&ctx);
  m->SetModelImpl("Builder");
  m->SetModelType("Facility");
  m->SetName("deployer");
  Builder* bld = dynamic_cast<Builder*>(m);
  bld->Schedule("dairy farm", 1);
  bld->Schedule("milk incinerator", 0);
  bld->Schedule("milk incinerator", 5);
  bld->Schedule("milk incinerator", 10);
  bld->Schedule("dairy farm", 15);
  bld->Schedule("milk incinerator", 20);
  bld->Deploy(bld); // has no parent

  m = dyn_mkt.ConstructInstance(&ctx);
  m->SetModelImpl("NullMarket");
  m->SetModelType("Market");
  m->SetName("milk market");
  cyc::MarketModel* mkt = dynamic_cast<cyc::MarketModel*>(m);
  mkt->SetCommodity("milk");
  mkt->Deploy(mkt);
  cyc::MarketModel::RegisterMarket(mkt);

  ti.RunSim();

  em.close();
}
