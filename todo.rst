
* transactions must be recorded AFTER resources are taken from supplier and
  BEFORE resources are sent to the receiver.  This allows correct determination
  of which resource spliiting, combining, etc. operations happen before vs
  after the transfer

* libcyclus.so isn't being linked to: boost_system properly

* Need to move default module install location to a cyclus-specific dir rather than just generic "lib/Models"

* Env::FindModuleLib should probably search the CYCLUS_MODULE_PATH dirs before the cyclus install dir

* Env::FindModuleLib should not search in Env::GetInstallPath (i.e. /usr) for modules - it should search in the specific module lib dir (i.e. lib/cyclusmodules).

* timer tick iteration should be robust against new registrations mid tick/tock.  Probably should push newly registered listeners to a separate list until the current tick/tock are finished.

* when the Context::InitTime is called, the timer resets, and any
  previously registered listeners are wiped - this behavior probably needs
  to be changed.
