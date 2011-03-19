/**********************************************************************

  fiber-mon.h -
   	Copyright (C) 2010-2011 Keiju ISHITSUKA
				(Penta Advanced Labrabries, Co.,Ltd)

**********************************************************************/

RUBY_EXTERN VALUE rb_cFiberMon;
RUBY_EXTERN VALUE rb_cFiberMonMonitor;
RUBY_EXTERN VALUE rb_cFiberMonConditionVariable;

RUBY_EXTERN VALUE rb_fibermon_new(void);
RUBY_EXTERN VALUE rb_fibermon_current(VALUE);
RUBY_EXTERN VALUE rb_fibermon_entry_fiber(VALUE, VALUE);
RUBY_EXTERN VALUE rb_fibermon_yield(VALUE);
RUBY_EXTERN VALUE rb_fibermon_new_mon(VALUE);

RUBY_EXTERN VALUE rb_fibermon_monitor_new(VALUE);
RUBY_EXTERN VALUE rb_fibermon_monitor_valid_owner_p(VALUE);
RUBY_EXTERN VALUE rb_fibermon_monitor_try_enter(VALUE);
RUBY_EXTERN VALUE rb_fibermon_monitor_enter(VALUE);
RUBY_EXTERN VALUE rb_fibermon_monitor_exit(VALUE);
RUBY_EXTERN VALUE rb_fibermon_monitor_synchronize(VALUE, VALUE (*)(VALUE), VALUE);
RUBY_EXTERN VALUE rb_fibermon_monitor_new_cond(VALUE);

RUBY_EXTERN VALUE rb_fibermon_cond_new(VALUE);
RUBY_EXTERN VALUE rb_fibermon_cond_signal(VALUE);
RUBY_EXTERN VALUE rb_fibermon_cond_broadcast(VALUE);
RUBY_EXTERN VALUE rb_fibermon_cond_wait(VALUE);
RUBY_EXTERN VALUE rb_fibermon_cond_wait_until(VALUE);
RUBY_EXTERN VALUE rb_fibermon_cond_wait_while(VALUE);
