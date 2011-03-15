/**********************************************************************

  xthread.h -

  Copyright (C) 2011 Keiju Ishitsuka
  Copyright (C) 2011 Penta Advanced Laboratories, Inc.

**********************************************************************/


#define XTHREAD_VERSION "0.1.2"

RUBY_EXTERN VALUE rb_mXThread;
RUBY_EXTERN VALUE rb_cXThreadFifo;
RUBY_EXTERN VALUE rb_cXThreadConditionVariable;
RUBY_EXTERN VALUE rb_cXThreadQueue;
RUBY_EXTERN VALUE rb_cXThreadSizedQueue;

RUBY_EXTERN VALUE rb_xthread_fifo_new(void);
RUBY_EXTERN VALUE rb_xthread_fifo_empty_p(VALUE);
RUBY_EXTERN VALUE rb_xthread_fifo_push(VALUE, VALUE);
RUBY_EXTERN VALUE rb_xthread_fifo_pop(VALUE);
RUBY_EXTERN VALUE rb_xthread_fifo_clear(VALUE);
RUBY_EXTERN VALUE rb_xthread_fifo_length(VALUE);

RUBY_EXTERN VALUE rb_xthread_cond_new(void);
RUBY_EXTERN VALUE rb_xthread_cond_signal(VALUE);
RUBY_EXTERN VALUE rb_xthread_cond_broadcast(VALUE);
RUBY_EXTERN VALUE rb_xthread_cond_wait(VALUE, VALUE, VALUE);

RUBY_EXTERN VALUE rb_xthread_monitor_new(void);
RUBY_EXTERN VALUE rb_xthread_monitor_try_enter(VALUE);
RUBY_EXTERN VALUE rb_xthread_monitor_enter(VALUE);
RUBY_EXTERN VALUE rb_xthread_monitor_exit(VALUE);
RUBY_EXTERN VALUE rb_xthread_monitor_synchronize(VALUE, VALUE (*)(VALUE), VALUE);
RUBY_EXTERN VALUE rb_xthread_monitor_new_cond(VALUE);

RUBY_EXTERN VALUE rb_xthread_monitor_cond_new(VALUE);
RUBY_EXTERN VALUE rb_xthread_monitor_cond_wait(VALUE, VALUE);
RUBY_EXTERN VALUE rb_xthread_monitor_cond_wait_while(VALUE);
RUBY_EXTERN VALUE rb_xthread_monitor_cond_wait_until(VALUE);
RUBY_EXTERN VALUE rb_xthread_monitor_cond_signal(VALUE);
RUBY_EXTERN VALUE rb_xthread_monitor_cond_broadcast(VALUE self);




