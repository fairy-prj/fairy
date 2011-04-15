/**********************************************************************

  xmarshaled_queue.c -
  Copyright (C) 2007-2011 Rakuten, Inc.

**********************************************************************/

#include "ruby.h"

#include "xthread.h"
#include "fiber_mon.h"

#include "fairy.h"

extern VALUE rb_cXThreadMonitor;
extern VALUE rb_cFiberMon;

static VALUE rb_cFairyFastTempfile;

static VALUE SET_NO_IMPORT;

VALUE rb_cFairyXSizedQueue;

#define xsq(name) fairy_xsized_queue##name
#define rb_xsq(name) rb_fairy_xsized_queue##name

typedef struct rb_xsq(_struct)
{
  long chunk_size;
  long min_chunk_no;
  long queues_limit;

  char use_string_buffer_p;
  char log_mstore_p;
  
  VALUE push_queue;
  VALUE push_cv;
  
  VALUE queues;
  VALUE queues_mon;
  
  VALUE pop_queue;
  VALUE pop_cv;

  VALUE (*queue_push)(VALUE, VALUE);

  VALUE (*mon_synchronize)();
  VALUE (*cv_wait)();
  VALUE (*cv_signal)();
  VALUE (*cv_broadcast)();
  
 } xsq(_t);

#define GetFairyXSizedQueuePtr(obj, tobj) \
  TypedData_Get_Struct((obj), xsq(_t), &xsq(_data_type), (tobj))

static void
xsq(_mark)(void *ptr)
{
  xsq(_t) *sq = (xsq(_t)*)ptr;
  
  rb_gc_mark(sq->push_queue);
  rb_gc_mark(sq->push_cv);
  rb_gc_mark(sq->queues);
  rb_gc_mark(sq->queues_mon);
  rb_gc_mark(sq->pop_queue);
  rb_gc_mark(sq->pop_cv);
}

static void
xsq(_free)(void *ptr)
{
  ruby_xfree(ptr);
}

static size_t
xsq(_memsize)(const void *ptr)
{
  return ptr ? sizeof(xsq(_t)) : 0;
}

static const rb_data_type_t xsq(_data_type) = {
    "fairy_xsized_queue",
    {xsq(_mark), xsq(_free), xsq(_memsize),},
};

static VALUE
xsq(_alloc)(VALUE klass)
{
  VALUE volatile obj;
  xsq(_t) *sq;

  obj = TypedData_Make_Struct(klass, xsq(_t), &xsq(_data_type), sq);

  sq->chunk_size = 0;
  sq->min_chunk_no = 0;
  sq->queues_limit = 0;
  
  sq->push_queue = Qnil;
  sq->push_cv = Qnil;
  
  sq->queues = Qnil;
  sq->queues_mon = Qnil;

  sq->pop_queue = Qnil;
  sq->pop_cv = Qnil;

  sq->queue_push = NULL;

  sq->mon_synchronize = NULL;
  sq->cv_wait = NULL;
  sq->cv_broadcast = NULL;
  
  return obj;
}

static VALUE rb_xsq(_monitor_cond_wait)(VALUE);
static VALUE rb_xsq(_genmon_synchronize)(VALUE, VALUE (*)(VALUE), VALUE);
static VALUE rb_xsq(_gencond_wait)(VALUE);
static VALUE rb_xsq(_gencond_broadcast)(VALUE);
static VALUE rb_xsq(_gencond_signal)(VALUE);

static VALUE rb_xsq(_empty_push)(VALUE, VALUE);
static VALUE rb_xsq(_str_push)(VALUE, VALUE);
static VALUE rb_xsq(_obj_push)(VALUE, VALUE);

static VALUE
rb_xsq(_initialize)(VALUE self, VALUE policy, VALUE queues_mon, VALUE pop_cv)
{
  VALUE sz;
  VALUE flag;
  VALUE dir;
  xsq(_t) *sq;
  
  GetFairyXSizedQueuePtr(self, sq);
  
  sz = rb_fairy_conf("XSIZED_QUEUE_CHUNK_SIZE", policy, "chunk_size");
  sq->chunk_size = NUM2LONG(sz);

  sz = rb_fairy_conf("XSIZED_QUEUE_QUEUES_LIMIT",
		     policy, "queues_limit");
  sq->queues_limit = NUM2LONG(sz);

  flag = rb_fairy_conf("XSIZED_QUEUE_USE_STRING_BUFFER",
		       policy, "use_string_buffer");
  sq->use_string_buffer_p = RTEST(flag);

  flag = rb_fairy_conf("XSIZED_QUEUE_LOG_MSTORE",
		       policy, "log_mstore");
  sq->log_mstore_p = RTEST(flag);

  sq->queues = rb_xthread_fifo_new();
  if (NIL_P(queues_mon)) {
    queues_mon = rb_xthread_monitor_new();
  }
  sq->queues_mon = queues_mon;
  
  sq->push_queue = Qnil;
  sq->push_cv = rb_funcall(queues_mon, rb_intern("new_cond"), 0);

  if (NIL_P(pop_cv)) {
    pop_cv = rb_funcall(queues_mon, rb_intern("new_cond"), 0);
  }
  sq->pop_cv = pop_cv;
  sq->pop_queue = Qnil;

  sq->queue_push = rb_xsq(_empty_push);

  if (CLASS_OF(sq->queues_mon) == rb_cXThreadMonitor) {
    sq->mon_synchronize = rb_xthread_monitor_synchronize;
    sq->cv_wait = rb_xsq(_monitor_cond_wait);
    sq->cv_broadcast = rb_xthread_monitor_cond_broadcast;
    sq->cv_signal = rb_xthread_monitor_cond_signal;
  }
  else if (CLASS_OF(sq->queues_mon) == rb_cFiberMonMonitor) {
    sq->mon_synchronize = rb_fibermon_monitor_synchronize;
    sq->cv_wait = rb_fibermon_cond_wait;
    sq->cv_signal = rb_fibermon_cond_signal;
    sq->cv_broadcast = rb_fibermon_cond_broadcast;
  }
  else {
    sq->mon_synchronize = rb_xsq(_genmon_synchronize);
    sq->cv_wait = rb_xsq(_gencond_wait);
    sq->cv_signal = rb_xsq(_gencond_signal);
    sq->cv_broadcast = rb_xsq(_gencond_broadcast);
  }

  return self;
}

static VALUE
rb_xsq(_monitor_cond_wait)(VALUE arg)
{
  return rb_xthread_monitor_cond_wait(arg, Qnil);
}

static VALUE
rb_xsq(_genmon_synchronize)(VALUE arg1, VALUE (*arg2)(VALUE), VALUE arg3)
{
  static ID id_synchronize;
  if (!id_synchronize) id_synchronize = rb_intern("synchronize");

  return rb_funcall(arg1, id_synchronize, 2, arg2, arg3);
}

static VALUE
rb_xsq(_gencond_wait)(VALUE arg)
{
  static ID id_wait;
  if (!id_wait) id_wait = rb_intern("wait");

  return rb_funcall(arg, id_wait, 1, Qnil);
}

static VALUE
rb_xsq(_gencond_signal)(VALUE arg1)
{
  static ID id_signal;
  if (!id_signal) id_signal = rb_intern("signal");

  return rb_funcall(arg1, id_signal, 0);
}

static VALUE
rb_xsq(_gencond_broadcast)(VALUE arg1)
{
  static ID id_broadcast;
  if (!id_broadcast) id_broadcast = rb_intern("broadcast");

  return rb_funcall(arg1, id_broadcast, 0);
}


static VALUE
xsq(_initialize)(int argc, VALUE *argv, VALUE self)
{
  VALUE policy;
  VALUE queues_mon;
  VALUE pop_cv;
  
  rb_scan_args(argc, argv, "12", &policy, &queues_mon, &pop_cv);
  return rb_xsq(_initialize)(self, policy, queues_mon, pop_cv);
}

VALUE
rb_xsq(_new)(VALUE policy, VALUE queues_mon, VALUE pop_cv)
{
  VALUE self;

  self = xsq(_alloc)(rb_cFairyXSizedQueue);
  rb_xsq(_initialize)(self, policy, queues_mon, pop_cv);
  return self;
}

static VALUE rb_xsq(_queues_push)(VALUE, VALUE);
static VALUE rb_xsq(_broadcast)(VALUE);
static VALUE rb_xsq(_store)(VALUE, VALUE);
static VALUE rb_xsq(_restore)(VALUE, VALUE);

VALUE
rb_xsq(_push)(VALUE self, VALUE e)
{
  xsq(_t) *sq;

  GetFairyXSizedQueuePtr(self, sq);
  return sq->queue_push(self, e);
}

static VALUE
rb_xsq(_empty_push)(VALUE self, VALUE e)
{
  xsq(_t) *sq;
  GetFairyXSizedQueuePtr(self, sq);

  if (EOS_P(e)) {
    rb_xthread_fifo_push(sq->queues, e);
    return self;
  }

  if (sq->use_string_buffer_p && CLASS_OF(e) == rb_cString) {
    sq->push_queue = rb_fairy_string_buffer_new();
    sq->queue_push = rb_xsq(_str_push);
  }
  else {
    sq->push_queue = rb_ary_new2(sq->min_chunk_no);
    sq->queue_push = rb_xsq(_obj_push);
  }
  return sq->queue_push(self, e);
}

static VALUE
rb_xsq(_obj_push)(VALUE self, VALUE e)
{
  xsq(_t) *sq;
  GetFairyXSizedQueuePtr(self, sq);

  if (EOS_P(e)) {
    rb_xsq(_queues_push)(self, sq->push_queue);
    sq->push_queue = Qnil;
    rb_xsq(_queues_push)(self, e);
    return self;
  }

  if (sq->use_string_buffer_p && CLASS_OF(e) == rb_cString) {
    rb_xsq(_queues_push)(self, sq->push_queue);
    sq->push_queue = Qnil;
    sq->queue_push = rb_xsq(_empty_push);
    return sq->queue_push(self, e);
  }

  rb_ary_push(sq->push_queue, e);
  if (RARRAY_LEN(sq->push_queue) >= sq->chunk_size || e == SET_NO_IMPORT) {
    rb_xsq(_queues_push)(self, sq->push_queue);
    sq->push_queue = Qnil;
    sq->queue_push = rb_xsq(_empty_push);
  }
  return self;
}

static VALUE
rb_xsq(_str_push)(VALUE self, VALUE e)
{
  xsq(_t) *sq;
  
  GetFairyXSizedQueuePtr(self, sq);
  if (EOS_P(e)) {
    rb_xsq(_queues_push)(self, sq->push_queue);
    rb_xsq(_queues_push)(self, e);
    return self;
  }

  if (CLASS_OF(e) != rb_cString) {
    rb_xsq(_queues_push)(self, sq->push_queue);
    sq->push_queue = Qnil;
    sq->queue_push = rb_xsq(_empty_push);
  }
  rb_fairy_string_buffer_push(sq->push_queue, e);
  if (NUM2LONG(rb_fairy_string_buffer_size(sq->push_queue)) >= sq->chunk_size) {
    rb_xsq(_queues_push)(self, sq->push_queue);
    sq->push_queue = Qnil;
    sq->queue_push = rb_xsq(_empty_push);
  }
  return self;
}

static VALUE
rb_xsq(_push_raw)(VALUE self, VALUE raw)
{
  xsq(_t) *sq;
  
  GetFairyXSizedQueuePtr(self, sq);
  
  if (!NIL_P(sq->push_queue)) {
    rb_xsq(_queues_push)(self, sq->push_queue);
    sq->push_queue = Qnil;
  }
  rb_xsq(_queues_push)(self, raw);

  return self;
}

struct rb_xsq(_queues_push_arg) 
{
  VALUE self;
  VALUE buf;
};

static VALUE rb_xsq(_push_sync)(struct rb_xsq(_queues_push_arg) *);

static VALUE
rb_xsq(_queues_push)(VALUE self, VALUE buf)
{
  xsq(_t) *sq;
  struct rb_xsq(_queues_push_arg) arg;

  GetFairyXSizedQueuePtr(self, sq);

  arg.self = self;
  arg.buf = buf;
  
  sq->mon_synchronize(sq->queues_mon,
		      rb_xsq(_push_sync), &arg);
}

static VALUE
rb_xsq(_push_sync)(struct rb_xsq(_queues_push_arg) *arg)
{
  xsq(_t) *sq;
  GetFairyXSizedQueuePtr(arg->self, sq);

  if (NUM2LONG(rb_xthread_fifo_length(sq->queues)) > sq->queues_limit) {
    while(NUM2LONG(rb_xthread_fifo_length(sq->queues)) > sq->queues_limit) {
      sq->cv_wait(sq->push_cv);
    }
  }

  rb_xthread_fifo_push(sq->queues, arg->buf);
  sq->cv_broadcast(sq->pop_cv);
  return arg->self;
}

struct rb_xsq(_pop_arg)
{
  VALUE self;
  VALUE buf;
};

static VALUE rb_xsq(_pop_wait)(struct rb_xsq(_pop_arg) *arg);

VALUE
rb_xsq(_pop)(VALUE self)
{
  xsq(_t) *sq;
  VALUE buf;
  struct rb_xsq(_pop_arg) arg;
  GetFairyXSizedQueuePtr(self, sq);

  while (NIL_P(sq->pop_queue) || RARRAY_LEN(sq->pop_queue) == 0) {
    {
      arg.self = self;
      arg.buf = Qnil;
      sq->mon_synchronize(sq->queues_mon,
			rb_xsq(_pop_wait), &arg);
      buf = arg.buf;
    }

    if (EOS_P(buf)) {
      sq->pop_queue = rb_ary_new3(1, buf);
    }
    else {
      if (CLASS_OF(buf) == rb_cString) {
	buf = rb_marshal_load(buf);
      }
      if (CLASS_OF(buf) == rb_cFairyStringBuffer) {
	sq->pop_queue = rb_fairy_string_buffer_to_a(buf);
      }
      else {
	sq->pop_queue = buf;
      }
    }
  }
  return rb_ary_shift(sq->pop_queue);
}

static VALUE
rb_xsq(_pop_wait)(struct rb_xsq(_pop_arg) *arg)
{
  VALUE self = arg->self;
  xsq(_t) *sq;
  VALUE buf = Qnil;

  GetFairyXSizedQueuePtr(self, sq);
  
  buf = rb_xthread_fifo_pop(sq->queues);
  while (NIL_P(buf)) {
    sq->cv_wait(sq->pop_cv);
    buf = rb_xthread_fifo_pop(sq->queues);
  }
  sq->cv_signal(sq->push_cv);
  arg->buf = buf;
  return arg->self;
}

VALUE
rb_xsq(_pop_raw)(VALUE self)
{
  xsq(_t) *sq;
  VALUE pop_raw = Qnil;
  VALUE buf;
  struct rb_xsq(_pop_arg) arg;
  
  GetFairyXSizedQueuePtr(self, sq);
  {
    arg.self = self;
    arg.buf = Qnil;
    sq->mon_synchronize(sq->queues_mon,
			rb_xsq(_pop_wait), &arg);
    buf = arg.buf;
  }
  return buf;
}

VALUE
rb_xsq(_inspect)(VALUE self)
{
  VALUE str;
  xsq(_t) *sq;

  GetFairyXSizedQueuePtr(self, sq);

  str = rb_sprintf("<%s:%p chunk_size=%d, min_chunk_no=%d, queues_limit=%d use_string_bffer_p=%d>",
		   rb_obj_classname(self),
		   (void*)self,
		   sq->chunk_size,
		   sq->min_chunk_no,
		   sq->queues_limit,
		   sq->use_string_buffer_p);
  return str;
}


void
Init_xsized_queue()
{
  VALUE xsq;
  
  rb_cFairyFastTempfile = rb_const_get(rb_mFairy, rb_intern("FastTempfile"));


  SET_NO_IMPORT = rb_const_get(rb_cFairyImport, rb_intern("SET_NO_IMPORT"));

  rb_cFairyXSizedQueue  = rb_define_class_under(rb_mFairy, "XSizedQueue", rb_cObject);

  xsq = rb_cFairyXSizedQueue;
  
  rb_define_alloc_func(xsq, xsq(_alloc));
  rb_define_method(xsq, "initialize", xsq(_initialize), -1);
  rb_define_method(xsq, "push", rb_xsq(_push), 1);
  rb_define_method(xsq, "push_raw", rb_xsq(_push_raw), 1);
  rb_define_method(xsq, "pop", rb_xsq(_pop), 0);
  rb_define_method(xsq, "pop_raw", rb_xsq(_pop_raw), 0);
  rb_define_method(xsq, "inspect", rb_xsq(_inspect), 0);
}

