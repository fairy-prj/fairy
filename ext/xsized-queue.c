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
#define rb_xsq(name) rb_##xsq(name)

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
  long queues_cache_no;

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
  xsq(_t) *mq = (xsq(_t)*)ptr;
  
  rb_gc_mark(mq->push_queue);
  rb_gc_mark(mq->push_cv);
  rb_gc_mark(mq->queues);
  rb_gc_mark(mq->queues_mon);
  rb_gc_mark(mq->pop_queue);
  rb_gc_mark(mq->pop_cv);
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
  xsq(_t) *mq;

  obj = TypedData_Make_Struct(klass, xsq(_t), &xsq(_data_type), mq);

  mq->chunk_size = 0;
  mq->min_chunk_no = 0;
  mq->queues_limit = 0;
  
  mq->push_queue = Qnil;
  mq->push_cv = Qnil;
  
  mq->queues = Qnil;
  mq->queues_mon = Qnil;
  mq->queues_cache_no = 0;

  mq->pop_queue = Qnil;
  mq->pop_cv = Qnil;

  mq->queue_push = NULL;

  mq->mon_synchronize = NULL;
  mq->cv_wait = NULL;
  mq->cv_broadcast = NULL;
  
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
  xsq(_t) *mq;
  
  GetFairyXSizedQueuePtr(self, mq);
  
  sz = rb_fairy_conf("XSIZED_QUEUE_CHUNK_SIZE", policy, "chunk_size");
  mq->chunk_size = NUM2LONG(sz);

  sz = rb_fairy_conf("XSIZED_QUEUE_QUEUES_LIMIT",
		     policy, "queues_limit");
  mq->queues_limit = NUM2LONG(sz);

  flag = rb_fairy_conf("XSIZED_QUEUE_USE_STRING_BUFFER",
		       policy, "use_string_buffer");
  mq->use_string_buffer_p = RTEST(flag);

  flag = rb_fairy_conf("XSIZED_QUEUE_LOG_MSTORE",
		       policy, "log_mstore");
  mq->log_mstore_p = RTEST(flag);

  mq->queues = rb_xthread_fifo_new();
  if (NIL_P(queues_mon)) {
    queues_mon = rb_xthread_monitor_new();
  }
  mq->buffers_mon = queues_mon;
  
  mq->push_queue = Qnil;
  mq->push_cv = rb_funcall(buffers_mon, rb_intern("new_cond"), 0);

  if (NIL_P(pop_cv)) {
    pop_cv = rb_funcall(buffers_mon, rb_intern("new_cond"), 0);
  }
  mq->pop_cv = pop_cv;
  mq->pop_queue = Qnil;

  mq->queue_push = rb_xsq(_empty_push);

  if (CLASS_OF(mq->queues_mon) == rb_cXThreadMonitor) {
    mq->mon_synchronize = rb_xthread_monitor_synchronize;
    mq->cv_wait = rb_xsq(_monitor_cond_wait);
    mq->cv_broadcast = rb_xthread_monitor_cond_broadcast;
    mq->cv_signal = rb_xthread_monitor_cond_signal;
  }
  else if (CLASS_OF(mq->queues_mon) == rb_cFiberMonMonitor) {
    mq->mon_synchronize = rb_fibermon_monitor_synchronize;
    mq->cv_wait = rb_fibermon_cond_wait;
    mq->cv_signal = rb_fibermon_cond_signal;
    mq->cv_broadcast = rb_fibermon_cond_broadcast;
  }
  else {
    mq->mon_synchronize = rb_xsq(_genmon_synchronize);
    mq->cv_wait = rb_xsq(_gencond_wait);
    mq->cv_signal = rb_xsq(_gencond_signal);
    mq->cv_broadcast = rb_xsq(_gencond_broadcast);
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

static VALUE rb_xsq(_buffers_push)(VALUE, VALUE);
static VALUE rb_xsq(_broadcast)(VALUE);
static VALUE rb_xsq(_store)(VALUE, VALUE);
static VALUE rb_xsq(_restore)(VALUE, VALUE);

VALUE
rb_xsq(_push)(VALUE self, VALUE e)
{
  xsq(_t) *mq;

  GetFairyXSizedQueuePtr(self, mq);
  return mq->queue_push(self, e);
}

static VALUE
rb_xsq(_empty_push)(VALUE self, VALUE e)
{
  xsq(_t) *mq;
  GetFairyXSizedQueuePtr(self, mq);

  if (EOS_P(e)) {
    rb_xthread_fifo_push(mq->queues, e);
    return self;
  }

  if (mq->use_string_buffer_p && CLASS_OF(e) == rb_cString) {
    mq->push_queue = rb_fairy_string_buffer_new();
    mq->queue_push = rb_xsq(_str_push);
  }
  else {
    mq->push_queue = rb_ary_new2(mq->min_chunk_no);
    mq->queue_push = rb_xsq(_obj_push);
  }
  return mq->queue_push(self, e);
}

static VALUE
rb_xsq(_obj_push)(VALUE self, VALUE e)
{
  xsq(_t) *mq;
  GetFairyXSizedQueuePtr(self, mq);

  if (EOS_P(e)) {
    rb_xsq(_queues_push)(self, mq->push_queue);
    mq->push_queue = Qnil;
    rb_xsq(_queues_push)(self, e);
    return self;
  }

  if (mq->use_string_buffer_p && CLASS_OF(e) == rb_cString) {
    rb_xsq(_queues_push)(self, mq->push_queue);
    mq->push_queue = Qnil;
    mq->queue_push = rb_xsq(_empty_push);
    return mq->queue_push(self, e);
  }

  rb_ary_push(mq->push_queue, e);
  if (RARRAY_LEN(mq->push_queue) >= mq->chunk_size || e == SET_NO_IMPORT) {
    rb_xsq(_queues_push)(self, mq->push_queue);
    mq->push_queue = Qnil;
    mq->queue_push = rb_xsq(_empty_push);
  }
  return self;
}

static VALUE
rb_xsq(_str_push)(VALUE self, VALUE e)
{
  xsq(_t) *mq;
  
  GetFairyXSizedQueuePtr(self, mq);
  if (EOS_P(e)) {
    rb_xsq(_queues_push)(self, mq->push_queue);
    rb_xsq(_queues_push)(self, e);
    return self;
  }

  if (CLASS_OF(e) != rb_cString) {
    rb_xsq(_queues_push)(self, mq->push_queue);
    mq->push_queue = Qnil;
    mq->queue_push = rb_xsq(_empty_push);
  }
  rb_fairy_string_buffer_push(mq->push_queue, e);
  if (NUM2LONG(rb_fairy_string_buffer_size(mq->push_queue)) >= mq->chunk_size) {
    rb_xsq(_queues_push)(self, mq->push_queue);
    mq->push_queue = Qnil;
    mq->queue_push = rb_xsq(_empty_push);
  }
  return self;
}

static VALUE
rb_xsq(_push_raw)(VALUE self, VALUE raw)
{
  xsq(_t) *mq;
  
  GetFairyXSizedQueuePtr(self, mq);
  
  if (!NIL_P(mq->push_queue)) {
    rb_xsq(_queues_push)QUEUE(self, mq->push_queue);
    mq->push_queue = Qnil;
  }
  if (EOS_P(raw)) {
    rb_xsq(_queues_push)(self, raw);
  }
  else {
    if (mq->queues_cache_limit >= mq->queues_cache_no) { 
      rb_xsq(_queues_push)(self, raw); 
      mq->queues_cache_no++; 
    } 
    else { 
      rb_xsq(_queues_push)(self, rb_xsq(_store_raw)(self, raw)); 
    }
  }
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
  xsq(_t) *mq;
  struct rb_xsq(_queues_push_arg) arg;

  GetFairyXSizedQueuePtr(self, mq);

  arg.self = self;
  arg.buf = buf;
  
  mq->mon_synchronize(mq->queues_mon,
		      rb_xsq(_push_sync), &arg);
}

static VALUE
rb_xsq(_push_sync)(struct rb_xsq(_queues_push_arg) *arg)
{
  xsq(_t) *mq;
  GetFairyXSizedQueuePtr(arg->self, mq);

  if (rb_xthread_fifo_length(mq->queues) > mq->queues_limit) {
    while(rb_xthread_fifo_length(mq->queues) > mq->queues_limit) {
      mq->cv_wait(mq->push_cv);
    }
  }

  rb_xthread_fifo_push(mq->queues, arg->buf);
  mq->cv_broadcast(mq->pop_cv);
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
  xsq(_t) *mq;
  VALUE buf;
  struct rb_xsq(_pop_arg) arg;
  GetFairyXSizedQueuePtr(self, mq);

  while (NIL_P(mq->pop_queue) || RARRAY_LEN(mq->pop_queue) == 0) {
    {
      arg.self = self;
      arg.buf = Qnil;
      mq->mon_synchronize(mq->queues_mon,
			rb_xsq(_pop_wait), &arg);
      buf = arg.buf;
    }

    if (EOS_P(buf)) {
      mq->pop_queue = rb_ary_new3(1, buf);
    }
    else {
      if (CLASS_OF(buf) == rb_String) {
	buf = rb_marshal_load(buf);
      }
      if (CLASS_OF(buf) == rb_cFairyStringBuffer) {
	mq->pop_queue = rb_fairy_string_buffer_to_a(buf);
      }
      else {
	mq->pop_queue = buf;
      }
    }
  }
  return rb_ary_shift(mq->pop_queue);
}

static VALUE
rb_xsq(_pop_wait)(struct rb_xsq(_pop_arg) *arg)
{
  VALUE self = arg->self;
  xsq(_t) *mq;
  VALUE buf = Qnil;

  GetFairyXSizedQueuePtr(self, mq);
  
  buf = rb_xthread_fifo_pop(mq->queues);
  while (NIL_P(buf)) {
    mq->cv_wait(mq->pop_cv);
    buf = rb_xthread_fifo_pop(mq->queues);
  }
  mq->cv_signal(mq->push_cv);
  arg->buf = buf;
  return arg->self;
}

VALUE
rb_xsq(_pop_raw)(VALUE self)
{
  xsq(_t) *mq;
  VALUE pop_raw = Qnil;
  VALUE buf;
  struct rb_xsq(_pop_arg) arg;
  
  GetFairyXSizedQueuePtr(self, mq);
  {
    arg.self = self;
    arg.buf = Qnil;
    mq->mon_synchronize(mq->queues_mon,
			rb_xsq(_pop_wait), &arg);
    buf = arg.buf;
  }
  return buf;
}

VALUE
rb_xsq(_inspect)(VALUE self)
{
  VALUE str;
  xsq(_t) *mq;

  GetFairyXSizedQueuePtr(self, mq);

  str = rb_sprintf("<%s:%p chunk_size=%d, min_chunk_no=%d, wueues_cache_limit=%d use_string_bffer_p=%d queues_cache_no=%d>",
		   rb_obj_classname(self),
		   (void*)self,
		   mq->chunk_size,
		   mq->min_chunk_no,
		   mq->queues_cache_limit,
		   mq->use_string_buffer_p,
		   mq->queues_cache_no);
  return str;
}


void
Init_xsized_queue()
{
  VALUE xsq;
  
  rb_cFairyFastTempfile = rb_const_get(rb_mFairy, rb_intern("FastTempfile"));

  id_io = rb_intern("io");
  id_open = rb_intern("open");
  id_read = rb_intern("read");
  id_write = rb_intern("write");
  id_close = rb_intern("close");
  id_close_bang = rb_intern("close!");

  SET_NO_IMPORT = rb_const_get(rb_cFairyImport, rb_intern("SET_NO_IMPORT"));

  rb_cFairyXSizedQueue  = rb_define_class_under(rb_mFairy, "XSizedQueue", rb_cObject);

  xmq = rb_cFairyXSizedQueue;
  
  rb_define_alloc_func(xmq, xsq(_alloc));
  rb_define_method(xmq, "initialize", xsq(_initialize), -1);
  rb_define_method(xmq, "push", rb_xsq(_push), 1);
  rb_define_method(xmq, "push_raw", rb_xsq(_push_raw), 1);
  rb_define_method(xmq, "pop", rb_xsq(_pop), 0);
  rb_define_method(xmq, "pop_raw", rb_xsq(_pop_raw), 0);
  rb_define_method(xmq, "inspect", rb_fairy_xsized__queue_inspect, 0);
}

