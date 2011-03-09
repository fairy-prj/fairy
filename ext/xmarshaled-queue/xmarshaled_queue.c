/**********************************************************************

  xmarshaled_queue.c -
  Copyright (C) 2007-2011 Rakuten, Inc.

**********************************************************************/

#include "ruby.h"

#include "xthread.h"
#include "fiber-mon.h"

extern VALUE rb_cMonitor;
extern VALUE rb_cFiberMon;
extern VALUE rb_cFairyStringBuffer;

static VALUE rb_cFairy;
static VALUE rb_cFairyImport;
static VALUE rb_cFairyFastTempfile;

static VALUE SET_NO_IMPORT;

VALUE rb_cFairyXMarshaledQueue;

static ID EOS;

static ID id_io;
static ID id_open;
static ID id_write;
static ID id_close;
static ID id_close_bang;

#define EOS_P(v) (SYMBOL_P(v) && (SYM2ID(v) == EOS))

typedef struct rb_fairy_xmarshaled_queue_struct
{
  long chunk_size;
  long min_chunk_no;

  char use_string_buffer_p;
  enum {
    use_array,
    use_string_buffer,
  } push_queue_type;

  VALUE push_queue;
  
  VALUE buffers;
  VALUE buffers_mon;
  VALUE buffers_cv;

  VALUE pop_queue;

  VALUE buffer_dir;

  VALUE (*queue_push)(VALUE, VALUE);

  VALUE (*mon_synchronize)();
  VALUE (*cv_wait)();
  VALUE (*cv_broadcast)();
  
} fairy_xmarshaled_queue_t;

#define GetFairyXMarshaledQueuePtr(obj, tobj) \
  TypedData_Get_Struct((obj), fairy_xmarshaled_queue_t, &fairy_xmarshaled_queue_data_type, (tobj))

static void
fairy_xmarshaled_queue_mark(void *ptr)
{
  fairy_xmarshaled_queue_t *mq = (fairy_xmarshaled_queue_t*)ptr;
  
  rb_gc_mark(mq->push_queue);
  rb_gc_mark(mq->buffers);
  rb_gc_mark(mq->buffers_mon);
  rb_gc_mark(mq->buffers_cv);
  rb_gc_mark(mq->pop_queue);
  rb_gc_mark(mq->buffer_dir);
}

static void
fairy_xmarshaled_queue_free(void *ptr)
{
  ruby_xfree(ptr);
}

static size_t
fairy_xmarshaled_queue_memsize(const void *ptr)
{
  return ptr ? sizeof(fairy_xmarshaled_queue_t) : 0;
}

static const rb_data_type_t fairy_xmarshaled_queue_data_type = {
    "fairy_xmarshaled_queue",
    {fairy_xmarshaled_queue_mark, fairy_xmarshaled_queue_free, fairy_xmarshaled_queue_memsize,},
};

static VALUE
fairy_xmarshaled_queue_alloc(VALUE klass)
{
  VALUE volatile obj;
  fairy_xmarshaled_queue_t *mq;

  obj = TypedData_Make_Struct(klass, fairy_xmarshaled_queue_t, &fairy_xmarshaled_queue_data_type, mq);

  mq->chunk_size = 0;
  mq->min_chunk_no = 0;
  
  mq->push_queue = Qnil;
  mq->buffers = Qnil;
  mq->buffers_mon = Qnil;
  mq->buffers_cv = Qnil;
  mq->pop_queue = Qnil;

  mq->buffer_dir = Qnil;

  mq->queue_push = NULL;

  mq->mon_synchronize = NULL;
  mq->cv_wait = NULL;
  mq->cv_broadcast = NULL;
  
  return obj;
}

static VALUE rb_fairy_xmarshaled_queue_genmon_synchronize(VALUE, VALUE (*)(VALUE), VALUE);
static VALUE rb_fairy_xmarshaled_queue_gencond_wait(VALUE, VALUE);
static VALUE rb_fairy_xmarshaled_queue_gencond_broadcast(VALUE);

static VALUE rb_fairy_xmarshaled_queue_empty_push(VALUE, VALUE);
static VALUE rb_fairy_xmarshaled_queue_str_push(VALUE, VALUE);
static VALUE rb_fairy_xmarshaled_queue_obj_push(VALUE, VALUE);

static VALUE
rb_fairy_xmarshaled_queue_initialize(VALUE self, VALUE policy, VALUE buffers_mon, VALUE buffers_cv)
{
  fairy_xmarshaled_queue_t *mq;
  VALUE conf;
  
  GetFairyXMarshaledQueuePtr(self, mq);
  
  conf = rb_const_get(rb_cFairy, rb_intern("CONF"));
  {
    VALUE sz;
    VALUE flag;
    VALUE dir;
    ID id_aref = rb_intern("[]");
    
  
    sz = rb_funcall(policy, id_aref, 1, rb_intern("chunk_size"));
    if (NIL_P(sz)) {
      sz = rb_funcall(conf, rb_intern("XMARSHAL_QUEUE_CHUNK_SIZE"), 0);
    }
    mq->chunk_size = NUM2LONG(sz);

    sz = rb_funcall(policy, id_aref, 1, rb_intern("min_chunk_no"));
    if (NIL_P(sz)) {
      sz = rb_funcall(conf, rb_intern("XMARSHAL_QUEUE_MIN_CHUNK_NO"), 0);
    }
    mq->min_chunk_no = NUM2LONG(sz);

    flag = rb_funcall(policy, id_aref, 1, rb_intern("use_string_buffer"));
    if (NIL_P(flag)) {
      flag = rb_funcall(conf, rb_intern("XMARSHAL_QUEUE_MIN_CHUNK_NO"), 0);
    }
    mq->use_string_buffer_p = flag;

    dir = rb_funcall(policy, id_aref, 1, rb_intern("buffer_dir"));
    if (NIL_P(dir)) {
      flag = rb_funcall(conf, rb_intern("TMP_DIR"), 0);
    }
    mq->use_string_buffer_p = flag;
  }

  mq->push_queue = Qnil;
  
  mq->buffers = rb_fifo_new();
  if (NIL_P(buffers_mon)) {
    buffers_mon = rb_monitor_new();
  }
  mq->buffers_mon = buffers_mon;
  
  if (NIL_P(buffers_cv)) {
    buffers_cv = rb_funcall(buffers_mon, rb_intern("new_cond"), 0);
  }
  mq->buffers_cv = buffers_cv;
  
  mq->pop_queue = Qnil;

  mq->queue_push = rb_fairy_xmarshaled_queue_empty_push;

  if (CLASS_OF(mq->buffers_mon) == rb_cMonitor) {
    mq->mon_synchronize = rb_monitor_synchronize;
    mq->cv_wait = rb_monitor_cond_wait;
    mq->cv_broadcast = rb_monitor_cond_broadcast;
  }
  else if (CLASS_OF(mq->buffers_mon) == rb_cFiberMon) {
    mq->mon_synchronize = rb_fibermon_monitor_synchronize;
    mq->cv_wait = rb_fibermon_cond_wait;
    mq->cv_broadcast = rb_fibermon_cond_broadcast;
  }
  else {
    mq->mon_synchronize = rb_fairy_xmarshaled_queue_genmon_synchronize;
    mq->cv_wait = rb_fairy_xmarshaled_queue_gencond_wait;
    mq->cv_broadcast = rb_fairy_xmarshaled_queue_gencond_broadcast;
  }
  
  return self;
}

static VALUE
rb_fairy_xmarshaled_queue_genmon_synchronize(VALUE arg1, VALUE (*arg2)(VALUE), VALUE arg3)
{
  static ID id_synchronize;
  if (!id_synchronize) id_synchronize = rb_intern("synchronize");

  return rb_funcall(arg1, id_synchronize, 2, arg2, arg3);
}

static VALUE
rb_fairy_xmarshaled_queue_gencond_wait(VALUE arg1, VALUE arg2)
{
  static ID id_wait;
  if (!id_wait) id_wait = rb_intern("wait");

  return rb_funcall(arg1, id_wait, 1, arg2);
}

static VALUE
rb_fairy_xmarshaled_queue_gencond_broadcast(VALUE arg1)
{
  static ID id_broadcast;
  if (!id_broadcast) id_broadcast = rb_intern("broadcast");

  return rb_funcall(arg1, id_broadcast, 0);
}

static VALUE
fairy_xmarshaled_queue_initialize(int argc, VALUE *argv, VALUE self)
{
  VALUE policy;
  VALUE buffers_mon;
  VALUE buffers_cv;
  
  rb_scan_args(argc, argv, "12", &policy, &buffers_mon, &buffers_cv);

  return rb_fairy_xmarshaled_queue_initialize(self, policy, buffers_mon, buffers_cv);
}

VALUE
rb_fairy_xmarshaled_queue_new(VALUE policy, VALUE buffers_mon, VALUE buffers_cv)
{
  VALUE self;

  self = fairy_xmarshaled_queue_alloc(rb_cFairyXMarshaledQueue);
  rb_fairy_xmarshaled_queue_initialize(self, policy, buffers_mon, buffers_cv);
  return self;
}

static VALUE rb_fairy_xmarshaled_queue_buffers_push(VALUE, VALUE);
static VALUE rb_fairy_xmarshaled_queue_broadcast(VALUE);
static VALUE rb_fairy_xmarshaled_queue_store(VALUE, VALUE);
static VALUE rb_fairy_xmarshaled_queue_restore(VALUE, VALUE);

#define BUFFERS_PUSH(self, buf) \
  rb_fairy_xmarshaled_queue_buffers_push(self, buf)

VALUE
rb_fairy_xmarshaled_queue_push(VALUE self, VALUE e)
{
  fairy_xmarshaled_queue_t *mq;
  GetFairyXMarshaledQueuePtr(self, mq);

  return mq->queue_push(self, e);
  
}

static VALUE
rb_fairy_xmarshaled_queue_empty_push(VALUE self, VALUE e)
{
  fairy_xmarshaled_queue_t *mq;
  GetFairyXMarshaledQueuePtr(self, mq);

  if (EOS_P(e)) {
    rb_queue_push(mq->buffers, e);
    return self;
  }

  if (mq->use_string_buffer_p && CLASS_OF(e) == rb_cString) {
    mq->push_queue = rb_ary_new2(mq->min_chunk_no);
    mq->queue_push = rb_fairy_xmarshaled_queue_str_push;
  }
  else {
    mq->push_queue = rb_fairy_string_buffer_new();
    mq->queue_push = rb_fairy_xmarshaled_queue_obj_push;
  }
  return mq->queue_push(self, e);
}

static VALUE
rb_fairy_xmarshaled_queue_obj_push(VALUE self, VALUE e)
{
  fairy_xmarshaled_queue_t *mq;
  GetFairyXMarshaledQueuePtr(self, mq);

  puts("AAAAAAAAAAAAAAAAAA:");

  if (EOS_P(e)) {
    BUFFERS_PUSH(self, rb_fairy_xmarshaled_queue_store(self, mq->push_queue));
    BUFFERS_PUSH(self, e);
    return self;
  }

  if (mq->use_string_buffer_p && CLASS_OF(e) == rb_cString) {
    BUFFERS_PUSH(self, rb_fairy_xmarshaled_queue_store(self, mq->push_queue));
    mq->push_queue = Qnil;
    mq->queue_push = rb_fairy_xmarshaled_queue_empty_push;
    return mq->queue_push(self, e);
  }

  rb_ary_push(mq->push_queue, e);
  if (RARRAY_LEN(mq->push_queue) >= mq->chunk_size || e == SET_NO_IMPORT) {
    BUFFERS_PUSH(self, rb_fairy_xmarshaled_queue_store(self, mq->push_queue));
    mq->push_queue = Qnil;
    mq->queue_push = rb_fairy_xmarshaled_queue_empty_push;
  }
  return self;
}

static VALUE
rb_fairy_xmarshaled_queue_str_push(VALUE self, VALUE e)
{
  fairy_xmarshaled_queue_t *mq;
  
  GetFairyXMarshaledQueuePtr(self, mq);
  if (EOS_P(e)) {
    BUFFERS_PUSH(self, rb_fairy_xmarshaled_queue_store(self, mq->push_queue));
    BUFFERS_PUSH(self, e);
    
    return self;
  }

  if (CLASS_OF(e) != rb_cString) {
    BUFFERS_PUSH(self, rb_fairy_xmarshaled_queue_store(self, mq->push_queue));
    mq->push_queue = Qnil;
    mq->queue_push = rb_fairy_xmarshaled_queue_empty_push;
  }
  mq->queue_push(self, e);
  if (rb_fairy_string_buffer_size(mq->push_queue) >= mq->chunk_size) {
    BUFFERS_PUSH(self, rb_fairy_xmarshaled_queue_store(self, mq->push_queue));
    mq->push_queue = Qnil;
    mq->queue_push = rb_fairy_xmarshaled_queue_empty_push;
  }
  return self;
}

static VALUE
rb_fairy_xmarshaled_queue_push_raw(VALUE self, VALUE raw)
{
  fairy_xmarshaled_queue_t *mq;
  
  GetFairyXMarshaledQueuePtr(self, mq);

  if (!NIL_P(mq->push_queue)) {
    BUFFERS_PUSH(self, rb_fairy_xmarshaled_queue_store(self, mq->push_queue));
    mq->push_queue = Qnil;
  }
  if (EOS_P(raw)) {
    BUFFERS_PUSH(self, rb_ary_new3(1, raw));
  }
  else {
    BUFFERS_PUSH(self, rb_fairy_xmarshaled_queue_store(self, raw));
  }
  return self;
}

struct rb_fairy_xmarshaled_queue_buffers_push_arg 
{
  VALUE self;
  VALUE buf;
};

static VALUE rb_fairy_xmarshaled_queue_push_sync(struct rb_fairy_xmarshaled_queue_buffers_push_arg *);


static VALUE
rb_fairy_xmarshaled_queue_buffers_push(VALUE self, VALUE buf)
{
  fairy_xmarshaled_queue_t *mq;
  struct rb_fairy_xmarshaled_queue_buffers_push_arg arg;

  GetFairyXMarshaledQueuePtr(self, mq);

  arg.self = self;
  arg.buf = buf;
  
  mq->mon_synchronize(mq->buffers_mon,
		      rb_fairy_xmarshaled_queue_push_sync, &arg);
}

static VALUE
rb_fairy_xmarshaled_queue_push_sync(struct rb_fairy_xmarshaled_queue_buffers_push_arg *arg)
{
  fairy_xmarshaled_queue_t *mq;
  GetFairyXMarshaledQueuePtr(arg->self, mq);

  rb_fifo_push(mq->buffers, arg->buf);
  mq->cv_broadcast(mq->buffers_cv);
  
}

struct rb_fairy_xmarshaled_queue_pop_arg 
{
  VALUE self;
  VALUE buf;
};

static VALUE rb_fairy_xmarshaled_queue_pop_wait(struct rb_fairy_xmarshaled_queue_pop_arg *arg);

VALUE
rb_fairy_xmarshaled_queue_pop(VALUE self)
{
  fairy_xmarshaled_queue_t *mq;
  VALUE buf;
  struct rb_fairy_xmarshaled_queue_pop_arg arg;
  
  GetFairyXMarshaledQueuePtr(self, mq);

  while (NIL_P(mq->pop_queue) || RARRAY_LEN(mq->pop_queue) == 0) {
    buf = rb_fifo_pop(mq->buffers);
    if (NIL_P(buf)) {
      arg.self = self;
      arg.buf = Qnil;
      mq->mon_synchronize(mq->buffers_mon,
			  rb_fairy_xmarshaled_queue_pop_wait, &arg);
      buf = arg.buf;
    }
    mq->pop_queue = rb_xmarshaled_queue_restore(self, buf);
  }
  return rb_ary_shift(mq->pop_queue);
}

static VALUE
rb_fairy_xmarshaled_queue_pop_wait(struct rb_fairy_xmarshaled_queue_pop_arg *arg)
{
  VALUE self = arg->self;
  fairy_xmarshaled_queue_t *mq;
  VALUE buf = Qnil;

  GetFairyXMarshaledQueuePtr(self, mq);

  buf = rb_fifo_pop(self);
  while (!buf) {
    mq->cv_wait(mq->buffers_cv);
    buf = rb_fifo_pop(self);
  }
  arg->buf = buf;
}

VALUE
rb_fairy_xmarshaled_queue_pop_raw(VALUE self)
{
  fairy_xmarshaled_queue_t *mq;
  VALUE pop_raw = Qnil;
  VALUE buf;
  struct rb_fairy_xmarshaled_queue_pop_arg arg;
  
  GetFairyXMarshaledQueuePtr(self, mq);

  buf = rb_fifo_pop(mq->buffers);
  if (NIL_P(buf)) {
    arg.self = self;
    arg.buf = Qnil;
    mq->mon_synchronize(mq->buffers_mon,
			rb_fairy_xmarshaled_queue_pop_wait, &arg);
    buf = arg.buf;
  }
  pop_raw = rb_xmarshaled_queue_restore(self, buf);
  
  return pop_raw;
}


static VALUE
rb_fairy_xmarshaled_queue_store(VALUE self, VALUE buffer)
{
  VALUE tmpbuf;
  VALUE io;
  fairy_xmarshaled_queue_t *mq;

  GetFairyXMarshaledQueuePtr(self, mq);

  tmpbuf = rb_funcall(rb_cFairyFastTempfile, id_open, 2,
		      rb_str_new2("port-buffer-"),
		      mq->buffer_dir);
  io = rb_funcall(tmpbuf, id_io, 0);
  rb_marshal_dump(buffer, io);
  rb_funcall(tmpbuf, id_close, 0);
  return tmpbuf;
}

static VALUE
rb_fairy_xmarshaled_queue_restore(VALUE self, VALUE tmpbuf)
{
  VALUE buf;
  VALUE io;
  fairy_xmarshaled_queue_t *mq;

  GetFairyXMarshaledQueuePtr(self, mq);

  io = rb_funcall(tmpbuf, id_open, 0);
  
  buf = rb_marshal_load(io);
  if (CLASS_OF(buf) == rb_cFairyStringBuffer) {
    buf = rb_fairy_string_buffer_to_a(buf);
  }
  return buf;
}

void
Init_xmarshaled_queue()
{
  VALUE xmq;
  ID id_Fairy = rb_intern("Fairy");

  rb_require("fairy/string_buffer");
  
  rb_cFairy = rb_const_get(rb_cObject, id_Fairy);
  rb_cFairyFastTempfile = rb_const_get(rb_cFairy, rb_intern("FastTempfile"));
  rb_cFairyImport = rb_const_get(rb_cFairy, rb_intern("Import"));
  
  EOS = rb_intern("END_OF_STREAM");
  SET_NO_IMPORT = rb_const_get(rb_cFairyImport, rb_intern("SET_NO_IMPORT"));

  id_io = rb_intern("io");
  id_open = rb_intern("open");
  id_write = rb_intern("write");
  id_close = rb_intern("close");
  id_close_bang = rb_intern("close!");

  rb_cFairyXMarshaledQueue  = rb_define_class_under(rb_cFairy, "XMarshaledQueue", rb_cObject);

  xmq = rb_cFairyXMarshaledQueue;
  
  rb_define_alloc_func(xmq, fairy_xmarshaled_queue_alloc);
  rb_define_method(xmq, "initialize", fairy_xmarshaled_queue_initialize, -1);
  rb_define_method(xmq, "push", rb_fairy_xmarshaled_queue_push, 1);
  rb_define_method(xmq, "pop", rb_fairy_xmarshaled_queue_pop, 0);

}

