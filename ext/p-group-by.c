/**********************************************************************

  p-group-by-buffer.c -
  Copyright (C) 2007-2011 Rakuten, Inc.

**********************************************************************/

#include "ruby.h"
#include "xthread.h"

#include "fairy.h"

static ID id_yield;
static ID id_each;

static ID id_sort_by_bang;
static ID id_open_buffer;
static ID id_io;
static ID id_close;

static VALUE rb_cFairyPGroupBy;
static VALUE PPostFilter;
static VALUE MergeSortBuffer;
static VALUE MSBMerger;
static VALUE MSBCachedBuffer;

VALUE rb_cFairyPGbXDirctMergeSortBuffer;
VALUE rb_cFairyPGbXDirctMergeSortBufferCachedBuffer;

#define xmsb(name) fairy_pgb_xdirect_merge_sort_buffer##name
#define rb_xmsb(name) rb_fairy_pgb_xdirect_merge_sort_buffer##name

typedef struct rb_xmsb(_struct)
{
  VALUE njob;
  VALUE buffers;
  
  VALUE threshold;
  long CHUNK_SIZE;

  VALUE hash_proc;
  
} xmsb(_t);

#define GetFairyPGbXDirectMergeSortBufferPtr(obj, tobj) \
  TypedData_Get_Struct((obj), xmsb(_t), &xmsb(_data_type), (tobj))

#define GetFXMSBPtr(obj, tobj) \
  GetFairyPGbXDirectMergeSortBufferPtr(obj, tobj)

static void
xmsb(_mark)(void *ptr)
{
  xmsb(_t) *db = (xmsb(_t)*)ptr;
  rb_gc_mark(db->njob);
  rb_gc_mark(db->buffers);
  rb_gc_mark(db->threshold);
  rb_gc_mark(db->hash_proc);
}

static void
xmsb(_free)(void *ptr)
{
  ruby_xfree(ptr);
}

static size_t
xmsb(_memsize)(const void *ptr)
{
  xmsb(_t) *gb = (xmsb(_t)*)ptr;

  return ptr ? sizeof(xmsb(_t)) : 0;
}

#ifdef HAVE_RB_DATA_TYPE_T_FUNCTION
static const rb_data_type_t xmsb(_data_type) = {
    "fairy_p_xgroup_by_direct_merge_sort_buffer",
    {xmsb(_mark), xmsb(_free), xmsb(_memsize),},
};
#else
static const rb_data_type_t xmsb(_data_type) = {
    "fairy_p_xgroup_by_direct_merge_sort_buffer",
    xmsb(_mark),
    xmsb(_free),
    xmsb(_memsize),
};
#endif

static VALUE
xmsb(_alloc)(VALUE klass)
{
  VALUE volatile obj;
  xmsb(_t) *db;

  obj = TypedData_Make_Struct(klass, xmsb(_t), &xmsb(_data_type), db);

  db->njob = Qnil;
  db->buffers = Qnil;
  db->threshold = Qnil;
  db->CHUNK_SIZE = 0;

  db->hash_proc = Qnil;
  
  return obj;
}

static VALUE
rb_xmsb(_initialize)(VALUE self, VALUE njob, VALUE policy)
{
  xmsb(_t) *db;

  VALUE argv[] = {
    njob,
    policy,
  };

  GetFXMSBPtr(self, db);
  rb_call_super(2, argv);

  db->njob = rb_iv_get(self, "@njob");
  db->threshold = rb_iv_get(self, "@threshold");
  db->CHUNK_SIZE = NUM2LONG(rb_iv_get(self, "@CHUNK_SIZE"));

  return self;
}

static VALUE
rb_xmsb(_init_2ndmemory)(VALUE self)
{
  xmsb(_t) *db;
  
  GetFXMSBPtr(self, db);

  rb_call_super(0, NULL);
  db->buffers = rb_iv_get(self, "@buffers");
}

static VALUE rb_xmsb(_store_2ndmemory_sort_b)(VALUE, VALUE, int, VALUE *);
static VALUE rb_xmsb(_store_2ndmemory_str)(VALUE, VALUE, VALUE, long *);
static VALUE rb_xmsb(_store_2ndmemory_obj)(VALUE, VALUE, VALUE, long *);

VALUE
rb_xmsb(_store_2ndmemory)(VALUE self, VALUE key_values)
{
  VALUE buffer = Qnil;
  VALUE io;
  long i;
  VALUE e;
  VALUE element_class = Qnil;
  xmsb(_t) *db;

  GetFXMSBPtr(self, db);
  
  rb_fairy_debug(self, "START STORE");

  if (NIL_P(db->hash_proc)) {
    db->hash_proc = rb_iv_get(db->njob, "@hash_proc");
  }

  rb_block_call(key_values, id_sort_by_bang, 0, 0,
		rb_xmsb(_store_2ndmemory_sort_b), self);

  buffer = rb_funcall(self, id_open_buffer, 0);
  io = rb_funcall(buffer, id_io, 0);
  i = 0;
  while (i < RARRAY_LEN(key_values)) {
    e = RARRAY_PTR(key_values)[i];
    if (CLASS_OF(e) == rb_cString) {
      rb_xmsb(_store_2ndmemory_str)(self, io, key_values, &i);
    }
    else {
      rb_xmsb(_store_2ndmemory_obj)(self, io, key_values, &i);
    }
  }
  rb_funcall(buffer, id_close, 0);
  
  rb_fairy_debug(self, "FINISH STORE");
  return self;
}

static VALUE
rb_xmsb(_store_2ndmemory_sort_b)(VALUE e, VALUE self, int argc, VALUE *argv)
{
  xmsb(_t) *db;
  VALUE key;

  GetFXMSBPtr(self, db);
  if (CLASS_OF(db->hash_proc) == rb_cProc) {
    key = rb_proc_call(db->hash_proc, rb_ary_new3(1, e));
  }
  else {
    key = rb_funcall(db->hash_proc, id_yield, 1, e);
  }
  return key;
}


static VALUE
rb_xmsb(_store_2ndmemory_str)(VALUE self, VALUE io, VALUE key_values, long *i)
{
  xmsb(_t) *db;
  VALUE sb;
  long start = *i;
  long j;
  VALUE e;
  
  GetFXMSBPtr(self, db);
  
  sb = rb_fairy_string_buffer_new();

  for (j = start;
       j < RARRAY_LEN(key_values) && j - start <= db->CHUNK_SIZE;
       j++) {
    e = RARRAY_PTR(key_values)[j];
    if (CLASS_OF(e) != rb_cString) {
      break;
    }
    rb_fairy_string_buffer_push(sb, e);
  }
  *i = j;
  rb_marshal_dump(sb, io);
  rb_fairy_string_buffer_clear(sb);
  return self;
}

static VALUE
rb_xmsb(_store_2ndmemory_obj)(VALUE self, VALUE io, VALUE key_values, long *i)
{
  
  xmsb(_t) *db;
  long start = *i;
  long j;
  VALUE e;
  
  GetFXMSBPtr(self, db);
  
  for (j = start;
       j < RARRAY_LEN(key_values) && j - start <= db->CHUNK_SIZE;
       j++) {
    e = RARRAY_PTR(key_values)[j];
    if (CLASS_OF(e) == rb_cString) {
      break;
    }
  }
  *i = j;

  if (start == 0 && j == RARRAY_LEN(key_values) -1) {
    rb_marshal_dump(key_values, io);
  }
  else {
    rb_marshal_dump(rb_ary_subseq(key_values, start, j - start), io);
  }

  return self;
}

static VALUE
rb_xmsb(_each_2ndmemory_sub)(VALUE values, VALUE self, int argc, VALUE *argv)
{
  return rb_yield(values);
}


static VALUE
rb_xmsb(_each_2ndmemory)(VALUE self)
{
  xmsb(_t) *db;
  VALUE key_values = rb_iv_get(self, "@key_values");
  VALUE m;

  GetFXMSBPtr(self, db);

  if (RARRAY_LEN(key_values) > 0) {
    rb_xmsb(_store_2ndmemory)(self, key_values);
    rb_iv_set(self, "@key_values", Qnil);
  }
  {
    VALUE arg[] = {
      db->njob,
      db->buffers,
      rb_cFairyPGbXDirctMergeSortBufferCachedBuffer,
    };
   m = rb_class_new_instance(3, arg, MSBMerger);
   return rb_block_call(m, id_each, 0, 0, rb_xmsb(_each_2ndmemory_sub), self);
  }
}

#define xmsbcb(name) fairy_pxg_direct_merge_sort_buffer_cached_buffer##name
#define rb_xmsbcb(name) rb_fairy_pxg_direct_merge_sort_buffer_cached_buffer##name

typedef struct rb_xmsbcb(_struct)
{
  VALUE tmpbuf;
  VALUE io;
} xmsbcb(_t);

#define GetFairyPXGDirectMergeSortBufferCachedBufferPtr(obj, tobj) \
  TypedData_Get_Struct((obj), xmsbcb(_t), &xmsbcb(_data_type), (tobj))

#define GetFXMSBCBPtr(obj, tobj) \
  GetFairyPXGDirectMergeSortBufferCachedBufferPtr(obj, tobj)

static void
xmsbcb(_mark)(void *ptr)
{
  xmsbcb(_t) *cb = (xmsbcb(_t)*)ptr;
  rb_gc_mark(cb->tmpbuf);
  rb_gc_mark(cb->io);
}

static void
xmsbcb(_free)(void *ptr)
{
  ruby_xfree(ptr);
}

static size_t
xmsbcb(_memsize)(const void *ptr)
{
  xmsbcb(_t) *cb = (xmsbcb(_t)*)ptr;

  return ptr ? sizeof(xmsbcb(_t)) : 0;
}

#ifdef HAVE_RB_DATA_TYPE_T_FUNCTION
static const rb_data_type_t xmsbcb(_data_type) = {
    "fairy_p_xgroup_by_direct_merge_sort_buffer_cached_buffer",
    {xmsbcb(_mark), xmsbcb(_free), xmsbcb(_memsize),},
};
#else
static const rb_data_type_t xmsbcb(_data_type) = {
    "fairy_p_xgroup_by_direct_merge_sort_buffer_cached_buffer",
    xmsbcb(_mark),
    xmsbcb(_free),
    xmsbcb(_memsize),
};
#endif

static VALUE
xmsbcb(_alloc)(VALUE klass)
{
  VALUE volatile obj;
  xmsbcb(_t) *cb;

  obj = TypedData_Make_Struct(klass, xmsbcb(_t), &xmsbcb(_data_type), cb);

  cb->tmpbuf = Qnil;
  cb->io = Qnil;

  return obj;
}

static VALUE
rb_xmsbcb(_initialize)(VALUE self, VALUE njob, VALUE io)
{
  xmsbcb(_t) *cb;
  VALUE tmpbuf;

  VALUE argv[] = {
    njob,
    io,
  };

  GetFXMSBCBPtr(self, cb);
  cb->tmpbuf = io;
  rb_call_super(2, argv);
  
  return self;
}

/*
 * copy from eval_intern.h
 */

enum ruby_tag_type {
    RUBY_TAG_RETURN	= 0x1,
    RUBY_TAG_BREAK	= 0x2,
    RUBY_TAG_NEXT	= 0x3,
    RUBY_TAG_RETRY	= 0x4,
    RUBY_TAG_REDO	= 0x5,
    RUBY_TAG_RAISE	= 0x6,
    RUBY_TAG_THROW	= 0x7,
    RUBY_TAG_FATAL	= 0x8,
    RUBY_TAG_MASK	= 0xf
};
#define TAG_RETURN	RUBY_TAG_RETURN
#define TAG_BREAK	RUBY_TAG_BREAK
#define TAG_NEXT	RUBY_TAG_NEXT
#define TAG_RETRY	RUBY_TAG_RETRY
#define TAG_REDO	RUBY_TAG_REDO
#define TAG_RAISE	RUBY_TAG_RAISE
#define TAG_THROW	RUBY_TAG_THROW
#define TAG_FATAL	RUBY_TAG_FATAL
#define TAG_MASK	RUBY_TAG_MASK


static VALUE rb_xmsbcb(_read_buffer_sub)(VALUE);

#define DEBUG_MSG(msg) "rb_xmsbcb(_readbuffer): " #msg

VALUE
rb_xmsbcb(_read_buffer)(VALUE self)
{
  VALUE result;
  int state;
  xmsbcb(_t) *cb;

  GetFXMSBCBPtr(self, cb);
  
  rb_iv_set(self, "@cache_pv", INT2FIX(0));

  if (NIL_P(cb->io)) {
    cb->io = rb_funcall(cb->tmpbuf, id_io, 0);
  }
  
  if (RTEST(rb_io_eof(cb->io))) {
    rb_fairy_debugf(self, DEBUG_MSG(EOF reached: %s), RSTRING_PTR(rb_inspect(cb->io)));
    rb_iv_set(self, "@eof", Qtrue);
    rb_iv_set(self, "@cache", rb_ary_new());
    return self;
  }
  result = rb_protect(rb_xmsbcb(_read_buffer_sub), self, &state);
  if (state) {
    rb_fairy_debug(self, DEBUG_MSG(1 - rb_protext return non zero state!!));
    rb_fairy_debugf(self, DEBUG_MSG(state: %d), state);

    if (state == TAG_RAISE) {
      VALUE exp = rb_errinfo();
      
      rb_fairy_debug(self, DEBUG_MSG(2));  
      rb_fairy_debugf(self, DEBUG_MSG(Exeption: %s), RSTRING_PTR(rb_inspect(exp)));
      if (CLASS_OF(exp) ==  rb_eEOFError) {
	rb_fairy_debug(self, DEBUG_MSG(3 - EOF reached));
	rb_iv_set(self, "@eof", Qtrue);
	rb_iv_set(self, "@cache", rb_ary_new());
      }
      else if (CLASS_OF(exp) == rb_eArgError) {
	const char *head = "File Contents: ";
	/*	VALUE SEEK_CUR = rb_const_get(rb_cIO, rb_intern("SEEK_CUR")); */
	char *buf = ALLOCA_N(char, strlen(head) + 2048 + 1);
	VALUE readed;
	VALUE io;
	
	rb_fairy_debug(self, DEBUG_MSG(4 - MARSHAL ERROR OCCURED!!));  

	io = rb_funcall(cb->tmpbuf, id_io, 0);
	rb_funcall(io, rb_intern("seek"), 2, INT2NUM(-1024), SEEK_CUR);
	readed = rb_funcall(io, rb_intern("read"), 1, INT2NUM(2048));

	strcat(buf, head);
	strncat(buf, RSTRING_PTR(readed), RSTRING_LEN(readed));
	rb_fairy_debug(self, buf);
	rb_jump_tag(state);
      }
      else {
	rb_fairy_debug(self, DEBUG_MSG(5)); 
	rb_jump_tag(state);
      }
      rb_fairy_debug(self, DEBUG_MSG(6)); 
    }
    else {
      rb_fairy_debug(self, DEBUG_MSG(7)); 
      rb_jump_tag(state);
    }
  }
  return self;
}

static VALUE
rb_xmsbcb(_read_buffer_sub)(VALUE self)
{
  xmsbcb(_t) *cb;
  VALUE cache;
  
  GetFXMSBCBPtr(self, cb);

  cache = rb_marshal_load(cb->io);
  if (CLASS_OF(cache) == rb_cFairyStringBuffer) {
    VALUE tmp = cache;
    cache = rb_fairy_string_buffer_to_a(tmp);
    rb_fairy_string_buffer_clear(tmp);
  }
  rb_iv_set(self, "@cache", cache);
  return self;
}

void
Init_p_group_by()
{
  VALUE msb;
  VALUE xmsb;
  VALUE xmsbcb;

  if (!rb_const_defined(rb_mFairy, rb_intern("PGroupBy"))) {
    return;
  }
  rb_cFairyPGroupBy = rb_const_get(rb_mFairy, rb_intern("PGroupBy"));
  
  id_yield = rb_intern("yield");
  id_each = rb_intern("each");

  id_sort_by_bang = rb_intern("sort_by!");
  id_open_buffer = rb_intern("open_buffer");
  id_io = rb_intern("io");
  id_close = rb_intern("close");

  msb= rb_const_get(rb_cFairyPGroupBy, rb_intern("DirectMergeSortBuffer"));
  MergeSortBuffer = msb;
  MSBMerger = rb_const_get(msb, rb_intern("Merger"));
  MSBCachedBuffer = rb_const_get(msb, rb_intern("CachedBuffer"));

  xmsb = rb_define_class_under(rb_cFairyPGroupBy,
			       "XDirectMergeSortBuffer", MergeSortBuffer);
  rb_cFairyPGbXDirctMergeSortBuffer = xmsb;
  rb_define_alloc_func(xmsb, xmsb(_alloc));
  rb_define_method(xmsb, "initialize", rb_xmsb(_initialize), 2);
  rb_define_method(xmsb, "init_2ndmemory", rb_xmsb(_init_2ndmemory), 0);
  rb_define_method(xmsb, "store_2ndmemory", rb_xmsb(_store_2ndmemory), 1);
  rb_define_method(xmsb, "each_2ndmemory", rb_xmsb(_each_2ndmemory), 0);

  xmsbcb = rb_define_class_under(xmsb, "CachedBuffer", MSBCachedBuffer);
  rb_cFairyPGbXDirctMergeSortBufferCachedBuffer = xmsbcb;
  rb_define_alloc_func(xmsbcb, xmsbcb(_alloc));
  rb_define_method(xmsbcb, "initialize", rb_xmsbcb(_initialize), 2);
  rb_define_method(xmsbcb, "read_buffer", rb_xmsbcb(_read_buffer), 0);
}

